using System;
using System.Collections.Concurrent;
using System.Drawing;
using System.Text;

namespace NesterovskyBros.VectorIndex;

/// <summary>
/// An API to build vector index.
/// </summary>
public partial class IndexBuilder
{
  /// <summary>
  /// Populates ranges using points as input.
  /// </summary>
  /// <param name="points">Input points.</param>
  /// <param name="ranges">Output ranges. Should be empty initially.</param>
  /// <param name="pointRanges">
  /// Point to ranges mapping. Should be empty initially.
  /// </param>
  /// <param name="stats">
  /// Intermediate grouping results. Should be empty initially.
  /// </param>
  /// <returns>A value task.</returns>
  public static async ValueTask Build(
    IStore<long, Memory<float>> points,
    IStore<long, RangeValue> ranges,
    IStore<long, long> pointRanges,
    IStore<StatsKey, Memory<Stats>> stats)
  {
    var parallel = false;
    long count = 0;
    var segments = new ConcurrentDictionary<long, (int index, byte[] bits)>();
    var rangeLocks = new ConcurrentDictionary<long,
      (SemaphoreSlim semaphore, int refcount)>();
    var singleSegment = true;

    var iteration = 0;

    Console.WriteLine("Iteraton: 0.");

    // Calculate initial stats.
    await ForEach(
      points.GetItems(),
      async item =>
      {
        await CollectStats(item.id, 0, item.value);
        
        var x = Interlocked.Increment(ref count);

        if (x % 100000 == 0)
        {
          Console.WriteLine($"Processed {x}");
        }
      });

    if (count == 0)
    {
      return;
    }

    if (!singleSegment)
    {
      await CombineStatsSegments();
    }

    Console.WriteLine("Update ranges.");

    // Set initial ranges.
    {
      var item = await stats.Get(new(0, 0));

      await stats.Remove(new(0, 0));

      // Select best split.
      var index = MatchStats(item!);
      var match = item.Span[index];

      RangeValue range = match.Count == 1 ?
        new() { Dimension = -1, Id = (long)match.IdN } :
        match.Stdev2N == 0 ?
          new() { Dimension = -2, Id = (long)(match.IdN / match.Count) } :
          new() { Dimension = index, Mid = match.Mean };

      await ranges.Set(0, range);

      if (match.Count == 1)
      {
        return;
      }

      await ForEach(
        points.GetItems(),
        async item =>
        {
          var (id, point) = item;

          var rangeId = range.Dimension < 0 ?
            id <= range.Id ? 1 : 2 :
            point.Span[range.Dimension] < range.Mid ? 1 : 2;

          await pointRanges.Set(id, rangeId);
        });
    }

    while(count > 0)
    {
      ++iteration;
      
      Console.WriteLine($"Iteraton: {iteration}, count: {count}.");

      singleSegment = true;

      var c = 0;

      // Calculate stats.
      await ForEach(
        pointRanges.GetItems(),
        async item =>
        {
          var (id, rangeId) = item;
          var point = await points.Get(id);

          await CollectStats(id, rangeId, point!);

          var x = Interlocked.Increment(ref c);

          if(x % 100000 == 0)
          {
            Console.WriteLine($"Processed {x}");
          }
        });

      if (!singleSegment)
      {
        await CombineStatsSegments();
      }

      // Select best splits.
      await ForEach(
        stats.GetItems(),
        //options,
        async item =>
        {
          var rangeId = item.id.RangeID;
          var index = MatchStats(item.value);
          var match = item.value.Span[index];

          if (match.Count == 1)
          {
            var id = (long)match.IdN;

            await ranges.Set(rangeId, new() { Dimension = -1, Id = id });
            await pointRanges.Remove(id);
            Interlocked.Decrement(ref count);
          }
          else if (match.Stdev2N == 0)
          {
            await ranges.Set(
              rangeId, 
              new() { Dimension = -2, Id = (long)(match.IdN / match.Count) });
          }
          else
          {
            await ranges.Set(
              rangeId, 
              new() { Dimension = index, Mid = match.Mean });
          }

          await stats.Remove(item.id);
        });

      if (count == 0)
      {
        break;
      }

      Console.WriteLine("Update ranges.");

      // Update point ranges;
      await ForEach(
        pointRanges.GetItems(),
        async item =>
        {
          var (pointId, rangeId) = item;
          var range = await ranges.Get(rangeId);
          long nextRangeId;

          if (range.Dimension < 0)
          {
            nextRangeId = rangeId * 2 + (pointId <= range.Id ? 1 : 2);
          }
          else
          {
            var point = await points.Get(pointId);

            nextRangeId = 
              rangeId * 2 + (point.Span[range.Dimension] < range.Mid ? 1 : 2);
          }

          await pointRanges.Set(pointId, nextRangeId);
        });
    }

    int GetSegment(long rangeId) =>
      !parallel ? 0 :
      segments.AddOrUpdate(
        rangeId,
        rangeId => (0, new byte[] { 1 }),
        (rangeId, value) =>
        {
          var bits = value.bits;
          var index = Array.IndexOf<byte>(bits, 0);

          if (index < 0)
          {
            index = bits.Length;
            Array.Resize(ref bits, index + 1);
          }
          else
          {
            bits = (byte[])bits.Clone();
          }

          bits[index] = 1;

          return (index, bits);
        }).index;

    void ReleaseSegment(long rangeId, int segment)
    {
      if (!parallel)
      {
        return;
      }

      while(true)
      {
        var value = segments![rangeId];
        var bits = value.bits;
        var empty = true;

        for(var i = 0; i < bits.Length; ++i)
        {
          if (i != segment && bits[i] != 0)
          {
            empty = false;

            break;
          }
        }

        if (empty)
        {
          if (segments.TryRemove(new(rangeId, value)))
          {
            break;
          }
        }
        else
        {
          bits = (byte[])bits.Clone();
          bits[segment] = 0;

          if (segments.TryUpdate(rangeId, (-1, bits), value))
          {
            break;
          }
        }
      }
    }

    async ValueTask<(SemaphoreSlim semaphore, int refcount)> LockRange(
      long rangeId)
    {
      var handle = rangeLocks.AddOrUpdate(
        rangeId,
        n => (new(1, 1), 1),
        (n, h) => (h.semaphore, h.refcount + 1));

      await handle.semaphore.WaitAsync();

      return handle;
    }

    void ReleaseRange(
      long rangeId, 
      (SemaphoreSlim semaphore, int refcount) handle)
    {
      handle.semaphore.Release();

      handle = rangeLocks.AddOrUpdate(
        rangeId,
        handle,
        (id, h) => (h.semaphore, h.refcount - 1));

      if (handle.refcount == 0 &&
        rangeLocks.TryRemove(new(rangeId, handle)))
      {
        handle.semaphore.Dispose();
      }
    }

    async ValueTask CollectStats(long id, long rangeId, Memory<float> point)
    {
      var segment = GetSegment(rangeId);

      if (segment != 0)
      {
        singleSegment = false;
      }

      var item = await stats.Get(new(segment, rangeId));

      await stats.Set(new(segment, rangeId), UpdateStats(id, item, point));

      ReleaseSegment(rangeId, segment);
    }

    Memory<Stats> UpdateStats(long id, Memory<Stats> stats, Memory<float> point)
    {
      var pointSpan = point.Span;

      if (stats.IsEmpty)
      {
        stats = new Stats[pointSpan.Length];

        var span = stats.Span;

        for(var i = 0; i < pointSpan.Length; ++i)
        {
          span[i] = new()
          {
            Mean = pointSpan[i],
            Stdev2N = 0,
            Count = 1,
            IdN = id
          };
        }
      }
      else
      {
        var span = stats.Span;

        for(var i = 0; i < pointSpan.Length; ++i)
        {
          var value = pointSpan[i];
          ref var item = ref span[i];
          var pa = item.Mean;
          var pq = item.Stdev2N;
          var count = item.Count + 1;
          var a = pa + (value - pa) / count;
          var q = pq + (value - pa) * (value - a);

          item = new()
          {
            Mean = a,
            Stdev2N = q,
            Count = count,
            IdN = item.IdN + id
          };
        }
      }

      return stats;
    }

    int MatchStats(Memory<Stats> stats)
    {
      var index = -1;
      var value = -1f;
      var span = stats.Span;

      for(var i = 0; i < span.Length; ++i)
      {
        ref var item = ref span[i];

        if (value < item.Stdev2N)
        {
          value = item.Stdev2N;
          index = i;
        }
      }

      return index;
    }

    async ValueTask CombineStatsSegments() =>
      await ForEach(
        stats.GetItems(),
        //options,
        async item =>
        {
          if (item.id.Segment == 0)
          {
            return;
          }

          var rangeId = item.id.RangeID;
          var handle = await LockRange(rangeId);

          try
          {
            var groupItem = await stats.Get(new(0, rangeId));

            CombineStats(item.value, groupItem);
            await stats.Set(new(0, rangeId), groupItem);
          }
          finally
          {
            ReleaseRange(rangeId, handle);
          }

          await stats.Remove(item.id);
        });

    void CombineStats(Memory<Stats> source, Memory<Stats> target)
    {
      var sourceSpan = source.Span;
      var targetSpan = target.Span;

      for(var i = 0; i < sourceSpan.Length; ++i)
      {
        ref var si = ref sourceSpan[i];
        ref var ti = ref targetSpan[i];

        ti = new()
        {
          Mean = (float)si.Count / (si.Count + ti.Count) * si.Mean +
            (float)ti.Count / (si.Count + ti.Count) * ti.Mean,
          Stdev2N = si.Stdev2N + ti.Stdev2N,
          Count = si.Count + ti.Count,
          IdN = si.IdN + ti.IdN
        };
      }
    }

    async ValueTask ForEach<T>(
      IAsyncEnumerable<T> items, 
      Func<T, ValueTask> action)
    {
      if (parallel)
      {
        await Parallel.ForEachAsync(
          items,
          (item, cancellationToken) => action(item));
      }
      else
      {
        await foreach(var item in items)
        {
          await action(item);
        }
      }
    }
  }

  /// <summary>
  /// Gets <see cref="IndexRange"/> for range id and <see cref="RangeValue"/>.
  /// </summary>
  /// <param name="rangeId">A range id.</param>
  /// <param name="range">A range value.</param>
  /// <returns></returns>
  public static IndexRange GetRange(long rangeId, RangeValue range) =>
    range.Dimension switch 
    {
      -1 => new()
      {
        RangeId = rangeId,
        Id = range.Id,
      },
      -2 => new()
      {
        RangeId = rangeId,
        LowRangeId = rangeId * 2 + 1,
        HighRangeId = rangeId * 2 + 2
      },
      _ => new()
      {
        RangeId = rangeId,
        Dimension = range.Dimension,
        Mid = range.Mid,
        LowRangeId = rangeId * 2 + 1,
        HighRangeId = rangeId * 2 + 2
      }
    };

  public static async IAsyncEnumerable<(long rangeId, RangeValue range)> Build(
    IAsyncEnumerable<(long id, Memory<float> vector)> points,
    Func<IRangeStore> storeFactory)
  {
    Stats[]? stats = null;
    Stack<(long rangeId, IRangeStore store)> stack = new();

    stack.Push((0, new RangeStore { points = points }));

    try
    {
      while(stack.TryPop(out var item))
      {
        try
        {
          var count = 0L;

          await foreach(var (id, vector) in item.store.GetPoints())
          {
            if(count++ == 0)
            {
              stats ??= new Stats[vector.Length];
              InitStats(id, vector);
            }
            else
            {
              UpdateStats(id, vector);
            }
          }

          if (count == 0)
          {
            continue;
          }

          var (match, index) = stats!.
            Select((stats, index) => (stats, index)).
            MaxBy(item => item.stats.Stdev2N);

          RangeValue range = count == 1 ?
            new() { Dimension = -1, Id = (long)stats![0].IdN } :
            match.Stdev2N == 0 ?
              new() { Dimension = -2, Id = (long)(match.IdN / match.Count) } :
              new() { Dimension = index, Mid = match.Mean };

          var rangeId = item.rangeId;

          yield return (rangeId, range);

          if (count == 1)
          {
            continue;
          }

          var low = storeFactory();

          stack.Push((rangeId * 2 + 1, low));

          var high = storeFactory();

          stack.Push((rangeId * 2 + 2, high));

          await foreach(var (id, vector) in item.store.GetPoints())
          {
            var isHigh = range.Dimension < 0 ?
              id > range.Id :
              vector.Span[range.Dimension] >= range.Mid;

            await (isHigh ? high : low).Add(id, vector);
          }
        }
        finally
        {
          await item.store.DisposeAsync();
        }
      }
    }
    finally
    {
      while(stack.TryPop(out var item))
      {
        await item.store.DisposeAsync();
      }
    }

    void InitStats(long id, Memory<float> point)
    {
      var span = point.Span;

      for(var i = 0; i < span.Length; ++i)
      {
        stats[i] = new()
        {
          Mean = span[i],
          Stdev2N = 0,
          Count = 1,
          IdN = id
        };
      }
    }

    void UpdateStats(long id, Memory<float> point)
    {
      var span = point.Span;

      for(var i = 0; i < span.Length; ++i)
      {
        var value = span[i];
        ref var item = ref stats![i];
        var pa = item.Mean;
        var pq = item.Stdev2N;
        var count = item.Count + 1;
        var a = pa + (value - pa) / count;
        var q = pq + (value - pa) * (value - a);

        item = new()
        {
          Mean = a,
          Stdev2N = q,
          Count = count,
          IdN = item.IdN + id
        };
      }
    }
  }

  private class RangeStore: IRangeStore
  {
    public IAsyncEnumerable<(long id, Memory<float> vector)> points;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ValueTask Add(long id, Memory<float> vector)
    {
      throw new NotImplementedException();
    }

    public IAsyncEnumerable<(long id, Memory<float> vector)> GetPoints() => points;
  }
}
