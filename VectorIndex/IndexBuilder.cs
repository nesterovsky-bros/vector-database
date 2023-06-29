using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;

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
    IStore<long, float[]> points,
    IStore<long, IndexRange> ranges,
    IStore<long, long> pointRanges,
    IStore<StatsKey, Stats[]> stats)
  {
    long count = 0;

    // Initialize point ranges.
    await foreach(var (key, _) in points.GetItems())
    {
      ++count;
      await pointRanges.Set(key, 0);
    }

    var segments =
      new ConcurrentDictionary<long, ImmutableDictionary<long, int>>();
    var rangeLocks = new ConcurrentDictionary<long,
      (SemaphoreSlim semaphore, int refcount)>();

    while(count > 0)
    {
      // Calculate stats.
      var singleSegment = true;

      await Parallel.ForEachAsync(
        pointRanges.GetItems(),
        async (item, cancellationToken) =>
        {
          var (id, rangeId) = item;
          var (segment, ids) = GetSegment(id, rangeId);
          var point = await points.Get(id);

          if (ids.Count > 1)
          {
            singleSegment = false;
          }

          var key = new StatsKey { Segment = segment, RangeID = rangeId };
          var statsItem = await stats.Get(key);

          if (statsItem == null)
          {
            statsItem = new Stats[point!.Length];

            for(var i = 0; i < point.Length; ++i)
            {
              statsItem[i] = new()
              {
                Mean = point[i],
                Stdev2N = 0,
                Count = 1,
                IdN = id
              };
            }
          }
          else
          {
            for(var i = 0; i < point!.Length; ++i)
            {
              var value = point[i];
              var pa = statsItem[i].Mean;
              var pq = statsItem[i].Stdev2N;
              var count = statsItem[i].Count + 1;
              var a = pa + (value - pa) / count;
              var q = pq + (value - pa) * (value - a);

              statsItem[i] = new()
              {
                Mean = a,
                Stdev2N = q,
                Count = count,
                IdN = statsItem[i].IdN + id
              };
            }
          }

          await stats.Set(key, statsItem);

          ReleaseSegment(id, rangeId, ids);
        });

      if (!singleSegment)
      {
        await Parallel.ForEachAsync(
          stats.GetItems(),
          async (item, cancellationToken) =>
          {
            if (item.key.Segment == 0)
            {
              return;
            }

            var rangeId = item.key.RangeID;
            var statsItem = item.value;
            var handle = await LockRange(rangeId);

            try
            {
              var groupKey = new StatsKey { Segment = 0, RangeID = rangeId };
              var groupItem = await stats.Get(groupKey);

              for(var i = 0; i < groupItem!.Length; ++i)
              {
                var si = statsItem[i];
                var gi = groupItem[i];

                groupItem[i] = new()
                {
                  Mean = (float)gi.Count / (gi.Count + si.Count) * gi.Mean +
                    (float)si.Count / (gi.Count + si.Count) * si.Mean,
                  Stdev2N = gi.Stdev2N + si.Stdev2N,
                  Count = gi.Count + si.Count,
                  IdN = gi.IdN + si.IdN
                };
              }

              await stats.Set(groupKey, groupItem);
              await stats.Remove(item.key);
            }
            finally
            {
              ReleaseRange(rangeId, handle);
            }
          });
        }

      // Select next ranges.
      await Parallel.ForEachAsync(
        stats.GetItems(),
        async (item, cancellationToken) =>
        {
          var rangeId = item.key.RangeID;

          var (match, index) = item.value.
            Select((item, index) => (item, index)).
            MaxBy(item => item.item.Stdev2N);

          if (match.Count == 1)
          {
            var id = (long)match.IdN;

            await ranges.Set(rangeId, new() { RangeId = rangeId, Id = id });
            await pointRanges.Remove(id);
            Interlocked.Decrement(ref count);
          }
          else if (match.Stdev2N == 0)
          {
            await ranges.Set(rangeId, new()
            {
              RangeId = rangeId,
              LowRangeId = rangeId * 2 + 1,
              HighRangeId = rangeId * 2 + 2,
              Id = (long)(match.IdN / match.Count)
            });
          }
          else
          {
            await ranges.Set(rangeId, new()
            {
              RangeId = rangeId,
              Dimension = index,
              Mid = match.Mean,
              LowRangeId = rangeId * 2 + 1,
              HighRangeId = rangeId * 2 + 2
            });
          }

          await stats.Remove(item.key);
        });

      if (count == 0)
      {
        break;
      }

      // Update point ranges;
      await Parallel.ForEachAsync(
        pointRanges.GetItems(),
        async (item, cancellationToken) =>
        {
          var (pointId, rangeId) = item;
          var range = await ranges.Get(rangeId);
          long nextRangeId;

          if (range.Dimension == null)
          {
            nextRangeId = pointId <= range.Id ?
              range.LowRangeId!.Value : range.HighRangeId!.Value;
          }
          else
          {
            var point = await points.Get(pointId);

            nextRangeId = point![range.Dimension.Value] < range.Mid!.Value ?
              range.LowRangeId!.Value : range.HighRangeId!.Value;
          }

          await pointRanges.Set(pointId, nextRangeId);
        });
    }

    (int segment, ImmutableDictionary<long, int> ids) GetSegment(
      long id, 
      long rangeId)
    {
      var ids = segments.AddOrUpdate(
        rangeId,
        rangeId => ImmutableDictionary<long, int>.Empty.Add(id, 0),
        (rangeId, segments) =>
        {
          for(var i = 0; ; ++i)
          {
            if (!segments.ContainsValue(i))
            {
              return segments.Add(id, i);
            }
          }
        });

      var segment = ids[id];

      return (segment, ids);
    }

    void ReleaseSegment(
      long id, 
      long rangeId, 
      ImmutableDictionary<long, int> ids)
    {
      while(true)
      {
        if (ids.Count == 1)
        {
          if (segments.TryRemove(new(rangeId, ids)))
          {
            break;
          }
        }
        else
        {
          if (segments.TryUpdate(rangeId, ids.Remove(id), ids))
          {
            break;
          }
        }

        ids = segments[rangeId];
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
  }
}
