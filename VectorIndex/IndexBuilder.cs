using System;
using System.Collections.Concurrent;
using System.Drawing;
using System.Numerics;
using System.Text;

namespace NesterovskyBros.VectorIndex;

/// <summary>
/// An API to build vector index.
/// </summary>
public partial class IndexBuilder
{
  /// <summary>
  /// Gets range enumerations of points.
  /// </summary>
  /// <param name="points">A points enumeration.</param>
  /// <param name="storeFactory">
  /// A factory to create a temporary store of points. Called as:
  /// <code>storeFactory(rangeId, capacity)</code>.
  /// </param>
  /// <returns></returns>
  public static async IAsyncEnumerable<(long rangeId, RangeValue range)> Build(
    IAsyncEnumerable<(long id, Memory<float> vector)> points,
    Func<long, long, IRangeStore> storeFactory)
  {
    var iteration = 0L;
    var level = 0;

    Stats[]? stats = null;
    Stack<(long rangeId, IRangeStore store, bool max)> stack = new();

    stack.Push((0, new RangeStore { points = points }, true));

    try
    {
      while(stack.TryPop(out var item))
      {
        try
        {
          ++iteration;

          level = Math.Max(
            level, 
            64 - BitOperations.LeadingZeroCount((ulong)item.rangeId));

          if (iteration < 10 ||
            iteration < 1000 && iteration % 100 == 0 ||
            iteration < 10000 && iteration % 1000 == 0 ||
            iteration % 10000 == 0)
          {
            Console.WriteLine($"Process {iteration} ranges. Level {level}");
          }

          var count = 0L;

          await foreach(var (id, vector) in item.store.GetPoints())
          {
            if (count++ == 0)
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

          var max = item.max;
          
          var (match, index) = stats!.
            Select((stats, index) => (stats, index)).
            MaxBy(item => max ? item.stats.Stdev2N : -item.stats.Stdev2N);

          RangeValue range = count == 1 ?
            new() { Dimension = -1, Id = (long)stats![0].IdN } :
            new() 
            { 
              Dimension = index, 
              Mid = match.Mean, 
              Id = (long)(match.IdN / match.Count)
            };

          var rangeId = item.rangeId;

          yield return (rangeId, range);

          if (count == 1)
          {
            continue;
          }

          var lowRangeId = checked(rangeId * 2 + 1);
          var low = storeFactory(lowRangeId, count);

          try
          {
            var highRangeId = checked(rangeId * 2 + 2);
            var high = storeFactory(highRangeId, count);

            //var lowCount = 0L;

            try
            {
              await foreach(var (id, vector) in item.store.GetPoints())
              {
                var value = vector.Span[range.Dimension];

                if (value > range.Mid || value == range.Mid && id > range.Id)
                {
                  await high.Add(id, vector);
                }
                else
                {
                  await low.Add(id, vector);
                  //++lowCount;
                }
              }

              //stack.Push((lowRangeId, low, lowCount < count * 0.70710678118654752440084436210485));
              //stack.Push((highRangeId, high, lowCount > count * (1 - 0.70710678118654752440084436210485)));
              stack.Push((lowRangeId, low, !max));
              stack.Push((highRangeId, high, !max));
            }
            catch
            {
              await high.DisposeAsync();

              throw;
            }
          }
          catch
          {
            await low.DisposeAsync();

            throw;
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
