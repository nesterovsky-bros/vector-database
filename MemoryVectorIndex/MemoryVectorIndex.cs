using System.Collections;
using System.Numerics;
using System.Runtime.Intrinsics;

namespace NesterovskyBros.VectorIndex;

/// <summary>
/// A record index by normalized vectors, such 
/// that each their components lie in range [-1, 1].
/// All vectors must be of the same size.
/// </summary>
/// <typeparam name="R">A record type associated with vector.</typeparam>
public class MemoryVectorIndex<R>: IEnumerable<R>
{
  /// <summary>
  /// Creates a vector index.
  /// </summary>
  /// <param name="vectorSelector">
  /// A function returning vector for the record.
  /// </param>
  /// <param name="listThreshold">
  /// A threshold size to store records in list buckets.
  /// </param>
  public MemoryVectorIndex(
    Func<R, ReadOnlyMemory<float>> vectorSelector,
    int listThreshold = 10)
  {
    if (listThreshold <= 0)
    {
      throw new ArgumentException(
        "List threshold must be greater than zero.",
        nameof(listThreshold));
    }

    this.vectorSelector = vectorSelector;
    this.listThreshold = listThreshold;
  }

  /// <summary>
  /// Creates a vector index.
  /// </summary>
  /// <param name="records">A records to add to index.</param>
  /// <param name="vectorSelector">
  /// A function returning vector for the record.
  /// </param>
  /// <param name="listThreshold">
  /// A threshold size to store records in list buckets.
  /// </param>
  public MemoryVectorIndex(
    IEnumerable<R> records,
    Func<R, ReadOnlyMemory<float>> vectorSelector,
    int listThreshold = 10):
    this(vectorSelector, listThreshold)
  {
    foreach(var record in records)
    {
      Add(record);
    }
  }

  /// <summary>
  /// Number of records.
  /// </summary>
  public int Count { get; private set; }

  /// <inheritdoc/>
  public IEnumerator<R> GetEnumerator() =>
    records.Values.SelectMany(items => items).GetEnumerator();

  /// <inheritdoc/>
  IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

  /// <summary>
  /// Clears the index.
  /// </summary>
  public void Clear()
  {
    Count = 0;
    records.Clear();
    entries.Clear();
  }

  /// <summary>
  /// Adds a record to the index.
  /// </summary>
  /// <param name="record">A record to add.</param>
  public void Add(R record)
  {
    var vector = vectorSelector(record).Span;

    if (entries is [])
    {
      if (vector.Length == 0)
      {
        throw new ArgumentException("Invalid vector size.", nameof(record));
      }

      vectorSize = vector.Length;
      Count = 1;
      records[0] = [record];
      entries.Add((-1, -1));

      return;
    }

    if (vector.Length != vectorSize)
    {
      throw new ArgumentException("Invalid vector size.", nameof(record));
    }

    var index = 0;
    var center = new float[vector.Length];
    var step = new float[vector.Length];

    for(var depth = 0; depth < maxDepth; ++depth)
    {
      for(var i = 0; i < vector.Length; ++i)
      {
        var (low, high) = entries[index];

        if (vector[i] < center[i])
        {
          if (low >= 0)
          {
            center[i] -= depth == 0 ? step[i] = .5f : step[i] /= 2;
            index = low;

            continue;
          }

          if (high >= 0)
          {
            entries[index] = (entries.Count, high);
            records[entries.Count] = [record];
            entries.Add((-1, -1));
            ++Count;

            return;
          }
        }
        else
        {
          if (high >= 0)
          {
            center[i] += depth == 0 ? step[i] = .5f : step[i] /= 2;
            index = high;

            continue;
          }

          if (low >= 0)
          {
            entries[index] = (low, entries.Count);
            records[entries.Count] = [record];
            entries.Add((-1, -1));
            ++Count;

            return;
          }
        }

        // This is a leaf.
        var list = records[index];

        list.Add(record);
        ++Count;

        if (list.Count <= listThreshold || depth >= maxDepth - 1)
        {
          return;
        }

        records.Remove(index);

        // Split the list;
        List<R> lowList = [];

        for(; depth < maxDepth; ++depth)
        {
          for(; i < vector.Length; ++i)
          {
            for(var j = list.Count; j-- > 0;)
            {
              var item = list[j];

              if (vectorSelector(item).Span[i] < center[i])
              {
                lowList.Add(item);
                list.RemoveAt(j);
              }
            }

            if (lowList is [])
            {
              center[i] += depth == 0 ? step[i] = .5f : step[i] /= 2;
              entries[index] = (-1, entries.Count);
              index = entries.Count;
              entries.Add((-1, -1));
            }
            else if (list is [])
            {
              center[i] -= depth == 0 ? step[i] = .5f : step[i] /= 2;
              (lowList, list) = (list, lowList);
              entries[index] = (entries.Count, -1);
              index = entries.Count;
              entries.Add((-1, -1));
            }
            else
            {
              entries[index] = (entries.Count, entries.Count + 1);
              records[entries.Count] = lowList;
              records[entries.Count + 1] = list;
              entries.Add((-1, -1));
              entries.Add((-1, -1));

              return;
            }
          }
        }

        // Bad distribution, probably not normalized.
        records[index] = list;

        return;
      }
    }
  }

  /// <summary>
  /// Finds records in the index.
  /// </summary>
  /// <param name="vector">A vector for the neighborhood origin.</param>
  /// <param name="distance">An euclidian distance for the match.</param>
  /// <param name="predicate">A filter predicate.</param>
  /// <returns>A enumeration of matched record.</returns>
  /// <remarks>
  /// Index searches records and discards those that are too far, yet 
  /// predicate may recieve records that are still far enough for the match, 
  /// so predicate should verify the match.
  /// </remarks>
  public IEnumerable<R> Find(
    ReadOnlyMemory<float> vector,
    float distance,
    Func<R, ReadOnlyMemory<float>, bool> predicate)
  {
    if (entries is [])
    {
      yield break;
    }

    if (vector.Length != vectorSize)
    {
      throw new ArgumentException("Invalid vector size.", nameof(vector));
    }

    var index = 0;
    var center = new float[vector.Length];
    var step = new float[vector.Length];

    Stack<
      (
        int index, 
        int depth, 
        int i, 
        float center, 
        float step, 
        float length
      )> state = [];

    state.Push((0, 0, 0, 0, 1, distance * distance));

    while(state.TryPeek(out var item))
    {
      var i = item.i;
      (var prev, index) = (index, item.index);
      var (low, high) = entries[index];
      
      center[i] = item.center;
      step[i] = item.step;

      if (prev == high)
      {
        state.Pop();

        continue;
      }

      var depth = item.depth;
      var value = vector.Span[i];
      var delta = value - item.center;
      
      var prevDelta = depth == 0 ? 0 :
        Math.Max(
          delta > 0 ?
            value - item.center - item.step : 
            item.center - item.step - value,
          0);

      if (prev != low && low != -1)
      {
        var length = item.length;

        if (delta > 0)
        {
          length += (prevDelta - delta) * (prevDelta + delta);
        }

        if (length >= 0)
        {
          center[i] -= step[i] /= 2;

          if (++i == vectorSize)
          {
            i = 0;
            ++depth;
          }

          state.Push((low, depth, i, center[i], depth == 0 ? 1 : step[i], length));

          continue;
        }
      }

      if (high != -1)
      {
        var length = item.length;

        if (delta < 0)
        {
          length += (prevDelta - delta) * (prevDelta + delta);
        }

        if (length >= 0)
        {
          center[i] += step[i] /= 2;

          if (++i == vectorSize)
          {
            i = 0;
            ++depth;
          }

          state.Push((high, depth, i, center[i], depth == 0 ? 1 : step[i], length));
        }
        else
        {
          state.Pop();
        }
        
        continue;
      }

      state.Pop();
  
      if (low == -1)
      {
        foreach(var record in records[index])
        {
          if (predicate(record, vector))
          {
            yield return record;
          }
        }
      }
    }
  }

  /// <summary>
  /// Removes records from the index.
  /// </summary>
  /// <param name="vector">A vector for the neighborhood origin.</param>
  /// <param name="distance">An euclidian distance for the match.</param>
  /// <param name="predicate">A filter predicate.</param>
  /// <remarks>
  /// Index searches records and discards those that are too far, yet 
  /// predicate may recieve records that are still far enough for the match, 
  /// so predicate should verify the match.
  /// </remarks>
  public void Remove(
    ReadOnlyMemory<float> vector,
    float distance,
    Func<R, ReadOnlyMemory<float>, bool> predicate)
  {
    if (entries is [])
    {
      return;
    }

    if (vector.Length != vectorSize)
    {
      throw new ArgumentException("Invalid vector size.", nameof(vector));
    }

    var vectorSpan = vector.Span;
    var index = 0;
    var center = new float[vector.Length];
    var step = new float[vector.Length];
    Stack<(int index, int depth, int i, float center, float step, float length)> state = [];

    state.Push((0, 0, 0, 0, 1, distance * distance));

    while(state.TryPeek(out var item))
    {
      var i = item.i;

      center[i] = item.center;
      step[i] = item.step;

      (var prev, index) = (index, item.index);
      var (low, high) = entries[index];

      if (prev == high)
      {
        state.Pop();

        continue;
      }

      var depth = item.depth;
      var value = vectorSpan[i];
      var delta = value - item.center;

      if (prev != low && low != -1)
      {
        var length = item.length;

        if (delta > 0)
        { 
          length -= delta * delta;
        }

        if (length >= 0)
        {
          center[i] -= step[i] /= 2;

          if (++i == vectorSize)
          {
            i = 0;
            ++depth;
          }

          state.Push((low, depth, i, center[i], depth == 0 ? 1 : step[i], length));

          continue;
        }
      }

      if (high != -1)
      {
        var length = item.length;

        if (delta < 0)
        {
          length -= delta * delta;
        }

        if (length >= 0)
        {
          center[i] += step[i] /= 2;

          if (++i == vectorSize)
          {
            i = 0;
            ++depth;
          }

          state.Push((high, depth, i, center[i], depth == 0 ? 1 : step[i], length));
        }
        else
        {
          state.Pop();
        }

        continue;
      }

      if (low == -1)
      {
        state.Pop();

        var list = records[index];

        for(i = list.Count; i-- > 0;)
        {
          if (predicate(list[i], vector))
          {
            list.RemoveAt(i);
          }
        }

        if (list is [])
        {
          records.Remove(index);

          // NOTE: we do not consolidate lists here.
          while(state.TryPeek(out item))
          {
            (low, high) = entries[item.index];

            if (low == -1 || high == -1)
            {
              center[i] = item.center;
              step[i] = item.step;
              index = item.index;
              entries[item.index] = (-1, -1);
              state.Pop();

              continue;
            }

            entries[item.index] = low == index ? (-1, high) : (low, -1);

            break;
          }
        }
      }
    }
  }

  public IEnumerable<
    (
      int index,
      int parent,
      int depth,
      ReadOnlyMemory<float> center,
      ReadOnlyMemory<float> range,
      IReadOnlyList<R>? records
    )> IndexHierarchy
  {
    get
    {
      if (entries is [])
      {
        yield break;
      }

      var index = 0;
      var center = new float[vectorSize];
      var step = new float[vectorSize];
      Stack<(int index, int parent, int depth, int i, float center, float step)> state = [];

      Array.Fill(step, 1);

      state.Push((0, -1, 0, 0, 0, 1));

      while(state.TryPeek(out var item))
      {
        var i = item.i;

        center[i] = item.center;
        step[i] = item.step;

        (var prev, index) = (index, item.index);
        var (low, high) = entries[index];

        if (prev == high)
        {
          state.Pop();

          continue;
        }

        var depth = item.depth;

        if (prev != low && low != -1)
        {
          center[i] -= step[i] /= 2;

          if (++i == vectorSize)
          {
            i = 0;
            ++depth;
          }

          state.Push((low, index, depth, i, center[i], step[i]));

          yield return (low, index, depth, center, step, null);

          continue;
        }

        if (high != -1)
        {
          center[i] += step[i] /= 2;

          if (++i == vectorSize)
          {
            i = 0;
            ++depth;
          }

          state.Push((high, index, depth, i, center[i], depth == 0 ? 1 : step[i]));

          yield return (low, index, depth, center, step, null);

          continue;
        }

        if (low == -1)
        {
          state.Pop();

          yield return (index, item.parent, depth, center, step, records[index]);
        }
      }
    }
  }

  /// <summary>
  /// A function returning a vector for a record.
  /// </summary>
  private readonly Func<R, ReadOnlyMemory<float>> vectorSelector;

  /// <summary>
  /// A threshold size to store records in list buckets.
  /// </summary>
  private readonly int listThreshold;

  /// <summary>
  /// A size of vector;
  /// </summary>
  private int vectorSize;

  /// <summary>
  /// List of buckets.
  /// </summary>
  private readonly List<(int low, int high)> entries = [];

  /// <summary>
  /// Record lists by entries.
  /// </summary>
  private readonly Dictionary<int, List<R>> records = [];

  /// <summary>
  /// Max depth of vectors before going to list.
  /// </summary>
  private static readonly int maxDepth = ((IFloatingPoint<float>)0f).GetSignificandBitLength();
}
