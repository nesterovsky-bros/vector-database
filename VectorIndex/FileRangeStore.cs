using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NesterovskyBros.VectorIndex;

/// <summary>
/// A memory mapped file as a store for building vector index.
/// </summary>
public class FileRangeStore: IDisposable
{
  /// <summary>
  /// Creates a <see cref="MemoryMappedIndexTempStore"/> instance.
  /// </summary>
  /// <param name="count">Number of vectors.</param>
  /// <param name="dimensions">Dimension of vectors.</param>
  /// <param name="buffer">A buffer size.</param>
  public FileRangeStore(long count, short dimensions, int buffer = 10000)
  {
    this.dimensions = dimensions;
    this.buffer = buffer;
    capacity = checked(
      (Marshal.SizeOf<long>() + Marshal.SizeOf<float>() * dimensions) * 
        4 * count);
    highOffset = capacity / 2;
    file = MemoryMappedFile.CreateNew(null, capacity);
  }

  /// <summary>
  /// Releases resources.
  /// </summary>
  public void Dispose() => file.Dispose();

  /// <summary>
  /// Gets next <see cref="IRangeStore"/> instance.
  /// </summary>
  /// <param name="rangeId">A range id.</param>
  /// <param name="capacity">A capacity.</param>
  /// <returns>The <see cref="IRangeStore"/> instance.</returns>
  public IRangeStore NextStore(
    long rangeId,
    long capacity) => 
    new RangeStore(this, rangeId, capacity);

  private class RangeStore: IRangeStore
  {
    public RangeStore(
      FileRangeStore container,
      long rangeId,
      long capacity)
    {
      this.container = container;
      this.rangeId = rangeId;
      this.capacity = capacity;
    }

    public ValueTask Add(long id, Memory<float> vector)
    {
      if (vector.Length != container.dimensions)
      {
        throw new ArgumentException(
          "Invalid length of vector.", 
          nameof(vector));
      }

      if (data.Count >= container.buffer)
      {
        Flush();
      }

      data.Add((id, vector));
      ++count;

      return ValueTask.CompletedTask;
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    public async IAsyncEnumerable<(long id, Memory<float> vector)> GetPoints()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
    {
      if (stream != null)
      {
        stream.Position = 0;

        for(var i = 0L; i < count - data.Count; ++i)
        {
          var id = 0L;
          var vector = new float[container.dimensions];

          stream.Read(MemoryMarshal.CreateSpan(
            ref Unsafe.As<long, byte>(ref id),
            Marshal.SizeOf<long>()));
          stream.Read(
            MemoryMarshal.CreateSpan(
              ref Unsafe.As<float, byte>(ref vector[0]),
              Marshal.SizeOf<float>() * container.dimensions));

          yield return (id, vector);
        }
      }

      foreach(var item in data)
      {
        yield return item;
      }
    }

    public ValueTask DisposeAsync()
    {
      if (stream != null)
      { 
        stream.Dispose();

        if ((rangeId & 1) != 0)
        {
          container.lowOffset = start;
        }
        else
        {
          container.highOffset = start;
        }
      }

      return ValueTask.CompletedTask;
    }

    private void Flush()
    {
      if (stream == null)
      {
        start = 
          (rangeId & 1) != 0 ? container.lowOffset : container.highOffset;

        stream = container.file.CreateViewStream(
          start,
          (Marshal.SizeOf<long>() + 
            Marshal.SizeOf<float>() * container.dimensions) * capacity);
      }

      foreach(var (id, vector) in data)
      {
        var idRef = id;

        stream.Write(MemoryMarshal.CreateSpan(
          ref Unsafe.As<long, byte>(ref idRef),
          Marshal.SizeOf<long>()));
        stream.Write(
          MemoryMarshal.CreateSpan(
            ref Unsafe.As<float, byte>(ref vector.Span[0]),
            Marshal.SizeOf<float>() * container.dimensions));
      }

      data.Clear();

      var offset = start + stream.Position;

      if ((rangeId & 1) != 0)
      {
        container.lowOffset = offset;
      }
      else
      {
        container.highOffset = offset;
      }
    }

    private readonly long rangeId;
    private readonly FileRangeStore container;
    private readonly List<(long id, Memory<float> vector)> data = new();
    private readonly long capacity;
    private long start;
    private long count;
    private Stream? stream;
  }

  private readonly int buffer;
  private readonly short dimensions;
  private readonly long capacity;
  private readonly MemoryMappedFile file;
  private long lowOffset;
  private long highOffset;
}
