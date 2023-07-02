namespace NesterovskyBros.VectorIndex;

/// <summary>
/// <para>A memory implementation of <see cref="IRangeStore"/>.</para>
/// <para>Note that instances of this class are not thread safe.</para>
/// </summary>
public class MemoryRangeStore: IRangeStore
{
  /// <inheritdoc/>
  public ValueTask DisposeAsync() => ValueTask.CompletedTask;

  /// <inheritdoc/>
  public ValueTask Add(long id, Memory<float> vector)
  {
    data.Add((id, vector));

    return ValueTask.CompletedTask;
  }

  /// <inheritdoc/>
  public IAsyncEnumerable<(long id, Memory<float> vector)> GetPoints() => 
    data.ToAsyncEnumerable();

  private List<(long id, Memory<float> vector)> data = new();
}
