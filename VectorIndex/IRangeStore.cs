namespace NesterovskyBros.VectorIndex;

/// <summary>
/// An interface encapsulating range store.
/// </summary>
public interface IRangeStore: IAsyncDisposable
{
  /// <summary>
  /// Adds a range to the store.
  /// </summary>
  /// <param name="id">A vector id.</param>
  /// <param name="vector">A vector.</param>
  /// <returns>A value task.</returns>
  ValueTask Add(long id, Memory<float> vector);

  /// <summary>
  /// Gets async enumerable of stored ranges.
  /// </summary>
  /// <returns>A enumerable of vectors in range.</returns>
  IAsyncEnumerable<(long id, Memory<float> vector)> 
    GetPoints();
}
