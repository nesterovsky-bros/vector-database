namespace NesterovskyBros.VectorIndex;

/// <summary>
/// A range spliting space by a specified dimension into two subregions.
/// </summary>
public readonly record struct RangeValue
{
  /// <summary>
  /// Index of dimension being indexed.
  /// </summary>
  public int Dimension { get; init; }

  /// <summary>
  /// A middle point of range.
  /// </summary>
  public float Mid { get; init; }

  /// <summary>
  /// Optional point id fit into the range.
  /// </summary>
  public long Id { get; init; }
}
