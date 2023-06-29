namespace NesterovskyBros.VectorIndex;

/// <summary>
/// A stats key.
/// </summary>
public readonly record struct StatsKey
{
  /// <summary>
  /// A grouping segment.
  /// </summary>
  public int Segment { get; init; }

  /// <summary>
  /// A range id.
  /// </summary>
  public long RangeID { get; init; }
}
