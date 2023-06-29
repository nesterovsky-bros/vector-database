namespace NesterovskyBros.VectorIndex;

/// <summary>
/// An aggregation stats.
/// </summary>
public readonly record struct Stats
{
  /// <summary>
  /// A mean value
  /// </summary>
  public float Mean { get; init; }

  /// <summary>
  /// A stdev^2*N value.
  /// </summary>
  public float Stdev2N { get; init; }

  /// <summary>
  /// Number of items collected.
  /// </summary>
  public long Count { get; init; }

  /// <summary>
  /// Sum of ids to get mean Id value.
  /// </summary>
  public Int128 IdN { get; init; }
}
