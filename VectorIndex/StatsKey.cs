namespace NesterovskyBros.VectorIndex;

/// <summary>
/// A stats key.
/// </summary>
/// <param name="Segment">A grouping segment.</param>
/// <param name="RangeID">A range id.</param>
public readonly record struct StatsKey(int Segment, long RangeID);
