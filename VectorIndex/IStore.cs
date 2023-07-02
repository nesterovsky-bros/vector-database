﻿namespace NesterovskyBros.VectorIndex;

/// <summary>
/// An interface encapsulating store.
/// </summary>
/// <typeparam name="K">A key type.</typeparam>
public interface IStore<K, V>: IAsyncDisposable
  where K: struct
{
  /// <summary>
  /// Gets a value, if avalialbe, for the key.
  /// </summary>
  /// <param name="id">A key.</param>
  /// <returns>A value task that returns a value or null.</returns>
  ValueTask<V?> Get(K id);

  /// <summary>
  /// Sets a value for the key.
  /// </summary>
  /// <param name="id">A key.</param>
  /// <param name="value">A value.</param>
  /// <returns>A value task.</returns>
  ValueTask Set(K id, V value);

  /// <summary>
  /// Removes a value for the key.
  /// </summary>
  /// <param name="id">A key.</param>
  /// <returns>A value task.</returns>
  ValueTask Remove(K id);

  /// <summary>
  /// Get enumerable of all items.
  /// </summary>
  /// <returns>An asynchronous enumerable.</returns>
  IAsyncEnumerable<(K id, V value)> GetItems();
}