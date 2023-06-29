using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Xml.Linq;

namespace NesterovskyBros.VectorIndex;

/// <summary>
/// A memory <see cref="IStore{K, V}"/> implementation.
/// </summary>
/// <typeparam name="K">A key type.</typeparam>
/// <typeparam name="V">A value type.</typeparam>
public class MemoryStore<K, V>: IStore<K, V>
  where K: struct
{
  /// <inheritdoc/>
  public ValueTask DisposeAsync() => ValueTask.CompletedTask;

  /// <inheritdoc/>
  public ValueTask<V?> Get(K key) =>
      new(data.TryGetValue(key, out var value) ? value : default);

  /// <inheritdoc/>
  public IAsyncEnumerable<(K key, V value)> GetItems() =>
      data.Select(entry => (entry.Key, entry.Value)).ToAsyncEnumerable();

  /// <inheritdoc/>
  public ValueTask Set(K key, V value)
  {
    data[key] = value;

    return ValueTask.CompletedTask;
  }

  /// <inheritdoc/>
  public ValueTask Remove(K key)
  {
    data.Remove(key, out _);

    return ValueTask.CompletedTask;
  }

  private readonly ConcurrentDictionary<K, V> data = new();
}
