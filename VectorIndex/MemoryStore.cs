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
  public ValueTask<V?> Get(K id) =>
      new(data.TryGetValue(id, out var value) ? value : default);

  /// <inheritdoc/>
  public ValueTask Set(K id, V value)
  {
    data[id] = value;

    return ValueTask.CompletedTask;
  }

  /// <inheritdoc/>
  public ValueTask Remove(K id)
  {
    data.Remove(id, out _);

    return ValueTask.CompletedTask;
  }

  /// <inheritdoc/>
  public IAsyncEnumerable<(K id, V value)> GetItems() =>
      data.Select(entry => (entry.Key, entry.Value)).ToAsyncEnumerable();

  private readonly ConcurrentDictionary<K, V> data = new();
}
