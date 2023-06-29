using System.Collections.Concurrent;

using FASTER.core;

namespace NesterovskyBros.VectorIndex;

/// <summary>
/// A <see cref="IStore{K, V}"/> backed by <see cref="FasterKV{Key, Value}"/>.
/// </summary>
/// <typeparam name="K">A key type.</typeparam>
/// <typeparam name="V">A value type.</typeparam>
public class FasterStore<K, V>: IStore<K, V>
  where K: struct
{
  /// <summary>
  /// <para>Creates a <see cref="FasterStore{K, V}"/> instance.</para>
  /// <para>
  /// Once constructor succeeds, the ownership of the settings
  /// is transferred to the new instance.
  /// </para>
  /// </summary>
  /// <param name="settings">
  /// A <see cref="FasterKVSettings{Key, Value}"/> instance.
  /// </param>
  public FasterStore(FasterKVSettings<K, V> settings)
  {
    this.settings = settings;
    store = new FasterKV<K, V>(settings);
  }

  /// <inheritdoc/>
  public ValueTask DisposeAsync()
  {
    foreach(var session in sessions)
    {
      session.Dispose();
    }

    sessions.Clear();
    store.Dispose();
    settings.Dispose();

    return ValueTask.CompletedTask;
  }

  /// <inheritdoc/>
  public ValueTask<V?> Get(K key) =>
    Run(async session =>
      (await session.ReadAsync(ref key)).Complete().output)!;

  /// <inheritdoc/>
  public async ValueTask Set(K key, V value) =>
    await Run(session => session.UpsertAsync(ref key, ref value));

  /// <inheritdoc/>
  public async ValueTask Remove(K key) =>
    await Run(session => session.DeleteAsync(ref key));

  /// <inheritdoc/>
  public IAsyncEnumerable<(K key, V value)> GetItems()
  {
    return items().ToAsyncEnumerable();

    IEnumerable<(K key, V value)> items()
    {
      var session = GetSession();

      try
      {
        using var iterator = session.Iterate();

        while(iterator.GetNext(out _, out var key, out var value))
        {
          yield return (key, value);
        }
      }
      finally
      {
        sessions.Push(session);
      }
    }
  }

  private async ValueTask<T> Run<T>(
    Func<
      ClientSession<K, V, V, V, Empty, IFunctions<K, V, V, V, Empty>>, 
      ValueTask<T>> func)
  {
    var session = GetSession();

    try
    {
      return await func(session);
    }
    finally
    {
      sessions.Push(session);
    }
  }

  private ClientSession<K, V, V, V, Empty, IFunctions<K, V, V, V, Empty>> 
    GetSession() =>
    sessions.TryPop(out var session) ? session :
      store.NewSession(new SimpleFunctions<K, V>());

  private readonly FasterKVSettings<K, V> settings;
  private readonly FasterKV<K, V> store;
  private readonly ConcurrentStack<
    ClientSession<K, V, V, V, Empty, IFunctions<K, V, V, V, Empty>>>
    sessions = new();
}
