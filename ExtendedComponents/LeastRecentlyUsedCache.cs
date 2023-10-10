using System.Runtime.CompilerServices;

namespace ExtendedComponents;

// From: https://stackoverflow.com/questions/754233/is-it-there-any-lru-implementation-of-idictionary

public class LeastRecentlyUsedCache<TKey,TValue>
{
    private readonly int _capacity;
    private readonly Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>> _cacheMap = new Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>>();
    private readonly LinkedList<KeyValuePair<TKey, TValue>> _lruList = new();

    public LeastRecentlyUsedCache(int capacity)
    {
        _capacity = capacity;
    }

    [MethodImpl(MethodImplOptions.Synchronized)]
    public virtual TValue? Get(TKey key)
    {
        LinkedListNode<KeyValuePair<TKey, TValue>>? node;
        if (!_cacheMap.TryGetValue(key, out node)) return default(TValue);
        TValue value = node.Value.Value;
        _lruList.Remove(node);
        _lruList.AddLast(node);
        return value;
    }

    [MethodImpl(MethodImplOptions.Synchronized)]
    public virtual void Add(TKey key, TValue val)
    {
        if (_cacheMap.TryGetValue(key, out var existingNode))
        {
            _lruList.Remove(existingNode);
        }
        else if (_cacheMap.Count >= _capacity)
        {
            RemoveFirst();
        }

        KeyValuePair<TKey, TValue> cacheItem = new KeyValuePair<TKey, TValue>(key, val);
        LinkedListNode<KeyValuePair<TKey, TValue>> node = new LinkedListNode<KeyValuePair<TKey, TValue>>(cacheItem);
        _lruList.AddLast(node);
        
        _cacheMap[key] = node;
    }

    private void RemoveFirst()
    {
        // Remove from LRUPriority
        var node = _lruList.First;
        if (node == null) return;
        _lruList.RemoveFirst();

        // Remove from cache
        _cacheMap.Remove(node.Value.Key);
    }
}

public class ThreadSafeLeastRecentlyUsedCache<TKey, TValue> : LeastRecentlyUsedCache<TKey, TValue>
{
    public ThreadSafeLeastRecentlyUsedCache(int capacity) : base(capacity)
    {
    }
    public override TValue? Get(TKey key)
    {
        lock (this)
        {
            return base.Get(key);
        }
    }

    public override void Add(TKey key, TValue val)
    {
        lock (this)
        {
            base.Add(key, val);
        }
    }
}