namespace ExtendedComponents;

public abstract class ObjectPool<T> : IDisposable, IAsyncDisposable
{
    public interface IObjectPoolInstance : IDisposable
    {
        public T GetInstance();
    }

    public abstract IObjectPoolInstance Borrow();
    protected abstract void Return(T obj);

    public virtual void Dispose() {}

    public virtual ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}

public class AsynchronousObjectPool<T> : ObjectPool<T>
{
    public class AsynchronousObjectPoolInstance : IObjectPoolInstance
    {
        private readonly AsynchronousObjectPool<T> _parent;
        private readonly T _instance;

        public AsynchronousObjectPoolInstance(AsynchronousObjectPool<T> parent, T instance)
        {
            _parent = parent;
            _instance = instance;
        }

        public T GetInstance() => _instance;

        public void Dispose()
        {
            _parent.Return(_instance);
        }
    }

    private readonly Func<T> _spawner;
    private readonly CommandQueue _commandQueue = new();
    private readonly Queue<T> _objectQueue = new();

    public AsynchronousObjectPool(Func<T> spawner)
    {
        _spawner = spawner;
    }
    
    public override void Dispose()
    {
        _commandQueue.Dispose();
        if (!typeof(T).IsAssignableTo(typeof(IDisposable))) return;
        foreach (var obj in _objectQueue)
        {
            ((IDisposable)obj!).Dispose();
        }
        _objectQueue.Clear();
    }

    public override async ValueTask DisposeAsync()
    {
        _commandQueue.Dispose();
        if (typeof(T).IsAssignableTo(typeof(IDisposable)))
        {
            foreach (var obj in _objectQueue)
            {
                ((IDisposable)obj!).Dispose();
            }
        }
        else if (typeof(T).IsAssignableTo(typeof(IAsyncDisposable)))
        {
            foreach (var obj in _objectQueue)
            {
                await ((IAsyncDisposable)obj!).DisposeAsync();
            }
        }
        _objectQueue.Clear();
    }

    public override AsynchronousObjectPoolInstance Borrow()
    {
        T? ret = default;
        _commandQueue.SyncTask(() => { ret = _objectQueue.Count == 0 ? _spawner() : _objectQueue.Dequeue(); });
        return new AsynchronousObjectPoolInstance(this, ret!);
    }

    protected override void Return(T obj)
    {
        _commandQueue.DispatchTask(() =>
        {
            _objectQueue.Enqueue(obj);
        });
    }
}

public class SynchronousObjectPool<T> : ObjectPool<T>
{
    private readonly Queue<T> _objectQueue = new();
    private readonly Func<T> _spawner;

    public SynchronousObjectPool(Func<T> spawner)
    {
        _spawner = spawner;
    }
    
    public class SynchronousObjectPoolInstance : IObjectPoolInstance
    {
        private readonly SynchronousObjectPool<T> _parent;
        private readonly T _instance;
        
        public SynchronousObjectPoolInstance(SynchronousObjectPool<T> parent, T instance)
        {
            _parent = parent;
            _instance = instance;
        }

        public void Dispose()
        {
            _parent.Return(_instance);
        }

        public T GetInstance() => _instance;
    }
    
    public override void Dispose()
    {
        lock (this)
        {
            if (!typeof(T).IsAssignableTo(typeof(IDisposable))) return;
            foreach (var obj in _objectQueue)
            {
                (obj as IDisposable)?.Dispose();
            }
            _objectQueue.Clear();
        }
    }

    public override SynchronousObjectPoolInstance Borrow()
    {
        lock (this)
        {
            T ret = _objectQueue.Count == 0 ? _spawner() : _objectQueue.Dequeue();
            return new SynchronousObjectPoolInstance(this, ret);   
        }
    }

    protected override void Return(T obj)
    {
        lock (this)
        {
            _objectQueue.Enqueue(obj);
        }
    }
}

public class FiniteObjectPool<T> : ObjectPool<T>
{
    public class FiniteObjectPoolInstance : IObjectPoolInstance
    {
        private readonly FiniteObjectPool<T> _parent;
        private readonly T _instance;
        
        public FiniteObjectPoolInstance(FiniteObjectPool<T> parent, T instance)
        {
            _parent = parent;
            _instance = instance;
        }

        public void Dispose()
        {
            _parent.Return(_instance);
        }

        public T GetInstance() => _instance;
    }

    private readonly object _lock;
    private readonly uint _capacity;
    private T[] _objectQueue;
    private uint _iterator;
    private bool _terminated;

    public uint Capacity => _capacity;

    public FiniteObjectPool(Func<T> spawner, uint capacity)
    {
        if (capacity <= 0) throw new IndexOutOfRangeException("Capacity must be greater than 0");
        _capacity = capacity;
        _iterator = capacity;
        _objectQueue = new T[capacity + 1];
        _lock = new List<int>();
        // _objectQueue[0] = default!;
        for (uint i = 1; i <= capacity; i++)
        {
            _objectQueue[i] = spawner();
        }
    }

    public override void Dispose()
    {
        lock (_lock)
        {
            if (!typeof(T).IsAssignableTo(typeof(IDisposable)))
            {
                foreach (var obj in _objectQueue)
                {
                    ((IDisposable)obj!).Dispose();
                }
            }
            _objectQueue = Array.Empty<T>();
            _terminated = true;
            Monitor.PulseAll(_lock);
        }
    }
    
    public override IObjectPoolInstance Borrow()
    {
        lock (_lock)
        {
            while (_iterator == 0 && !_terminated)
                Monitor.Wait(_lock);
            if (_terminated) throw new TaskCanceledException();
            return new FiniteObjectPoolInstance(this, _objectQueue[_iterator--]);
        }
    }

    protected override void Return(T obj)
    {
        lock (_lock)
        {
            _objectQueue[++_iterator] = obj;
            Monitor.Pulse(_lock);
        }
    }
}
