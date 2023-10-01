namespace ExtendedComponents;

public abstract class ObjectPool<T> : IDisposable
{
    public interface IObjectPoolInstance : IDisposable
    {
        public T GetInstance();
    }

    public abstract IObjectPoolInstance Borrow();
    protected abstract void Return(T obj);

    public virtual void Dispose() {}
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
            (obj as IDisposable)?.Dispose();
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