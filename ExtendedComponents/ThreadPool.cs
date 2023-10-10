namespace ExtendedComponents;

public class ThreadPool : IDisposable
{
    private abstract class AbstractPooledClass
    {
        public readonly TaskCompletionSource<bool> Source = new();
        public abstract Task InvokeAsync();
        public async Task Wait()
        {
            await Source.Task;
        }
    }
    private class PooledActionTask : AbstractPooledClass
    {
        private readonly Action _taskAction;
        
        public PooledActionTask(Action action)
        {
            _taskAction = action;
        }

        public override Task InvokeAsync()
        {
            _taskAction();
            return Task.CompletedTask;
        }
    }
    private class PooledAsyncTask : AbstractPooledClass
    {
        private readonly Func<Task> _taskAction;
        
        public PooledAsyncTask(Func<Task> action)
        {
            _taskAction = action;
        }

        public override async Task InvokeAsync() => await _taskAction();
    }
    private Dictionary<ulong, Thread> _workers = new();
    private readonly Queue<AbstractPooledClass> _taskQueue = new();
    private readonly object _poolConditionalLock = new();
    private uint _terminationFlag = 0;
    private ulong _currentId = 1;
    private int _activeThread;

    public Thread? GetWorker(ulong id)
    {
        return !_workers.TryGetValue(id, out var t) ? null : t;
    }
    public ulong AllocateThread()
    {
        lock (_poolConditionalLock)
        {
            var newThread = new Thread(() => Start(_currentId));
            newThread.Start();
            _workers[_currentId] = newThread;
            return _currentId++;
        }
    }
    public void BatchAllocateThread(uint count = 3)
    {
        lock (_poolConditionalLock)
        {
            for (; _currentId < count; _currentId++)
            {
                var newThread = new Thread(() => Start(_currentId));
                newThread.Start();
                _workers[_currentId] = newThread;
            }
        }
    }

    public void BatchRemoveThread(uint amount)
    {
        if (_terminationFlag > 0 || amount == 0) return;
        lock (_poolConditionalLock)
        {
            _terminationFlag = _workers.Count < amount ? (uint)_workers.Count : amount;
            for (uint i = 0, s = _terminationFlag; i < s; i++)
                Monitor.Pulse(_poolConditionalLock);
        }
    }
    public ThreadPool(uint initialCount = 3)
    {
        BatchAllocateThread(initialCount);
    }
    public Task EnqueueTask(Action task)
    {
        lock (_poolConditionalLock)
        {
            PooledActionTask pooledAction = new(task);
            _taskQueue.Enqueue(pooledAction);
            Monitor.Pulse(_poolConditionalLock);
            return pooledAction.Source.Task;
        }
    }
    public Task EnqueueTask(Func<Task> task)
    {
        lock (_poolConditionalLock)
        {
            PooledAsyncTask pooledAction = new(task);
            _taskQueue.Enqueue(pooledAction);
            Monitor.Pulse(_poolConditionalLock);
            return pooledAction.Source.Task;
        }
    }
    public void StopAll(bool cancelAll = false)
    {
        var curr = _workers;
        lock (_poolConditionalLock)
        {
            _terminationFlag = uint.MaxValue;
            Monitor.PulseAll(_poolConditionalLock);
            if (cancelAll)
            {
                foreach (var task in _taskQueue)
                {
                    task.Source.SetCanceled();
                }
                _taskQueue.Clear();
            }

            _workers = new();
        }
        foreach (var (_, thread) in curr)
        {
            thread.Join();
        }
        
        _terminationFlag = 0;
    }

    private void Enter() => Interlocked.Increment(ref _activeThread);

    private void Wait()
    {
        Exit();
        Monitor.Wait(_poolConditionalLock);
        Enter();
    }

    // No need for locks here
    private void Exit() => _activeThread--;

    public int ActiveThreadCount => _activeThread;
    
    private static async Task Resolve(AbstractPooledClass actionTask)
    {
        try
        {
            await actionTask.InvokeAsync();
            actionTask.Source.SetResult(true);
        }
        catch (Exception ex)
        {
            actionTask.Source.SetException(ex);
        }
    }
    
    private void Start(ulong myId)
    {
        Enter();
        while (true)
        {
            AbstractPooledClass actionTask;

            lock (_poolConditionalLock)
            {
                // Wait for a task or termination flag.
                while (_taskQueue.Count == 0 && _terminationFlag == 0)
                    Wait();
                
                if (_terminationFlag > 0)
                {
                    _terminationFlag--;
                    _workers.Remove(myId);
                    Exit();
                    return;
                }

                // Dequeue a task.
                actionTask = _taskQueue.Dequeue();
            }

            // Execute the task.
            Resolve(actionTask).Wait();
        }
    }

    public void Dispose()
    {
        StopAll(true);
    }
}