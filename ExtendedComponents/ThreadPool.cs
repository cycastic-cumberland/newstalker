namespace ExtendedComponents;

public class ThreadPool : IDisposable
{
    private class PooledTask
    {
        public readonly TaskCompletionSource<bool> Source;
        public readonly Action TaskAction;
        
        public PooledTask(Action action)
        {
            TaskAction = action;
            Source = new();
        }

        public async Task Wait()
        {
            await Source.Task;
        }
    }
    private readonly Dictionary<ulong, Thread> _threads = new();
    private readonly Queue<PooledTask> _taskQueue = new();
    private readonly object _poolConditionalLock = new();
    private uint _terminationFlag = 0;
    private ulong _currentId = 1;
    private int _activeThread;

    public Thread? GetWorker(ulong id)
    {
        return !_threads.TryGetValue(id, out var t) ? null : t;
    }
    public ulong AllocateThread()
    {
        lock (_poolConditionalLock)
        {
            var newThread = new Thread(() => Start(_currentId));
            newThread.Start();
            _threads[_currentId] = newThread;
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
                _threads[_currentId] = newThread;
            }
        }
    }

    public void BatchRemoveThread(uint amount)
    {
        if (_terminationFlag > 0 || amount == 0) return;
        lock (_poolConditionalLock)
        {
            _terminationFlag = _threads.Count < amount ? (uint)_threads.Count : amount;
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
            PooledTask pooled = new(task);
            _taskQueue.Enqueue(pooled);
            Monitor.Pulse(_poolConditionalLock);
            return pooled.Source.Task;
        }
    }
    public void StopAll(bool cancelAll = false)
    {
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
        }
        foreach (var (_, thread) in _threads)
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
    
    private static void Resolve(PooledTask task)
    {
        try
        {
            task.TaskAction.Invoke();
            task.Source.SetResult(true);
        }
        catch (Exception ex)
        {
            task.Source.SetException(ex);
        }
    }
    
    private void Start(ulong myId)
    {
        Enter();
        while (true)
        {
            PooledTask task;

            lock (_poolConditionalLock)
            {
                // Wait for a task or termination flag.
                while (_taskQueue.Count == 0 && _terminationFlag == 0)
                    Wait();
                
                if (_terminationFlag > 0)
                {
                    _terminationFlag--;
                    _threads.Remove(myId);
                    Exit();
                    return;
                }

                // Dequeue a task.
                task = _taskQueue.Dequeue();
            }

            // Execute the task.
            Resolve(task);
        }
    }

    public void Dispose()
    {
        StopAll(true);
    }
}