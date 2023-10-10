namespace ExtendedComponents;

public class CommandQueue : IDisposable
{
    public enum ServerState
    {
        Operating,
        Cancelled,
        Flushed
    }
    private abstract class AbstractQueuedClass
    {
        public readonly TaskCompletionSource<bool> Source = new();
        public abstract Task InvokeAsync();
        public async Task Wait()
        {
            await Source.Task;
        }
    }
    private class QueuedActionTask : AbstractQueuedClass
    {
        private readonly Action _taskAction;
        
        public QueuedActionTask(Action action)
        {
            _taskAction = action;
        }

        public override Task InvokeAsync()
        {
            _taskAction();
            return Task.CompletedTask;
        }
    }
    private class QueuedAsyncTask : AbstractQueuedClass
    {
        private readonly Func<Task> _taskAction;
        
        public QueuedAsyncTask(Func<Task> action)
        {
            _taskAction = action;
        }

        public override async Task InvokeAsync() => await _taskAction();
    }

    private ServerState _serverState = ServerState.Operating;
    private readonly Queue<AbstractQueuedClass> _taskQueue = new();
    private readonly object _conditionalLock = new();
    private readonly Thread _serverTask;

    public CommandQueue()
    {
        _serverTask = new Thread(Start);
        _serverTask.Start();
    }

    private static async Task Resolve(AbstractQueuedClass task)
    {
        try
        {
            await task.InvokeAsync();
            task.Source.SetResult(true);
        }
        catch (Exception ex)
        {
            task.Source.SetException(ex);
        }
    }
    
    private void Start()
    {
        while (_serverState == ServerState.Operating)
        {
            AbstractQueuedClass task;
            lock (_conditionalLock)
            {
                while (_taskQueue.Count == 0 && _serverState == ServerState.Operating)
                    Monitor.Wait(_conditionalLock);
                switch (_serverState)
                {
                    case ServerState.Cancelled:
                    {
                        foreach (var t in _taskQueue)
                        {
                            t.Source.SetCanceled();
                        }
                        _taskQueue.Clear();
                    
                        return;
                    }
                    case ServerState.Flushed:
                    {
                        foreach (var t in _taskQueue)
                        {
                            Resolve(t).Wait();
                        }
                        _taskQueue.Clear();

                        return;
                    }
                    default:
                        task = _taskQueue.Dequeue();
                        break;
                }
            }

            Resolve(task).Wait();
        }
    }

    public Task DispatchTask(Action action)
    {
        lock (_conditionalLock)
        {
            if (_serverState != ServerState.Operating)
                throw new TaskCanceledException();
            QueuedActionTask queued = new(action);
            _taskQueue.Enqueue(queued);
            Monitor.Pulse(_conditionalLock);
            return queued.Source.Task;
        }
    }
    public Task DispatchTask(Func<Task> action)
    {
        lock (_conditionalLock)
        {
            if (_serverState != ServerState.Operating)
                throw new TaskCanceledException();
            QueuedAsyncTask queued = new(action);
            _taskQueue.Enqueue(queued);
            Monitor.Pulse(_conditionalLock);
            return queued.Source.Task;
        }
    }

    public void SyncTask(Action action)
    {
        DispatchTask(action).Wait();
    }
    
    public void SyncTask(Func<Task> action)
    {
        DispatchTask(action).Wait();
    }

    private void SetState(ServerState type)
    {
        lock (_conditionalLock)
        {
            _serverState = type;
            Monitor.Pulse(_conditionalLock);
        }   
    }

    public void Flush()
    {
        SetState(ServerState.Flushed);
        _serverTask.Join();
    }
    public void Dispose()
    {
        SetState(ServerState.Cancelled);
        _serverTask.Join();
    }
}