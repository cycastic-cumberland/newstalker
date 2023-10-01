namespace ExtendedComponents;

public class CommandQueue : IDisposable
{
    public enum ServerState
    {
        Operating,
        Cancelled,
        Flushed
    }
    private class QueuedTask
    {
        public readonly TaskCompletionSource<bool> Source;
        public readonly Action TaskAction;
        
        public QueuedTask(Action action)
        {
            TaskAction = action;
            Source = new();
        }
    }

    private ServerState _serverState = ServerState.Operating;
    private readonly Thread _server;
    private readonly Queue<QueuedTask> _taskQueue = new();
    private readonly object _conditionalLock = new();

    public CommandQueue()
    {
        _server = new Thread(Start);
        _server.Start();
    }

    private static void Resolve(QueuedTask task)
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
    
    private void Start()
    {
        while (_serverState == ServerState.Operating)
        {
            QueuedTask task;
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
                            Resolve(t);
                        }
                        _taskQueue.Clear();

                        return;
                    }
                    default:
                        task = _taskQueue.Dequeue();
                        break;
                }
            }
            Resolve(task);
        }
    }

    public Task DispatchTask(Action action)
    {
        lock (_conditionalLock)
        {
            if (_serverState != ServerState.Operating)
                throw new TaskCanceledException();
            QueuedTask queued = new(action);
            _taskQueue.Enqueue(queued);
            Monitor.Pulse(_conditionalLock);
            return queued.Source.Task;
        }
    }

    public void SyncTask(Action action)
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
        _server.Join();
    }
    public void Dispose()
    {
        SetState(ServerState.Cancelled);
        _server.Join();
    }
}