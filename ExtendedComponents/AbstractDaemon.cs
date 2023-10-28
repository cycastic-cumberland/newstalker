namespace ExtendedComponents;

public abstract class AbstractDaemon : IDisposable, IDaemon
{
    private readonly CommandQueue _commandQueue = new();
    private readonly Thread _iterationThread;
    private readonly int _loopInterval;
    private bool _terminated;
    
    protected AbstractDaemon(uint loopInterval = 10)
    {
        _loopInterval = (int)(loopInterval <= 1000 ? loopInterval : 100);
        _iterationThread = new Thread(ServerLoop);
    }

    protected void StartIterator() => _iterationThread.Start();
    
    public virtual void Dispose()
    {
        _terminated = true;
        _iterationThread.Join();
        _commandQueue.Dispose();
    }

    public virtual void CloseDaemon()
    {
        Dispose();
    }
    
    protected virtual void CancelServerLoopInternal() {  }
    protected abstract bool IterateInternal();

    protected Task Dispatch(Action action) => _commandQueue.DispatchTask(action);
    
    protected void CancelServerLoopMaster()
    {
        _terminated = true;
        CancelServerLoopInternal();
        _iterationThread.Join();
    }
    
    private void ServerLoop()
    {
        bool lastIterationStatus = true;
        while (!_terminated && lastIterationStatus)
        {
            Thread.Sleep(_loopInterval);
            lastIterationStatus = IterateInternal();
        }
    }
}