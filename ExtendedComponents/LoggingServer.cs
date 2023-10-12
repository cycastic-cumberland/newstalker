namespace ExtendedComponents;

public struct LogSegment
{
    public enum LogSegmentType
    {
        Message = 1,
        Exception = 2
    }
    public DateTime Timestamp;
    public string Header = "";
    public string Message = "";
    public LogSegmentType LogType = LogSegmentType.Message;
    public object? Metadata = null;
    
    public LogSegment(){}
    public LogSegment(string header, string message, LogSegmentType logSegmentType, object? metadata = null)
    {
        Timestamp = DateTime.UtcNow;
        Header = header;
        Message = message;
        LogType = logSegmentType;
        Metadata = metadata;
    }
}

public abstract class LoggingServerDelegate : IDisposable
{
    public abstract void Write(LogSegment log);
    public virtual void Dispose(){}
}

public class StdLoggingServerDelegate : LoggingServerDelegate
{
    public override void Write(LogSegment log)
    {
        var msg =
            $"[{log.Timestamp:yyyy-MM-dd HH:mm:ss} UTC] {log.Header}: {log.Message}";
        if (log.LogType == LogSegment.LogSegmentType.Exception)
            Console.Error.WriteLine(msg);
        else
            Console.WriteLine(msg);
    }
}

public class LoggingServer : IDisposable
{
    private readonly LinkedList<LoggingServerDelegate> _delegates = new();
    private readonly CommandQueue _queue = new();
    
    public LoggingServer() {}

    public LoggingServer(IEnumerable<LoggingServerDelegate> loggers)
    {
        EnrollDelegates(loggers);
    }
    
    public void Dispose()
    {
        _queue.Dispose();
        foreach (var instance in _delegates)
        {
            instance.Dispose();
        }
    }
    public void EnrollDelegate(LoggingServerDelegate instance)
    {
        _queue.SyncTask(() => _delegates.AddLast(instance));
    }
    public void EnrollDelegates(IEnumerable<LoggingServerDelegate> loggers)
    {
        foreach (var logger in loggers)
        {
            EnrollDelegate(logger);
        }
    }
    public void Write(string header, string message, LogSegment.LogSegmentType type, object? metadata)
    {
        _queue.DispatchTask(() =>
        {
            var log = new LogSegment(header, message, type, metadata);
            foreach (var instance in _delegates)
            {
                try
                {
                    instance.Write(log);
                }
                catch (Exception)
                {
                    // Ignored
                }
            }
        });
    }

    public void Write(string header, string message, LogSegment.LogSegmentType type)
        => Write(header, message, type, null);
    public void Write(string message, LogSegment.LogSegmentType type = LogSegment.LogSegmentType.Message)
    {
        Write("", message, type);
    }
}