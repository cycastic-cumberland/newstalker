namespace ExtendedComponents;

public class LogSegment
{
    public enum LogSegmentType
    {
        Message,
        Exception
    }
    public long Timestamp;
    public string Header = "";
    public string Message = "";
    public LogSegmentType LogType = LogSegmentType.Message;
    public object? Metadata = null;
    
    public LogSegment(){}
    public LogSegment(string header, string message, LogSegmentType logSegmentType, object? metadata = null)
    {
        Timestamp = DateTimeOffset.Now.ToUnixTimeMilliseconds();
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
            $"[{DateTimeOffset.FromUnixTimeMilliseconds(log.Timestamp):yyyy-MM-dd HH:mm:ss}] {log.Header}: {log.Message}";
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