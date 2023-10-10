namespace ExtendedComponents;

public class AutoLogger : IDisposable
{
    private readonly long _epoch;
    private readonly string _header;
    private readonly string _methodName;
    private readonly LoggingServer _logger;

    public AutoLogger(string header, string methodName, LoggingServer logger)
    {
        _epoch = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        _header = header;
        _methodName = methodName;
        _logger = logger;
    }
    
    public void Dispose()
    {
        _logger.Write($"{_header}:{Environment.CurrentManagedThreadId}", 
            $"{_methodName} exited after {DateTimeOffset.Now.ToUnixTimeMilliseconds() - _epoch} ms",
            LogSegment.LogSegmentType.Message);
    }
}

public class AutoLoggerFactory
{
    private readonly string _header;
    private readonly LoggingServer _logger;

    public AutoLoggerFactory(string header, LoggingServer logger)
    {
        _header = header;
        _logger = logger;
    }

    public AutoLogger Create(string methodName)
        => new(_header, methodName, _logger);
}
