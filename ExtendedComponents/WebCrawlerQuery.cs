namespace ExtendedComponents;

public abstract class WebCrawlerQuery
{
    public abstract Task QueueWebsiteToVisit(string url);
    public abstract Task WriteVisitedUrl(string url);
    public abstract Task BlackListUrl(string url);
    public abstract Task<bool> IsUrlVisited(string url);
    public abstract Task<bool> IsUrlBlackListed(string url);
    public abstract void SetRunning(bool state);
    public abstract bool IsRunning();
}

public abstract class WebCrawlerQueryFactory
{
    public class QueryWrapper : IDisposable
    {
        public readonly WebCrawlerQuery Query;
        private readonly WebCrawlerQueryFactory _host;

        public QueryWrapper(WebCrawlerQueryFactory host, WebCrawlerQuery instance)
        {
            _host = host;
            Query = instance;
        }

        public void Dispose()
        {
            _host.Return(Query);
        }
    }

    protected QueryWrapper CreateInternal(WebCrawlerQuery instance)
    {
        return new(this, instance);
    }
    public abstract QueryWrapper Create();
    public abstract Task<QueryWrapper> CreateAsync();
    protected abstract void Return(WebCrawlerQuery instance);
}

public class StandardWebCrawlerQueryFactory<T>
    : WebCrawlerQueryFactory, IDisposable where T : WebCrawlerQuery, new()
{
    private readonly CommandQueue _commandQueue = new();
    private readonly Queue<T> _objectQueue = new();

    public override async Task<QueryWrapper> CreateAsync()
    {
        T? ret = default;
        await _commandQueue.DispatchTask(() => { ret = _objectQueue.Count == 0 ? new T() : _objectQueue.Dequeue(); });
        return CreateInternal(ret!);
    }
    
    public override QueryWrapper Create()
    {
        var task = CreateAsync();
        task.Wait();
        return task.Result;
    }

    protected override void Return(WebCrawlerQuery instance)
    {
        _commandQueue.DispatchTask(() =>
        {
            _objectQueue.Enqueue((T)instance);
        });
    }

    public void Dispose()
    {
        _commandQueue.Dispose();
        if (typeof(T).IsAssignableTo(typeof(IDisposable)))
        {
            foreach (var obj in _objectQueue)
            {
                (obj as IDisposable)?.Dispose();
            }
        }
        _objectQueue.Clear();
    }
}
