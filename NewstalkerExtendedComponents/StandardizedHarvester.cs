using ExtendedComponents;

namespace NewstalkerExtendedComponents;

public struct StandardizedHarvesterSettings
{
    public string[] Outlets { get; set; }
}

public class StandardizedHarvester : IDisposable
{
    private readonly LoggingServer _logger;
    private readonly IReadOnlyDictionary<string, AbstractNewsOutlet> _outletSource;
    private readonly IDisposable _outletPlug;
    
    private int GetHash() => GetHashCode();
    private readonly string _header;
    private string ThreadedHeader => $"{_header}:{Environment.CurrentManagedThreadId}";
    
    public StandardizedHarvester(StandardizedHarvesterSettings settings, OutletSource outletSource, LoggingServer logger)
    {
        _header = $"StandardizedHarvester:{GetHash()}";
        _outletSource = outletSource;
        _outletPlug = outletSource;
        _logger = logger;
        _logger.Write(_header, "StandardizedHarvester online", LogSegment.LogSegmentType.Message);
    }

    public void Dispose()
    {
        _outletPlug.Dispose();
    }

    public async Task<IEnumerable<string>> QueryFrontPageAsync(string outletName, AbstractNewsOutlet.FrontPageQueryOptions options)
    {
        var outlet = _outletSource[outletName];
        try
        {
            var ret = await outlet.QueryFrontPageAsync(options);
            return ret;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in QueryFrontPageAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return ArraySegment<string>.Empty;
        }
    }
    
    
    public async Task<IEnumerable<string>> QueryFrontPageAsync(AbstractNewsOutlet outlet, AbstractNewsOutlet.FrontPageQueryOptions options)
    {
        try
        {
            var ret = await outlet.QueryFrontPageAsync(options);
            return ret;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in QueryFrontPageAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return ArraySegment<string>.Empty;
        }
    }

    public async Task<AbstractNewsOutlet.ArticleScrapeResult?> QueryArticleAsync(string outletName, string url)
    {
        var outlet = _outletSource[outletName];
        try
        {
            var ret = await outlet.QueryArticleAsync(url);
            return ret;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in QueryArticleAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return null;
        }
    }

    public async Task<IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>> AggregateArticles(string outletName, IEnumerable<string> urls)
    {
        var outlet = _outletSource[outletName];
        var results = await Task.WhenAll(from url in urls select Task.Run(async () =>
        {
            try
            {
                return await outlet.QueryArticleAsync(url);
            }
            catch (Exception e)
            {
                _logger.Write(ThreadedHeader, "Exception thrown in AggregateArticles",
                    LogSegment.LogSegmentType.Exception, e.ToString());
                return null;
            }
        }));
        return from result in results where result != null select result;
    }

    private static Task<T> DispatchTask<T>(Func<Task<T>> func, ref int count)
    {
        count++;
        return func();
    }
    
    public async Task<IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>> AggregateArticles(AbstractNewsOutlet outlet, IEnumerable<string> urls, int limit)
    {
        var i = 0;
        var results = await Task.WhenAll(from url in urls where i <= limit select DispatchTask(async () =>
        {
            try
            {
                return await outlet.QueryArticleAsync(url);
            }
            catch (Exception e)
            {
                _logger.Write(ThreadedHeader, "Exception thrown in AggregateArticles",
                    LogSegment.LogSegmentType.Exception, e.ToString());
                return null;
            }
        }, ref i));
        return from result in results where result != null select result;
    }

    public async Task<IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>> AggregateFrontpage(string outletName,
        AbstractNewsOutlet.FrontPageQueryOptions options)
    {
        try
        {
            return await AggregateArticles(outletName, await QueryFrontPageAsync(outletName, options));
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in AggregateFrontpage",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return ArraySegment<AbstractNewsOutlet.ArticleScrapeResult>.Empty;
        }
    }
    
    public async Task<IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>> AggregateFrontpage(AbstractNewsOutlet outlet,
        AbstractNewsOutlet.FrontPageQueryOptions options)
    {
        try
        {
            return await AggregateArticles(outlet, await QueryFrontPageAsync(outlet, options), options.Limit);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in AggregateFrontpage",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return ArraySegment<AbstractNewsOutlet.ArticleScrapeResult>.Empty;
        }
    }
}