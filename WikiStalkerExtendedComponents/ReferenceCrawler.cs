using System.Collections;
using ExtendedComponents;
using Newtonsoft.Json;
using ThreadPool = ExtendedComponents.ThreadPool;

namespace WikiStalkerExtendedComponents;

// public readonly struct MonoDirectionalReference<T>
// {
//     public readonly T Left;
//     public readonly T Right;
//     
//     public MonoDirectionalReference(T left, T right)
//     {
//         Left = left;
//         Right = right;
//     }
//
//     public override int GetHashCode()
//     {
//         int hash = 17;
//         if (Left != null) hash = hash * 31 + Left.GetHashCode();
//         if (Right != null) hash = hash * 31 + Right.GetHashCode();
//         return hash;
//     }
//
//     public override bool Equals(object? obj)
//     {
//         var casted = obj is MonoDirectionalReference<T> reference ? reference : default;
//         return EqualityComparer<T>.Default.Equals(Left, casted.Left) &&
//                EqualityComparer<T>.Default.Equals(Right, casted.Right);
//     }
//
//     public override string ToString()
//     {
//         return $"({Left?.ToString()}, {Right?.ToString()})";
//     }
//
//     public void Deconstruct(out T left, out T right)
//     {
//         left = Left;
//         right = Right;
//     }
// }

public class ReferencesMap : IEnumerable<(long left, long right)>
{
    private readonly HashSet<(long left, long right)> _set = new();
    private readonly HashSet<long> _visited = new();
    
    public void TryReference(long fromId, long toId, Action newTunnelOp)
    {
        lock (this)
        {
            _set.Add((fromId, toId));
            var result = _visited.Contains(toId);
            if (!result)
            {
                _visited.Add(toId);
                newTunnelOp();
            }
        }
    }

    public void ManualAddExisted(long id)
    {
        lock (this)
        {
            _visited.Add(id);
        }
    }

    public IEnumerator<(long left, long right)> GetEnumerator() => _set.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _set.GetEnumerator();
}

public class ReferenceCrawler : IDisposable
{
    private readonly HttpClient _httpClient = new();
    private readonly ThreadPool _threadPool;
    private readonly LoggingServer _logger;
    private readonly uint _maxDepth;
    private Task<ReferencesMap>? _activeTask;
    private int _cancelled;

    private readonly string _header;
    private string ThreadedHeader => $"{_header}:{Environment.CurrentManagedThreadId}";
    private int GetHash() => GetHashCode();
    
    public ReferenceCrawler(uint threadCount, uint maxDepth, string apiKey, LoggingServer loggingServer)
    {
        _maxDepth = maxDepth;
        _header = $"ReferenceCrawler:{GetHash()}";
        // Avoid contingency with dummy task
        _threadPool = new(threadCount < 2 ? 2 : threadCount);
        _logger = loggingServer;
        if (apiKey.Length > 0)
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {apiKey}");
    }
    
    public void Dispose()
    {
        Interlocked.Increment(ref _cancelled);
        // We all know it's cancelled at this point
        try
        {
            _activeTask?.Wait();
        }
        catch (Exception)
        {
            // Ignored
        }
        _threadPool.Dispose();
        _httpClient.Dispose();
    }

    private void DummyThread()
    {
        // Prevent the group task from ending immediately
        Thread.Sleep(2000);
    }

    private async Task<ReferencesMap> GroupTaskWait(ReferencesMap map)
    {
        while (true)
        {
            if (_cancelled != 0) throw new TaskCanceledException();
            if (_threadPool.ActiveThreadCount == 0)
            {
                _activeTask = null;
                return map;
            }
            await Task.Delay(1000);
        }
    }

    // https://en.wikipedia.org/w/api.php?action=query&titles=Draught%20beer&format=json
    private bool AppendReference(long fromId, string title, ReferencesMap map, uint depth)
    {
        const string urlTemplate = "https://en.wikipedia.org/w/api.php?action=query";
        try
        {
            var url = $"{urlTemplate}&titles={Uri.EscapeDataString(title)}&format=json";
            var response = Get(url);
            var body = ReadAsString(response);
            if (!response.IsSuccessStatusCode)
            {
                _logger.Write(ThreadedHeader, $"CrawlInternalAsync: HTTP request failed with status code: {response.StatusCode}",
                    LogSegment.LogSegmentType.Message, body);
                return false;
            }
            var converted = JsonConvert.DeserializeObject<PagesQuery>(body);
            if (converted == null || converted.Query == null!)
            {
                _logger.Write(ThreadedHeader, "AppendReference: Failed to deserialize HTTP response",
                    LogSegment.LogSegmentType.Message, body);
                return false;
            }

            // There should only be one record
            foreach (var (_, page) in converted.Query.AllPages)
            {
                map.TryReference(fromId, page.PageId, () =>
                {
                    _threadPool.EnqueueTask(() => CrawlByIdAsync(page.PageId, map, depth));
                });
                return true;
            }
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in AppendReference",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
        return false;
    }
    
    private int AppendReferences(long fromId, string body, ReferencesMap map, uint depth)
    {
        if (depth >= _maxDepth) return 0;
        int appended = 0;
        try
        {
            var converted = JsonConvert.DeserializeObject<ArticleReferences>(body);
            if (converted == null || converted.Parse == null! || converted.Parse.Links == null!)
            {
                _logger.Write(ThreadedHeader, "AppendReferences: Failed to deserialize HTTP response",
                    LogSegment.LogSegmentType.Message, body);
                return appended;
            }

            var references = converted.Parse.Links;
            // Select all articles (aka. namespace 0)
            foreach (var reference in from r in references where r.Ns == 0 select r)
            {
                var ret = AppendReference(fromId, reference.Name, map, depth + 1);
                if (ret) appended++;
            }
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in AppendReferences",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
        
        return appended;
    }

    private HttpResponseMessage Get(string url)
    {
        var task = _httpClient.GetAsync(url);
        task.Wait();
        if (task.Exception != null) throw task.Exception;
        return task.Result;
    }

    private static string ReadAsString(HttpResponseMessage msg)
    {
        var task = msg.Content.ReadAsStringAsync();
        task.Wait();
        if (task.Exception != null) throw task.Exception;
        return task.Result;
    }
    
    // References (by pageid): https://en.wikipedia.org/w/api.php?action=parse&pageid=74884249&prop=links&format=json
    private void CrawlByIdAsync(long pageId, ReferencesMap map, uint depth = 0)
    {
        const string urlTemplate = "https://en.wikipedia.org/w/api.php?action=parse";
        try
        {
            var url = $"{urlTemplate}&pageid={pageId}&prop=links&format=json";
            var response = Get(url);
            var body = ReadAsString(response);
            if (!response.IsSuccessStatusCode)
            {
                _logger.Write(ThreadedHeader, $"CrawlInternalAsync: HTTP request failed with status code: {response.StatusCode}",
                    LogSegment.LogSegmentType.Message, body);
                return;
            }

            AppendReferences(pageId, body, map, depth);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in CrawlInternalAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
    }
    private void PooledCrawl(IEnumerable<WikipediaRecentChange> records, ReferencesMap map)
    {
        foreach (var record in records)
        {
            map.ManualAddExisted(record.PageId);
            _threadPool.EnqueueTask(() => CrawlByIdAsync(record.PageId, map));
        }
    }

    public Task<ReferencesMap> Crawl(IEnumerable<WikipediaRecentChange> records)
    {
        lock (this)
        {
            if (_activeTask != null) throw new NotSupportedException("Running multiple crawl session at the sametime is not supported");
            var map = new ReferencesMap();
            _cancelled = 0;
            _threadPool.EnqueueTask(DummyThread);
            _threadPool.EnqueueTask(() => PooledCrawl(records, map));
            _activeTask = GroupTaskWait(map);
            return _activeTask;
        }
    }
}