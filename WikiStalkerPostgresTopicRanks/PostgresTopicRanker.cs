using System.Text;
using ETLnR;
using ExtendedComponents;
using Newtonsoft.Json;
using PostgresDriver;
using WikiStalkerExtendedComponents;

namespace WikiStalkerPostgresTopicRanks;


public class PostgresTopicRankerSettings
{
    public PostgresConnectionSettings ConnectionSettings = new();
    public string PageRankApiUrl = "";
    public string PageRankApiKey = "";
    public string WikipediaApiKey = "";
    public uint WorkerCount;
    public uint CrawlMaxDepth;
    public float RankIntervalHr;
}

internal class Reference
{
    [JsonProperty("fromId")] public long From;
    [JsonProperty("toId")] public long To;

    public Reference((long, long) tuple)
    {
        From = tuple.Item1;
        To = tuple.Item2;
    }
}

internal class ReferenceData
{
    [JsonProperty("references")] public Reference[] References = null!;
}

public class PostgresTopicRanker : AbstractTopicRanker
{
    private const long HourToMs = 1000 * 60 * 60;
    
    private readonly HttpClient _httpClient = new();
    private readonly LoggingServer _logger = new();
    private readonly ReferenceCrawler _referenceCrawler;
    private readonly PostgresQueryFactory _queryFactory;
    private readonly string _pageRankApiUrl;
    private readonly long _rankIntervalMs;
    private readonly string _header;
    private string ThreadedHeader => $"{_header}:{Environment.CurrentManagedThreadId}";
    private long _lastRankInterval;
    private long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    private int GetHash() => GetHashCode();

    private async Task GetLastEpochFromDb()
    {
        using var queryWrapper = _queryFactory.NewQueryInstance();
        var db = queryWrapper.GetInstance().Db;

        try
        {
            var coll = await db.TryMappedQuery<MicroStamp>(
                "SELECT end_timestamp AS Timestamp FROM popularity_windows ORDER BY end_timestamp DESC LIMIT 1;");
            var list = coll.ToList();
            if (list.Count == 0) return;
            _lastRankInterval = list[0].Timestamp;
        }
        catch (Exception)
        {
            // Ignored
        }
    }
    
    public PostgresTopicRanker(PostgresTopicRankerSettings settings, LoggingServerDelegate[]? loggers = null)
    {
        _rankIntervalMs = (long)(settings.RankIntervalHr * HourToMs);
        _pageRankApiUrl = settings.PageRankApiUrl;
        _queryFactory = new PostgresQueryFactory(settings.ConnectionSettings);
        _header = $"PostgresTopicRanker:{GetHash()}";
        // _httpClient.DefaultRequestHeaders.Add("Content-Type", "application/json");
        if (settings.PageRankApiKey.Length > 0)
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {settings.PageRankApiKey}");
        if (loggers != null)
        {
            foreach (var logger in loggers)
                _logger.EnrollDelegate(logger);
        }
        _referenceCrawler = new(settings.WorkerCount, settings.CrawlMaxDepth, settings.WikipediaApiKey, _logger);

        GetLastEpochFromDb().Wait();
        StartIterator();
        _logger.Write(_header, "PostgresTopicRanker online", LogSegment.LogSegmentType.Message);
    }

    public override void Dispose()
    {
        base.Dispose();
        _referenceCrawler.Dispose();
        _httpClient.Dispose();
        _queryFactory.Dispose();
        _logger.Dispose();
    }

    public override void CloseDaemon()
    {
        base.CloseDaemon();
        _referenceCrawler.Dispose();
        _httpClient.Dispose();
        _queryFactory.Dispose();
        _logger.Dispose();
    }

    private async Task<IEnumerable<WikipediaRecentChange>> AggregateData(TopicRanksSettings settings)
    {
        var commenced = Now;
        _logger.Write(ThreadedHeader, "Aggregating data", LogSegment.LogSegmentType.Message);
        
        using var queryWrapper = _queryFactory.NewQueryInstance();
        var db = queryWrapper.GetInstance().Db;
        var (begin, end) = settings.TimeWindow;
        try
        {
            var ret = await db.TryMappedQuery<WikipediaRecentChange>(
                "SELECT hash as Hash, " +
                "type as Type, " +
                "ns as Ns, " +
                "title as Title, " +
                "pageid as PageId, " +
                "revid as RevId, " +
                "old_revid as OldRevId, " +
                "rcid as RcId, " +
                "timestamp as Timestamp " +
                "FROM recent_changes WHERE timestamp >= @timeBegin AND timestamp < @timeEnd",
                new { timeBegin = begin, timeEnd = end } );
            _logger.Write(ThreadedHeader, $"Aggreagation concluded after {Now - commenced} ms", LogSegment.LogSegmentType.Message);
            return ret;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in AggregateData",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return new List<WikipediaRecentChange>();
        }
    }

    private async Task<ReferencesMap> ReferencesCrawl(IEnumerable<WikipediaRecentChange> records)
    {
        var commenced = Now;
        _logger.Write(ThreadedHeader, "Crawling for references", LogSegment.LogSegmentType.Message);
        var ret = await _referenceCrawler.Crawl(records);
        _logger.Write(ThreadedHeader, $"Crawling concluded after {Now - commenced} ms", LogSegment.LogSegmentType.Message);
        return ret;
    }

    private async Task<JsonTopicsMap> DelegatePopularityRanking(IEnumerable<Reference> references)
    {
        try
        {
            var commenced = Now;
            _logger.Write(ThreadedHeader, "Delegating popularity ranking", LogSegment.LogSegmentType.Message);
            var payload = new ReferenceData
            {
                References = references.ToArray()
            };
            var stringPayload = JsonConvert.SerializeObject(payload);
            var httpContent = new StringContent(stringPayload, Encoding.UTF8, "application/json");
            var response = await _httpClient.PostAsync(_pageRankApiUrl, httpContent);
            var body = await response.Content.ReadAsStringAsync();
            if (!response.IsSuccessStatusCode)
            {
                throw new HttpRequestException($"DelegatePopularityRanking: HTTP request failed with status code: {response.StatusCode}");
            }

            var marshalled = JsonConvert.DeserializeObject<JsonTopicsMap>(body);
            if (marshalled == null || marshalled.Map == null!)
            {
                _logger.Write(ThreadedHeader, "DelegatePopularityRanking: Failed to convert response body into C# object",
                    LogSegment.LogSegmentType.Message, body);
                throw new JsonException();
            }

            _logger.Write(ThreadedHeader, $"Delegation concluded after {Now - commenced} ms", LogSegment.LogSegmentType.Message);
            return marshalled;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in DelegatePopularityRanking",
                LogSegment.LogSegmentType.Exception, e.ToString());
            throw;
        }
    }
    
    private async Task<TopicsMap> RankTopicsAsync(TopicRanksSettings settings)
    {
        var records = await AggregateData(settings);
        var references = await ReferencesCrawl(records);
        var asReferences = from r in references select new Reference(r);
        var rawMap = await DelegatePopularityRanking(asReferences);

        return rawMap.ToStruct();
    }
    
    protected override TopicsMap RankTopicsInternal(TopicRanksSettings settings)
    {
        var task = RankTopicsAsync(settings);
        task.Wait();
        if (task.Exception != null) throw task.Exception;
        return task.Result;
    }

    private async Task SavePopularity(TopicRanksSettings settings, TopicsMap map)
    {
        var hashed = Crypto.HashSha256String(settings.ToString());
        var (beginStamp, endStamp) = settings.TimeWindow;
        var timespan = beginStamp - endStamp;
        using var queryWrapper = _queryFactory.NewQueryInstance();
        var db = queryWrapper.GetInstance().Db;
        using var transaction = db.CreateTransaction();
        try
        {
            transaction.Start();
            await db.TryExecute("INSERT INTO popularity_windows (hash, begin_timestamp, end_timestamp, span) VALUES " +
                                "(@hash, @begin, @end, @span);",
                new { hash = hashed, begin = beginStamp, end = endStamp, span = timespan },
                transaction.GetRawTransaction());

            foreach (var (keyword, popularity) in map.Map)
            {
                await db.TryExecute("INSERT INTO topic_popularity (window_id, keyword, popularity) VALUES " +
                                    "(@windowId, @kw, @percent);",
                    new { windowId = hashed, kw = keyword, percent = popularity },
                    transaction.GetRawTransaction());
            }
            transaction.Commit();
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in SavePopularity",
                LogSegment.LogSegmentType.Exception, e.ToString());
            transaction.RollBack();
        }
    }
    
    private async Task InternalRankJob()
    {
        var current = Now;
        TopicRanksSettings settings = new()
        {
            TimeWindow = (current - _rankIntervalMs, current)
        };
        var ret = await RankTopicsAsync(settings);
        await SavePopularity(settings, ret);
    }
    
    private void RankJob()
    {
        InternalRankJob().Wait();
        _lastRankInterval = Now;
    }
    
    protected override bool IterateInternal()
    {
        if (Now - _lastRankInterval >= _rankIntervalMs)
        {
            _lastRankInterval = long.MaxValue;
            Dispatch(RankJob);
        }
        return true;
    }
}