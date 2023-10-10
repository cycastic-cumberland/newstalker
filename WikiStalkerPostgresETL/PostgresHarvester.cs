using ETLnR;
using ExtendedComponents;
using Newtonsoft.Json;
using Npgsql;
using PostgresDriver;
using WikiStalkerExtendedComponents;

namespace WikiStalkerPostgresETL;

public class PostgresHarvesterSettings
{
    public PostgresConnectionSettings ConnectionSettings { get; set; } = null!;
    public string WikipediaApiKey { get; set; } = "";
    public float RecordTimeToLiveHr { get; set; }
    public float HarvestIntervalHr { get; set; }
    public float GarbageCollectionIntervalHr { get; set; }
}

public class PostgresHarvester : AbstractHarvester
{
    private const long HourToMs = 1000 * 60 * 60;
    private readonly HttpClient _httpClient = new();
    private readonly LoggingServer _logger = new();
    private readonly PostgresQueryFactory _queryFactory;

    private readonly long _ttlMs;
    private readonly long _harvestIntervalMs;
    private readonly long _garbageCollectionIntervalMs;
    
    private long _lastHarvestIntervalEpoch;
    private long _lastGarbageCollectionEpoch;
    private long _lastHarvestedEpoch = 1000;

    private readonly string _header;
    private string ThreadedHeader => $"{_header}:{Environment.CurrentManagedThreadId}";
    
    private long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    
    private int GetHash() => GetHashCode();
    
    private async Task GetLastEpochFromDb()
    {
        using var queryWrapper = _queryFactory.NewQueryInstance();
        var db = queryWrapper.GetInstance().Db;

        try
        {
            var coll = await db.TryMappedQuery<MicroStamp>(
                "SELECT timestamp AS Timestamp FROM recent_changes ORDER BY timestamp DESC LIMIT 1;");
            var list = coll.ToList();
            if (list.Count == 0) return;
            _lastHarvestedEpoch = list[0].Timestamp;
        }
        catch (Exception)
        {
            // Ignored
        }
    }
    
    public PostgresHarvester(PostgresHarvesterSettings settings, LoggingServerDelegate[]? loggers = null)
        : base(settings.WikipediaApiKey)
    {
        _queryFactory = new PostgresQueryFactory(settings.ConnectionSettings);
        _ttlMs = (long)(settings.RecordTimeToLiveHr * HourToMs);
        _harvestIntervalMs = (long)(settings.HarvestIntervalHr * HourToMs);
        _garbageCollectionIntervalMs = (long)(settings.GarbageCollectionIntervalHr * HourToMs);
        _lastGarbageCollectionEpoch = Now; 
        _header = $"PostgresHarvester:{GetHash()}";
        var apiKey = ApiKey;
        if (apiKey.Length > 0)
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {ApiKey}");
        if (loggers != null)
        {
            foreach (var logger in loggers)
                _logger.EnrollDelegate(logger);
        }

        // Use this instead of the public one
        GetLastEpochFromDb().Wait();
        RunGarbageCollectionAsync().Wait();
        StartIterator();
        _logger.Write(_header, "PostgresHarvester online", LogSegment.LogSegmentType.Message);
    }

    public override void Dispose()
    {
        base.Dispose();
        _httpClient.Dispose();
        _queryFactory.Dispose();
        _logger.Dispose();
    }

    public override void CloseDaemon()
    {
        base.CloseDaemon();
        _httpClient.Dispose();
        _queryFactory.Dispose();
        _logger.Dispose();
    }
    
    private async Task RunGarbageCollectionAsync()
    {
        using var queryWrapper = _queryFactory.NewQueryInstance();
        var db = queryWrapper.GetInstance().Db;

        long threshold = Now - _ttlMs;
        try
        {
            var affected = await db.TryExecute("DELETE FROM recent_changes WHERE timestamp < @timestamp;",
                new { timestamp = threshold });
            if (affected > 0)
                _logger.Write(ThreadedHeader, $"Garbage collected, affected rows: {affected}", LogSegment.LogSegmentType.Message);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in RunGarbageCollectionAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
    }
    
    protected override void RunGarbageCollectionInternal()
    {
#pragma warning disable CS4014
        RunGarbageCollectionAsync(); //.Wait();
#pragma warning restore CS4014
        _lastGarbageCollectionEpoch = Now;
    }

    private async Task<int> SaveHarvestedData(string body)
    {
        try
        {
            int harvested = 0;
            var transformed = JsonConvert.DeserializeObject<RecentChangesQuery>(body);
            if (transformed == null || transformed.BatchComplete == null!)
            {
                _logger.Write(ThreadedHeader, "SaveHarvestedData: Failed to deserialize HTTP response",
                    LogSegment.LogSegmentType.Message, body);
                return harvested;
            }
            var changes = transformed.Query.Changes;
            using var queryWrapper = _queryFactory.NewQueryInstance();
            var db = queryWrapper.GetInstance().Db;
            using var transaction = db.CreateTransaction();
            try
            {
                foreach (var change in changes)
                {
                    var rectified = change.Rectify();
                    // This most likely mean the hashes collide
                    try
                    {
                        await db.TryExecute("INSERT INTO recent_changes (hash, type, ns, title, pageid, revid, old_revid, rcid, timestamp) " +
                                            "VALUES  (@hash, @type, @ns, @title, @pageid, @revid, @old_revid, @rcid, @timestamp);",
                            new
                            {
                                hash = rectified.Hash,
                                type = rectified.Type,
                                ns = rectified.Ns,
                                title = rectified.Title,
                                pageid = rectified.PageId,
                                revid = rectified.RevId,
                                old_revid = rectified.OldRevId,
                                rcid = rectified.RcId,
                                timestamp = rectified.Timestamp
                            }, transaction.GetRawTransaction());
                        harvested++;
                    }
                    catch (NpgsqlException)
                    {
                        // Ignored
                    }
                }
                transaction.Commit();
                return harvested;
            }
            catch (Exception)
            {
                transaction.RollBack();
                throw;
            }
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in SaveHarvestedData",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return 0;
        }
    }
    
    private async Task RunHarvestAsync()
    {
        const string urlTemplate = "https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&rcnamespace=0&format=json";
        try
        {
            var currentStamp = Now;
            var url = $"{urlTemplate}&rclimit=500&rcstart={currentStamp / 1000}&rcend={_lastHarvestedEpoch / 1000}";
            var response = await _httpClient.GetAsync(url);
            _lastHarvestedEpoch = currentStamp;
            var body = await response.Content.ReadAsStringAsync();
            if (!response.IsSuccessStatusCode)
            {
                _logger.Write(ThreadedHeader, $"RunHarvestAsync: HTTP request failed with status code: {response.StatusCode}",
                    LogSegment.LogSegmentType.Message, body);
                return;
            }

            var harvested = await SaveHarvestedData(body);
            var finished = Now;
            _logger.Write(ThreadedHeader, $"Data harvested in {finished - currentStamp} ms. Affected rows: {harvested}", LogSegment.LogSegmentType.Message);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in RunHarvestAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
    }
    
    protected override void RunHarvestInternal()
    {
        RunHarvestAsync().Wait();
        _lastHarvestIntervalEpoch = Now;
    }

    protected override void UpdateSettingsInternal() {  }

    protected override void CancelServerLoopInternal() { }

    protected override bool IterateInternal()
    {
        if (Now - _lastHarvestIntervalEpoch >= _harvestIntervalMs)
        {
            _lastHarvestIntervalEpoch = long.MaxValue;
            RunHarvest();
        }

        if (Now - _lastGarbageCollectionEpoch >= _garbageCollectionIntervalMs)
        {
            _lastGarbageCollectionEpoch = long.MaxValue;
            RunGarbageCollection();
        }
        
        return true;
    }
}