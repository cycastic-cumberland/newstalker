using ExtendedComponents;
using NewstalkerExtendedComponents;
using Npgsql;
using PostgresDriver;

namespace NewstalkerPostgresGrader;

public struct NewstalkerPostgresConductorSettings
{
    public StandardizedHarvesterSettings HarvesterSettings;
    public PostgresConnectionSettings ConnectionSettings;
    public DelegatedSummarizerSettings SummarizerSettings;
    public PostgresGraderSettings GraderSettings;
    public AbstractNewsOutlet.FrontPageQueryOptions DefaultQueryOption;
    public TimeSpan HarvestInterval;
    public TimeSpan GarbageCollectionInterval;
    public OutletSource Outlets;
    public uint PostgresConnectionsLimit;
    public uint DelegationConnectionsLimit;
}

public class NewstalkerPostgresConductor : AbstractDaemon
{
    private struct IntegerStruct
    {
        public int IntegerValue;
    }
    private struct CountStruct
    {
        public long Count;
    }
    private struct DateTimeStruct
    {
        public DateTime Timestamp;
    }
    
    public const double StandardTagsWeight = 0.07132897;

    private readonly NewstalkerPostgresConductorSettings _settings;
    private readonly LoggingServer _logger = new();
    private readonly ObjectPool<PostgresProvider> _queryFactory;
    private readonly ObjectPool<int> _delegationChoker;
    private readonly StandardizedHarvester _harvester;
    private readonly DelegatedSummarizer _summarizer;
    private readonly PostgresGrader _grader;
    private readonly AutoLoggerFactory _loggerFactory;

    private int _currentSessionId;
    private DateTime _lastHarvestTime = new(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
    private DateTime _lastGarbageCollectionTime = DateTime.Now;

    private int AnswerToLifeUniverseAndEveryThing() => 42;
    private int GetHash() => GetHashCode();
    private readonly string _header;
    private string ThreadedHeader => $"{_header}:{Environment.CurrentManagedThreadId}";

    private async Task GetLastEpochFromDb()
    {
        using var queryWrapper = _queryFactory.Borrow();
        var db = queryWrapper.GetInstance();

        try
        {
            var coll = await db.TryMappedQuery<DateTimeStruct>(
                "SELECT time_end AS Timestamp FROM scrape_sessions WHERE is_finished = true ORDER BY time_end DESC LIMIT 1;");
            var list = coll.ToList();
            if (list.Count == 0) return;
            _lastHarvestTime = list[0].Timestamp;
        }
        catch (Exception)
        {
            // Ignored
        }
    }
    
    public NewstalkerPostgresConductor(NewstalkerPostgresConductorSettings settings, LoggingServerDelegate[]? loggers = null)
    {
        _settings = settings;
        _header = $"PostgresHarvestScheduler:{GetHash()}";
        _queryFactory = new FiniteObjectPool<PostgresProvider>(() => new(_settings.ConnectionSettings),
            settings.PostgresConnectionsLimit == 0 ? 64 : settings.PostgresConnectionsLimit);
        _delegationChoker = new FiniteObjectPool<int>(AnswerToLifeUniverseAndEveryThing,
            settings.DelegationConnectionsLimit == 0 ? 16 : settings.DelegationConnectionsLimit);
        if (loggers != null)
        {
            foreach (var logger in loggers)
                _logger.EnrollDelegate(logger);
        }

        var graderSettings = settings.GraderSettings;
        if (graderSettings.TagsWeight <= 0.0 ||
            graderSettings.TagsWeight >= 1.0) graderSettings.TagsWeight = StandardTagsWeight;

        GetLastEpochFromDb().Wait();
        _loggerFactory = new("PostgresHarvestScheduler", _logger);
        _harvester = new(_settings.HarvesterSettings, _settings.Outlets, _logger);
        _summarizer = new(_settings.SummarizerSettings, _logger);
        _grader = new(_queryFactory, graderSettings, _logger);
        RunGarbageCollection();
        StartIterator();
        _logger.Write(_header, "PostgresHarvestScheduler online", LogSegment.LogSegmentType.Message);
    }
    public override void Dispose()
    {
        base.Dispose();
        _queryFactory.Dispose();
        _grader.Dispose();
        _summarizer.Dispose();
        _harvester.Dispose();
        _logger.Dispose();
        GC.SuppressFinalize(this);
    }

    public override void CloseDaemon()
    {
        base.CloseDaemon();
        _queryFactory.Dispose();
        _harvester.Dispose();
        _logger.Dispose();
    }

    private async Task InsertArticle(AbstractNewsOutlet.ArticleScrapeResult result)
    {
        try
        {
            using var wrapped = _queryFactory.Borrow();
            var db = wrapped.GetInstance();
            await db.OpenTransaction(async t =>
            {
                var transaction = t.GetRawTransaction();
                await db.TryExecute("INSERT INTO scrape_results " +
                                    "(url, outlet_url, language, title, author, time_posted, original_text, word_count) VALUES " +
                                    "(@id, @outlet, @lang, @title, @author, @uptime, @text, @word);",
                    new
                    {
                        id = result.Url, outlet = result.OutletUrl, lang = result.Lang, title = result.Title,
                        author = result.Author, uptime = result.TimePosted, text = result.Text, word = result.WordCount
                    }, transaction);
                foreach (var tag in result.Tags)
                {
                    await db.TryExecute("INSERT INTO article_tags (tag) VALUES (@articleTag) ON CONFLICT DO NOTHING;",
                        new { articleTag = tag }, transaction);
                    await db.TryExecute("INSERT INTO tags_used (article_url, tag) VALUES " +
                                        "(@articleUrl, @articleTag);",
                        new { articleUrl = result.Url, articleTag = tag }, transaction);
                }
            });
        }
        catch (PostgresException e)
        {
            if (e.SqlState != "23505") _logger.Write(ThreadedHeader, "Exception thrown in InsertArticle",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in InsertArticle",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
    }

    public async Task<string> SummarizeArticle(AbstractNewsOutlet.ArticleScrapeResult article)
        =>  await _summarizer.SummarizeArticleAsync(article);
    
    private async Task SummarizeAndSaveArticle(AbstractNewsOutlet.ArticleScrapeResult article)
    {
        using var choker = _delegationChoker.Borrow();
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        try
        {
            var summarizedText = await SummarizeArticle(article);
            await db.TryExecute("INSERT INTO summarization_results (article_url, summarized_text) VALUES " +
                                "(@url, @summarized) ON CONFLICT DO NOTHING;",
                new { url = article.Url, summarized = summarizedText });
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in SummarizeArticle",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
    }

    public async Task<Dictionary<string, double>> ExtractTopics(AbstractNewsOutlet.ArticleScrapeResult article)
        => await _summarizer.ExtractTopicsAsync(article);
    
    private async Task ExtractAndSaveTopics(AbstractNewsOutlet.ArticleScrapeResult article)
    {
        using var choker = _delegationChoker.Borrow();
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        // var epoch = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        try
        {
            var topics = await ExtractTopics(article);
            foreach (var (topic, relevancy) in topics)
            {
                await db.OpenTransaction(async t =>
                {
                    var transaction = t.GetRawTransaction();
                    try
                    {
                        await db.TryExecute("INSERT INTO unique_keywords (keyword) " +
                                            "VALUES (@kw) ON CONFLICT DO NOTHING;",
                            new { kw = topic }, transaction);
                        await db.TryExecute(
                            "INSERT INTO extracted_keywords (article_url, keyword, relevancy) " +
                            "VALUES (@url, @kw, @rel) ON CONFLICT DO NOTHING",
                            new { url = article.Url, kw = topic, rel = relevancy }, transaction);
                    }
                    catch (Exception e)
                    {
                        _logger.Write(ThreadedHeader, "Exception thrown in ExtractTopics/@OpenTransaction",
                            LogSegment.LogSegmentType.Exception, e.ToString());
                        t.RollBack();
                    }
                });
            }
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in ExtractTopics",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
    }

    public async Task<AbstractNewsOutlet.ArticleScrapeResult?> QueryArticle(string url)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        return (await db.TryMappedQuery<AbstractNewsOutlet.ArticleScrapeResult>(
            "SELECT url AS Url," +
            "outlet_url As OutletUrl, " +
            "language AS Lang, " +
            "title AS Title, " +
            "author As Author, " +
            "time_posted As TimePosted, " +
            "original_text As Text, " +
            "word_count As WordCount FROM scrape_results " +
            "WHERE url = @url;", new { url })).FirstOrDefault();
    }
    
    public async Task<IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>> QueryAllArticles()
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        return await db.TryMappedQuery<AbstractNewsOutlet.ArticleScrapeResult>(
            "SELECT url AS Url," +
            "outlet_url As OutletUrl, " +
            "language AS Lang, " +
            "title AS Title, " +
            "author As Author, " +
            "time_posted As TimePosted, " +
            "original_text As Text, " +
            "word_count As WordCount FROM scrape_results;");
    }

    private static T Passthrough<T>(Func<T> func) => func();

    private async Task TimedOffload(Func<Task> task, string funcName, int taskNo, int totalTask, string content)
    {
        _logger.Write(ThreadedHeader, $"[{taskNo}/{totalTask}] {funcName} starting",
            LogSegment.LogSegmentType.Message, content);
        var epoch = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        await task();
        _logger.Write(ThreadedHeader, $"[{taskNo}/{totalTask}] {funcName} finished " +
                                      $"in {DateTimeOffset.Now.ToUnixTimeMilliseconds() - epoch} ms",
            LogSegment.LogSegmentType.Message, content);
    }
    
    private async Task RunHarvestAsync()
    {
        try
        {
            using var unused = _loggerFactory.Create("RunHarvestAsync");
            var aggregationLogger = _loggerFactory.Create("RunHarvestAsync/@AggregateFrontpage");
            var results = await Task.WhenAll(from pair in _settings.Outlets
                select Task.Run(async () => await _harvester.AggregateFrontpage(pair.Value, _settings.DefaultQueryOption)));
            aggregationLogger.Dispose();

            using var wrapped = _queryFactory.Borrow();
            var db = wrapped.GetInstance();
            var curr = await db.TryMappedQuery<IntegerStruct>(
                "INSERT INTO scrape_sessions (time_initialized, time_end, is_finished) " +
                "VALUES (@init, @end, false) RETURNING id AS IntegerValue;",
                new { init = DateTime.Now, end = _lastHarvestTime });
            _currentSessionId = curr.FirstOrDefault().IntegerValue;

            var articles = results.SelectMany(o => o).ToArray();
            {
                using var unused1 = _loggerFactory.Create("RunHarvestAsync/@InsertArticle");
                var insertCount = 0;
                var syncTasks = from article in articles
                    select Passthrough(() =>
                    {
                        var currInsert = ++insertCount;
                        return TimedOffload(() => InsertArticle(article), "InsertArticle",
                            currInsert, articles.Length, article.Url);
                    });
                await Task.WhenAll(syncTasks);
            }
            {
                using var unused1 = _loggerFactory.Create("RunHarvestAsync/@ExtractAndSaveTopics");
                using var unused2 = _loggerFactory.Create("RunHarvestAsync/@SummarizeAndSaveArticle");
                var extractCount = 0;
                var summarizeCount = 0;
                var analyzeTasks = (from article in articles
                    select Passthrough(() =>
                    {
                        var currExtract = ++extractCount;
                        var currSummarize = ++summarizeCount;
                        return new[]
                        {
                            TimedOffload(() => ExtractAndSaveTopics(article), "ExtractAndSaveTopics",
                                currExtract, articles.Length, article.Url),
                            TimedOffload(() => SummarizeAndSaveArticle(article), "SummarizeAndSaveArticle",
                                currSummarize, articles.Length, article.Url)
                        };
                    })).SelectMany(o => o);
                await Task.WhenAll(analyzeTasks);
            }
            
            var commitTime = DateTime.Now;
            lock (this)
            {
                _lastGarbageCollectionTime = commitTime;
            }
            var affected = await db.TryExecute(
                "UPDATE scrape_sessions SET time_end = @end, is_finished = true " +
                "WHERE id = @id;", new { end = commitTime, id = _currentSessionId });
            if (affected == 0) _logger.Write(ThreadedHeader, $"Could not commit session with ID: {_currentSessionId}",
                LogSegment.LogSegmentType.Exception);
            _currentSessionId = 0;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception during commit in RunHarvestAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
    }

    private async Task RunGarbageCollectionAsync()
    {
        using var unused = _loggerFactory.Create("RunGarbageCollectionAsync");

        try
        {
            var threshold = DateTime.Now - _settings.GarbageCollectionInterval;
            using var wrapped = _queryFactory.Borrow();
            var db = wrapped.GetInstance();

            var affected = await db.TryExecute("DELETE FROM tags_used WHERE article_url IN " +
                                               "(SELECT url FROM scrape_results WHERE " +
                                               "time_posted < @time); " +
                                               "DELETE FROM scrape_results WHERE time_posted < @time",
                new { time = threshold });
            if (affected > 0)
                _logger.Write(ThreadedHeader, $"Garbage collected, affected rows: {affected}", LogSegment.LogSegmentType.Message);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in RunGarbageCollectionAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
        lock (this)
        {
            _lastGarbageCollectionTime = DateTime.Now;
        }
    }

    private void RunHarvest()
    {
#pragma warning disable CS4014
        RunHarvestAsync();
#pragma warning restore CS4014
    }

    private void RunGarbageCollection()
    {
#pragma warning disable CS4014
        RunGarbageCollectionAsync();
#pragma warning restore CS4014
    }
    
    protected override bool IterateInternal()
    {
        lock (this)
        {
            if (DateTime.Now - _lastHarvestTime >= _settings.HarvestInterval)
            {
                _lastHarvestTime = DateTime.Now.AddDays(1.0f);
                RunHarvest();
            }
            if (DateTime.Now - _lastGarbageCollectionTime >= _settings.HarvestInterval)
            {
                _lastGarbageCollectionTime = DateTime.MaxValue;;
                RunGarbageCollection();
            }
        }

        return true;
    }
}