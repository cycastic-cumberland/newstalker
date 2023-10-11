using System.Text;
using System.Text.RegularExpressions;
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
    public uint ExtractorConnectionsLimit;
    public uint SummarizerConnectionsLimit;
    public bool UseDualSyncMode;
}

public struct ScrapeSession
{
    public DateTime StartTime { get; set; }
    public DateTime ConclusionTime { get; set; }
    public bool IsFinished { get; set; }
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
    private readonly ObjectPool<int> _extractorChoker;
    private readonly ObjectPool<int> _summarizerChoker;
    private readonly StandardizedHarvester _harvester;
    private readonly DelegatedSummarizer _summarizer;
    private readonly PostgresGrader _grader;
    private readonly AutoLoggerFactory _loggerFactory;
    private readonly string[] _allOutlets;

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
        _extractorChoker = new FiniteObjectPool<int>(AnswerToLifeUniverseAndEveryThing,
            settings.ExtractorConnectionsLimit == 0 ? 16 : settings.ExtractorConnectionsLimit);
        var summarizerConnectionLimit = settings.SummarizerConnectionsLimit;
        var summarizerMode = summarizerConnectionLimit == 0 ? " (Shared)" : "";
        _summarizerChoker = summarizerConnectionLimit == 0 ? _extractorChoker : new FiniteObjectPool<int>(AnswerToLifeUniverseAndEveryThing,
            summarizerConnectionLimit);
        if (loggers != null)
        {
            foreach (var logger in loggers)
                _logger.EnrollDelegate(logger);
        }

        _allOutlets = (from pair in settings.Outlets select pair.Value.GetBaseUrl()).ToArray();
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
        _logger.Write(_header, $"Extractor limit: {((FiniteObjectPool<int>)_extractorChoker).Capacity}",
            LogSegment.LogSegmentType.Message);
        _logger.Write(_header, $"Summarizer limit: {((FiniteObjectPool<int>)_summarizerChoker).Capacity}" +
                               $"{summarizerMode}",
            LogSegment.LogSegmentType.Message);
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

    private async Task<bool> InsertArticle(AbstractNewsOutlet.ArticleScrapeResult result)
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
            return true;
        }
        catch (PostgresException e)
        {
            if (e.SqlState != "23505")
                _logger.Write(ThreadedHeader, "Exception thrown in InsertArticle",
                    LogSegment.LogSegmentType.Exception, e.ToString());
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in InsertArticle",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }
        return false;
    }

    public async Task<string> SummarizeArticle(AbstractNewsOutlet.ArticleScrapeResult article)
        =>  await _summarizer.SummarizeArticleAsync(article);
    
    private async Task<bool> SummarizeAndSaveArticle(AbstractNewsOutlet.ArticleScrapeResult article)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        try
        {
            var summarizedText = await SummarizeArticle(article);
            await db.TryExecute("INSERT INTO summarization_results (article_url, summarized_text) VALUES " +
                                "(@url, @summarized) ON CONFLICT DO NOTHING;",
                new { url = article.Url, summarized = summarizedText });
            return true;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in SummarizeArticle",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }

        return false;
    }

    public async Task<Dictionary<string, double>> ExtractTopics(AbstractNewsOutlet.ArticleScrapeResult article)
        => await _summarizer.ExtractTopicsAsync(article);
    
    private async Task<bool> ExtractAndSaveTopics(AbstractNewsOutlet.ArticleScrapeResult article)
    {
        var charsToRemove = new char[] { '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', '=', '+', '~',
            '`', '{', '[', '}', ']', '\\', '|', ';', ':', '\"', '\'', '<', ',', '>', '.', '?', '/' };
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        // var epoch = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        var ret = true;
        try
        {
            var topics = await ExtractTopics(article);
            foreach (var (topic, relevancy) in topics)
            {
                var cleansedKw = new string((from c in topic where !charsToRemove.Contains(c) select c).ToArray()).Trim();
                if (cleansedKw.Length == 0)
                {
                    _logger.Write(ThreadedHeader, "Topic contains all special characters and will be skipped",
                        LogSegment.LogSegmentType.Message);
                    continue;
                }
                await db.OpenTransaction(async t =>
                {
                    var transaction = t.GetRawTransaction();
                    try
                    {
                        await db.TryExecute("INSERT INTO unique_keywords (keyword) " +
                                            "VALUES (@kw) ON CONFLICT DO NOTHING;",
                            new { kw = cleansedKw }, transaction);
                        await db.TryExecute(
                            "INSERT INTO extracted_keywords (article_url, keyword, relevancy) " +
                            "VALUES (@url, @kw, @rel) ON CONFLICT DO NOTHING",
                            new { url = article.Url, kw = cleansedKw, rel = relevancy }, transaction);
                    }
                    catch (Exception e)
                    {
                        _logger.Write(ThreadedHeader, "Exception thrown in ExtractTopics/@OpenTransaction",
                            LogSegment.LogSegmentType.Exception, e.ToString());
                        t.RollBack();
                        ret = false;
                    }
                });
            }
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "Exception thrown in ExtractTopics",
                LogSegment.LogSegmentType.Exception, e.ToString());
        }

        return ret;
    }

    private static T Passthrough<T>(Func<T> func) => func();

    private async Task<T> TimedOffload<T>(Func<Task<T>> task, ObjectPool<int>? choker, string funcName, int taskNo, int totalTask, string content)
    {
        using var unused = choker?.Borrow();
        _logger.Write(ThreadedHeader, $"[{taskNo}/{totalTask}] {funcName} starting",
            LogSegment.LogSegmentType.Message, content);
        var epoch = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        var ret = await task();
        _logger.Write(ThreadedHeader, $"[{taskNo}/{totalTask}] {funcName} finished " +
                                      $"in {DateTimeOffset.Now.ToUnixTimeMilliseconds() - epoch} ms",
            LogSegment.LogSegmentType.Message, content);
        return ret;
    }

    private async Task DualSync(AbstractNewsOutlet.ArticleScrapeResult[] articles, bool[] completionState)
    {
        using var unused1 = _loggerFactory.Create("RunHarvestAsync/@ExtractAndSaveTopics");
        using var unused2 = _loggerFactory.Create("RunHarvestAsync/@SummarizeAndSaveArticle");
        var extractCount = 0;
        var summarizeCount = 0;
        var analyzeTasks = (from article in articles
            select Passthrough(() =>
            {
                if (!completionState[extractCount])
                {
                    var currExtract = ++extractCount;
                    var currSummarize = ++summarizeCount;
                    _logger.Write(ThreadedHeader,
                        $"[{currExtract}/{articles.Length}] {nameof(ExtractAndSaveTopics)} skipped " +
                        "due to synchronization failure", LogSegment.LogSegmentType.Message);
                    _logger.Write(ThreadedHeader,
                        $"[{currSummarize}/{articles.Length}] {nameof(SummarizeAndSaveArticle)} skipped " +
                        "due to synchronization failure", LogSegment.LogSegmentType.Message);
                    return new[] { Task.FromResult(false), Task.FromResult(false) };
                }
                else
                {
                    var currExtract = ++extractCount;
                    var currSummarize = ++summarizeCount;
                    return new[]
                    {
                        TimedOffload(() => ExtractAndSaveTopics(article), _extractorChoker, nameof(ExtractAndSaveTopics),
                            currExtract, articles.Length, article.Url),
                        TimedOffload(() => SummarizeAndSaveArticle(article), _summarizerChoker, nameof(SummarizeAndSaveArticle),
                            currSummarize, articles.Length, article.Url)
                    };
                }
            })).SelectMany(o => o);
        var syncResults = await Task.WhenAll(analyzeTasks);
        var completed = from result in syncResults where result select result;
        _logger.Write(ThreadedHeader, "ExtractAndSaveTopics and SummarizeAndSaveArticle completed: " +
                                      $"{completed.Count()}/{syncResults.Length}",
            LogSegment.LogSegmentType.Message);
    }

    private async Task SequentialSync(AbstractNewsOutlet.ArticleScrapeResult[] articles, bool[] completionState)
    {
        {
            using var unused1 = _loggerFactory.Create("RunHarvestAsync/@ExtractAndSaveTopics");
            var extractCount = 0;
            var extractionTasks = from article in articles select Passthrough(() =>
            {
                var currExtract = ++extractCount;
                if (completionState[extractCount - 1])
                    return TimedOffload(() => ExtractAndSaveTopics(article), _extractorChoker, nameof(ExtractAndSaveTopics),
                        currExtract, articles.Length, article.Url);
                _logger.Write(ThreadedHeader,
                    $"[{currExtract}/{articles.Length}] {nameof(ExtractAndSaveTopics)} skipped " +
                    "due to synchronization failure", LogSegment.LogSegmentType.Message);
                return Task.FromResult(false);
            });
            var syncResults = await Task.WhenAll(extractionTasks);
            var completed = from result in syncResults where result select result;
            _logger.Write(ThreadedHeader, $"{nameof(ExtractAndSaveTopics)} completed: " +
                                          $"{completed.Count()}/{syncResults.Length}",
                LogSegment.LogSegmentType.Message);
        }
        {
            using var unused2 = _loggerFactory.Create("RunHarvestAsync/@SummarizeAndSaveArticle");
            var summarizeCount = 0;
            var summarizationTasks = from article in articles select Passthrough(() =>
            {
                var currExtract = ++summarizeCount;
                if (completionState[summarizeCount - 1])
                    return TimedOffload(() => SummarizeAndSaveArticle(article), _summarizerChoker, nameof(SummarizeAndSaveArticle),
                        currExtract, articles.Length, article.Url);
                _logger.Write(ThreadedHeader,
                    $"[{summarizeCount}/{articles.Length}] {nameof(SummarizeAndSaveArticle)} skipped " +
                    "due to synchronization failure", LogSegment.LogSegmentType.Message);
                return Task.FromResult(false);
            });
            var syncResults = await Task.WhenAll(summarizationTasks);
            var completed = from result in syncResults where result select result;
            _logger.Write(ThreadedHeader, $"{nameof(SummarizeAndSaveArticle)} completed: " +
                                          $"{completed.Count()}/{syncResults.Length}",
                LogSegment.LogSegmentType.Message);
        }
    }
    
    private async Task RunHarvestAsync()
    {
        try
        {
            using var unused = _loggerFactory.Create("RunHarvestAsync");
            using var wrapped = _queryFactory.Borrow();
            var db = wrapped.GetInstance();
            {
                var curr = await db.TryMappedQuery<IntegerStruct>(
                    "INSERT INTO scrape_sessions (time_initialized, time_end, is_finished) " +
                    "VALUES (@init, @end, false) RETURNING id AS IntegerValue;",
                    new { init = DateTime.Now, end = _lastHarvestTime });
                _currentSessionId = curr.FirstOrDefault().IntegerValue;
            }

            IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>[] results; 
            {
                using var aggregationLogger = _loggerFactory.Create("RunHarvestAsync/@AggregateFrontpage");
                results = await Task.WhenAll(from pair in _settings.Outlets
                    select Task.Run(async () => await _harvester.AggregateFrontpage(pair.Value, _settings.DefaultQueryOption)));
            }

            var articles = results.SelectMany(o => o).ToArray();
            bool[] completionState;
            {
                using var unused1 = _loggerFactory.Create("RunHarvestAsync/@InsertArticle");
                var insertCount = 0;
                var syncTasks = from article in articles
                    select Passthrough(() =>
                    {
                        var currInsert = ++insertCount;
                        return TimedOffload(() => InsertArticle(article), null, "InsertArticle",
                            currInsert, articles.Length, article.Url);
                    });
                completionState = await Task.WhenAll(syncTasks);
            }
            if (_settings.UseDualSyncMode)
                await DualSync(articles, completionState);
            else await SequentialSync(articles, completionState);
            
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
                                               "(SELECT url AS article_url FROM scrape_results WHERE " +
                                               "time_posted < @time); " +
                                               "DELETE FROM extracted_keywords WHERE article_url IN " +
                                               "(SELECT url AS article_url FROM scrape_results WHERE " +
                                               "time_posted < @time); " +
                                               "DELETE FROM summarization_results WHERE article_url IN " +
                                               "(SELECT url AS article_url FROM scrape_results WHERE " +
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

    public Task<int> QueryAmountAsync(AbstractGrader.GraderSettings settings)
    {
        if (settings.OutletSelections == null! || settings.OutletSelections.Length == 0)
            settings.OutletSelections = _allOutlets;
        return _grader.QueryAmountAsync(settings);
    }
    
    public Task<Dictionary<string, double>> GradeRelevancyAsync(AbstractGrader.GraderSettings settings)
    {
        if (settings.OutletSelections == null! || settings.OutletSelections.Length == 0)
            settings.OutletSelections = _allOutlets;
        return _grader.GradeRelevancyAsync(settings);
    }

    public async Task<AbstractNewsOutlet.ArticleScrapeResult[]> QueryArticles(DateTime timeFrom, DateTime timeTo)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var article = await db.TryMappedQuery<AbstractNewsOutlet.ArticleScrapeResult>(
            "SELECT url AS Url," +
            "outlet_url As OutletUrl, " +
            "language AS Lang, " +
            "title AS Title, " +
            "author As Author, " +
            "time_posted As TimePosted, " +
            "original_text As Text, " +
            "word_count As WordCount FROM scrape_results " +
            "WHERE time_posted > @timeFrom AND time_posted <= @timeTo;",
            new { timeFrom, timeTo });
        return article.ToArray();
    }

    public async Task<AbstractNewsOutlet.ArticleScrapeResult?> QueryArticle(string articleUrl)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var article = (await db.TryMappedQuery<AbstractNewsOutlet.ArticleScrapeResult>(
            "SELECT url AS Url," +
            "outlet_url As OutletUrl, " +
            "language AS Lang, " +
            "title AS Title, " +
            "author As Author, " +
            "time_posted As TimePosted, " +
            "original_text As Text, " +
            "word_count As WordCount FROM scrape_results " +
            "WHERE url = @url;", new { url = articleUrl })).FirstOrDefault();
        return article;
    }

    public async Task<string[]> QueryArticleTags(string articleUrl)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var tags = (await db.TryMappedQuery<PostgresGrader.StringRepStruct>(
                "SELECT tag AS StringRep " +
                "FROM tags_used " +
                "WHERE article_url = @url;", new { url = articleUrl }))
            .Select(o => o.StringRep).ToArray();
        return tags;
    }
    
    public async Task<Dictionary<string, double>> QueryArticleKeywords(string articleUrl)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var keywords = await db.TryMappedQuery<PostgresGrader.Topic>(
            "SELECT keyword AS TopicName, " +
            "relevancy AS Relevancy " +
            "FROM extracted_keywords " +
            "WHERE article_url = @url;", new { url = articleUrl });
        return keywords.ToDictionary(o => o.TopicName, o => o.Relevancy);
    }

    public async Task<string> QuerySummarizedText(string articleUrl)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var summarized = (await db.TryMappedQuery<PostgresGrader.StringRepStruct>(
            "SELECT summarized_text AS StringRep " +
            "FROM summarization_results " +
            "WHERE article_url = @url;", new { url = articleUrl })).FirstOrDefault();
        return summarized.StringRep;
    }

    public async Task<IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>> ImpromptuFrontPageScrape(string outletName)
    {
        using var aggregationLogger = _loggerFactory.Create(nameof(ImpromptuFrontPageScrape));
        var results = await Task.Run(async () =>
            await _harvester.AggregateFrontpage(outletName, _settings.DefaultQueryOption));
        return results;
    }

    public async Task<ScrapeSession> GetLatestSession()
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var ret = (await db.TryMappedQuery<ScrapeSession>(
            "SELECT time_initialized AS StartTime, " +
            "time_end AS ConclusionTime, " +
            "is_finished AS IsFinished " +
            "FROM scrape_sessions ORDER BY time_initialized DESC LIMIT 1")).ToArray();
        var session = ret.First();
        if (session.IsFinished == false) session.ConclusionTime = DateTime.MaxValue;
        return session;
    }
    
    public async Task<ScrapeSession> GetLatestSession(bool isFinished)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var ret = (await db.TryMappedQuery<ScrapeSession>(
            "SELECT time_initialized AS StartTime, " +
            "time_end AS ConclusionTime, " +
            "is_finished AS IsFinished " +
            "FROM scrape_sessions " +
            "WHERE is_finished = @state " +
            "ORDER BY time_initialized DESC LIMIT 1",
            new { state = isFinished })).ToArray();
        var session = ret.First();
        if (session.IsFinished == false) session.ConclusionTime = DateTime.MaxValue;
        return session;
    }
    
    public async Task<IEnumerable<ScrapeSession>> GetSessions(DateTime timeFrom, DateTime timeTo)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var arr = await db.TryMappedQuery<ScrapeSession>(
            "SELECT time_initialized AS StartTime, " +
            "time_end AS ConclusionTime, " +
            "is_finished AS IsFinished " +
            "FROM scrape_sessions " +
            "WHERE time_initialized > @timeFrom " +
            "AND time_initialized <= @timeTo " +
            "AND time_end > @timeFrom " +
            "AND time_end <= @timeTo " +
            "ORDER BY time_initialized", new { timeFrom, timeTo });
        return from session in arr
            select !session.IsFinished
                ? new ScrapeSession
                {
                    StartTime = session.StartTime,
                    ConclusionTime = DateTime.MaxValue,
                    IsFinished = false
                }
                : session;
    }
    
    public async Task<IEnumerable<ScrapeSession>> GetSessions(DateTime timeFrom, DateTime timeTo, bool isFinished)
    {
        using var wrapped = _queryFactory.Borrow();
        var db = wrapped.GetInstance();
        var arr = await db.TryMappedQuery<ScrapeSession>(
            "SELECT time_initialized AS StartTime, " +
            "time_end AS ConclusionTime, " +
            "is_finished AS IsFinished " +
            "FROM scrape_sessions " +
            "WHERE time_initialized > @timeFrom " +
            "AND time_initialized <= @timeTo " +
            "AND time_end > @timeFrom " +
            "AND time_end <= @timeTo " +
            "AND is_finished = @state " +
            "ORDER BY time_initialized", new { timeFrom, timeTo, state = isFinished });
        if (!isFinished)
            return from session in arr
                select new ScrapeSession
                {
                    StartTime = session.StartTime,
                    ConclusionTime = DateTime.MaxValue,
                    IsFinished = false
                };
        return arr;
    }
}