using ExtendedComponents;
using NewstalkerExtendedComponents;
using PostgresDriver;

namespace NewstalkerPostgresGrader;

public struct PostgresGraderSettings
{
    public double TagsWeight;
}

public class PostgresGrader : AbstractGrader
{
    private struct CountStruct
    {
        public long Count;
    }
    public struct StringRepStruct
    {
        public string StringRep;
    }
    public struct Topic
    {
        public string TopicName;
        public double Relevancy;

        public void Deconstruct(out string topicName, out double relevancy)
        {
            topicName = TopicName;
            relevancy = Relevancy;
        }
    }
    private class DoubleWrapper
    {
        public double Value;
    }

    private readonly double _tagsWeight;
    private readonly double _kwWeight;
    private readonly LoggingServer _logger;
    private readonly ObjectPool<PostgresProvider> _queryFactory;
    private readonly AutoLoggerFactory _loggerFactory;
    
    private int GetHash() => GetHashCode();
    private readonly string _header;
    private string ThreadedHeader => $"{_header}:{Environment.CurrentManagedThreadId}";

    public PostgresGrader(ObjectPool<PostgresProvider> queryFactory, PostgresGraderSettings settings, LoggingServer logger)
    {
        _header = $"DelegatedPostgresGrader:{GetHash()}";
        _queryFactory = queryFactory;
        _logger = logger;
        _tagsWeight = settings.TagsWeight;
        _kwWeight = 1.0 - _tagsWeight;
        _loggerFactory = new("DelegatedPostgresGrader", _logger);
        _logger.Write(_header, "DelegatedPostgresGrader online", LogSegment.LogSegmentType.Message);
    }
    public override void Dispose()
    {
        _queryFactory.Dispose();
        base.Dispose();
    }

    private static (Dictionary<string, int> processedArray, int totalAppearance) CountUnique(IEnumerable<StringRepStruct> strings)
    {
        Dictionary<string, int> ret = new();
        int totalAppearance = 0;
        foreach (var str in strings)
        {
            var key = str.StringRep;
            if (ret.ContainsKey(key))
                ret[key] += 1;
            else ret[key] = 1;
            totalAppearance += 1;
        }
        return (ret, totalAppearance);
    }
    private (DateTime timeBegin, DateTime timeEnd) GetTimeVector(TimeSpan span, DateTime? epoch)
    {
        var now = epoch ?? DateTime.UtcNow;
        return (now - span, now);
    }

    private async Task<Dictionary<string, double>> GradeTemplateAsync(string methodName, string query, object parameters)
    {
        try
        {
            using var unused = _loggerFactory.Create(methodName);
            using var wrapped = _queryFactory.Borrow();
            var db = wrapped.GetInstance();
            var returnedArray = await db.TryMappedQuery<StringRepStruct>(query, parameters);
            var (processed, maxAppearance) = CountUnique(returnedArray);
            return processed.ToDictionary(o => o.Key, o => (double)o.Value / maxAppearance);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, $"{methodName} thrown in QueryFrontPageAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return new();
        }
    }
    private async Task<Dictionary<string, double>> GradeTagsRelevancyAsync(GraderSettings settings)
    {
        var (timeBegin, timeEnd) = GetTimeVector(settings.PopularityWindow, settings.TimeEnd);
        return await GradeTemplateAsync("GradeTagsRelevancyAsync",
            "SELECT tags_used.tag As StringRep FROM scrape_results " +
            "INNER JOIN tags_used ON scrape_results.url = tags_used.article_url " +
            "WHERE scrape_results.time_posted > @timeBegin AND scrape_results.time_posted < @timeEnd " +
            "AND scrape_results.outlet_url = ANY(@outlets);",
            new { timeBegin, timeEnd, outlets = settings.OutletSelections });
    }

    private async Task<Dictionary<string, double>> GradeKeywordsRelevancyAsync(GraderSettings settings)
    {
        var (timeBegin, timeEnd) = GetTimeVector(settings.PopularityWindow, settings.TimeEnd);
        return await GradeTemplateAsync("GradeTagsRelevancyAsync",
            "SELECT extracted_keywords.keyword As StringRep FROM scrape_results " +
            "INNER JOIN extracted_keywords ON scrape_results.url = extracted_keywords.article_url " +
            "WHERE scrape_results.time_posted > @timeBegin AND scrape_results.time_posted < @timeEnd " +
            "AND scrape_results.outlet_url = ANY(@outlets);",
            new { timeBegin, timeEnd, outletUrls = settings.OutletSelections, outlets = settings.OutletSelections });
    }

    private async Task<(string url, double relevancy)> GradeArticleCombinedRelevancyAsync(GraderSettings settings, string articleUrl,
        IReadOnlyDictionary<string, double> tagsRelevancyTable, IReadOnlyDictionary<string, double> keywordsRelevancyTable, DoubleWrapper wrappedDouble)
    {
        var tagsWeight = settings.NormalizedScale == 0.0 ? _tagsWeight : settings.NormalizedScale;
        var kwWeight = settings.NormalizedScale == 0.0 ? _kwWeight : 1.0 - settings.NormalizedScale;
        var (timeBegin, timeEnd) = GetTimeVector(settings.PopularityWindow, settings.TimeEnd);
        try
        {
            var fetchTagsTask = Task.Run(async () =>
            {
                using var wrapped = _queryFactory.Borrow();
                var db = wrapped.GetInstance();
                return await db.TryMappedQuery<StringRepStruct>(
                    "SELECT tags_used.tag As StringRep FROM scrape_results " +
                    "INNER JOIN tags_used ON scrape_results.url = tags_used.article_url " +
                    "WHERE scrape_results.time_posted > @timeBegin AND scrape_results.time_posted < @timeEnd " +
                    "AND url = @url " +
                    "AND scrape_results.outlet_url = ANY(@outletUrls);",
                    new { timeBegin, timeEnd, url = articleUrl, outletUrls = settings.OutletSelections });
            });
            var fetchKeywordsTask = Task.Run(async () =>
            {
                using var wrapped = _queryFactory.Borrow();
                var db = wrapped.GetInstance();
                return await db.TryMappedQuery<Topic>(
                    "SELECT extracted_keywords.keyword As TopicName, " +
                    "extracted_keywords.relevancy as Relevancy FROM scrape_results " +
                    "INNER JOIN extracted_keywords ON scrape_results.url = extracted_keywords.article_url " +
                    "WHERE scrape_results.time_posted > @timeBegin AND scrape_results.time_posted < @timeEnd " +
                    "AND url = @url " +
                    "AND scrape_results.outlet_url = ANY(@outletUrls);",
                    new { timeBegin, timeEnd, url = articleUrl, outletUrls = settings.OutletSelections });
            });
            var tags = await fetchTagsTask;
            var keywords = await fetchKeywordsTask;
            double tagRelevancy = 0;
            foreach (var tag in tags)
            {
                if (!tagsRelevancyTable.TryGetValue(tag.StringRep, out var rel)) continue;
                tagRelevancy += rel;
            }

            double keywordRelevancy = 0;
            foreach (var kw in keywords)
            {
                if (!keywordsRelevancyTable.TryGetValue(kw.TopicName, out var rel)) continue;
                keywordRelevancy += kw.Relevancy * rel;
            }

            var combinedRelevancy = (tagRelevancy * tagsWeight) + (keywordRelevancy * kwWeight);
            lock (wrappedDouble)
            {
                wrappedDouble.Value += combinedRelevancy;
            }
            return (articleUrl, combinedRelevancy);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "GradeArticleCombinedRelevancyAsync thrown in QueryFrontPageAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return ("", 0);
        }
    }

    private async Task<Dictionary<string, double>> GradeArticlesRelevancyAsync(GraderSettings settings)
    {
        var tagsRelevancyTask = GradeTagsRelevancyAsync(settings);
        var keywordsRelevancyTask = GradeKeywordsRelevancyAsync(settings);
        using var unused = _loggerFactory.Create("GradeArticlesRelevancyAsync");
        var (timeBegin, timeEnd) = GetTimeVector(settings.PopularityWindow, settings.TimeEnd);
        try
        {
            using var wrapped = _queryFactory.Borrow();
            var db = wrapped.GetInstance();
            var articles = await db.TryMappedQuery<StringRepStruct>(
                "SELECT scrape_results.url As StringRep FROM scrape_results " +
                "WHERE scrape_results.time_posted > @timeBegin AND scrape_results.time_posted < @timeEnd " +
                "AND scrape_results.outlet_url = ANY(@outletUrls);",
                new { timeBegin, timeEnd, outletUrls = settings.OutletSelections });
            var tagsTable = await tagsRelevancyTask;
            var keywordsTable = await keywordsRelevancyTask;
            var wrappedTotal = new DoubleWrapper
            {
                Value = 0.0
            };
            
            var combined = await Task.WhenAll(from article in articles
                select Task.Run(async () =>
                    await GradeArticleCombinedRelevancyAsync(settings, article.StringRep, tagsTable, keywordsTable,
                        wrappedTotal)));
            
            var total = wrappedTotal.Value == 0.0 ? 1.0 : wrappedTotal.Value;
            return combined.Where(o => o.url != "")
                .ToDictionary(o => o.url, o => o.relevancy / total);
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, "GradeArticlesRelevancyAsync thrown in QueryFrontPageAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return new();
        }
    }
    public override async Task<Dictionary<string, double>> GradeRelevancyAsync(GraderSettings settings)
    {
        return settings.GradingTarget switch
        {
            GradeType.Tags => await GradeTagsRelevancyAsync(settings),
            GradeType.Keywords => await GradeKeywordsRelevancyAsync(settings),
            GradeType.Articles => await GradeArticlesRelevancyAsync(settings),
            _ => throw new ArgumentOutOfRangeException(nameof(settings))
        };
    }
    private async Task<int> QueryTemplateAsync(string methodName, string query, object parameters)
    {
        try
        {
            using var unused = _loggerFactory.Create(methodName);
            using var wrapped = _queryFactory.Borrow();
            var db = wrapped.GetInstance();
            var returnedArray = await db.TryMappedQuery<CountStruct>(query, parameters);
            return (int)returnedArray.FirstOrDefault().Count;
        }
        catch (Exception e)
        {
            _logger.Write(ThreadedHeader, $"{methodName} thrown in QueryFrontPageAsync",
                LogSegment.LogSegmentType.Exception, e.ToString());
            return 0;
        }
    }
    private async Task<int> QueryTagsAmountAsync(GraderSettings settings)
    {
        var (timeBegin, timeEnd) = GetTimeVector(settings.PopularityWindow, settings.TimeEnd);
        return await QueryTemplateAsync("QueryTagsAmountAsync",
            "SELECT COUNT(scrape_results.word_count) AS Count FROM scrape_results " +
            "INNER JOIN tags_used ON scrape_results.url = tags_used.article_url " +
            "WHERE scrape_results.time_posted > @timeBegin AND scrape_results.time_posted < @timeEnd " +
            "AND tags_used.tag LIKE @tagName AND outlet_url = ANY(@outlets);",
            new { timeBegin, timeEnd, tagName = settings.TargetSelection, outlets = settings.OutletSelections });
    }
    private async Task<int> QueryKeywordsAmountAsync(GraderSettings settings)
    {
        var (timeBegin, timeEnd) = GetTimeVector(settings.PopularityWindow, settings.TimeEnd);
        return await QueryTemplateAsync("QueryKeywordsAmountAsync",
            "SELECT COUNT(scrape_results.word_count) AS Count FROM scrape_results " +
            "INNER JOIN extracted_keywords ON scrape_results.url = extracted_keywords.article_url " +
            "WHERE scrape_results.time_posted > @timeBegin AND scrape_results.time_posted < @timeEnd " +
            "AND extracted_keywords.keyword LIKE @topic AND outlet_url = ANY(@outlets);",
            new { timeBegin, timeEnd, topic = settings.TargetSelection, outlets = settings.OutletSelections });
    }
    private async Task<int> QueryArticlesAmountAsync(GraderSettings settings)
    {
        var (timeBegin, timeEnd) = GetTimeVector(settings.PopularityWindow, settings.TimeEnd);
        return await QueryTemplateAsync("QueryArticlesAmountAsync",
            "SELECT COUNT(word_count) AS Count FROM scrape_results " +
            "WHERE time_posted > @timeBegin AND time_posted < @timeEnd " +
            "AND outlet_url = ANY(@outlets)",
            new { timeBegin, timeEnd, outletUrls = settings.OutletSelections, outlets = settings.OutletSelections });
    }
    
    public override async Task<int> QueryAmountAsync(GraderSettings settings)
    {
        return settings.GradingTarget switch
        {
            GradeType.Tags => await QueryTagsAmountAsync(settings),
            GradeType.Keywords => await QueryKeywordsAmountAsync(settings),
            GradeType.Articles => await QueryArticlesAmountAsync(settings),
            _ => throw new ArgumentOutOfRangeException(nameof(settings))
        };
    }
}