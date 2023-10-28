namespace NewstalkerExtendedComponents;

public struct ScrapeSession
{
    public DateTime StartTime { get; set; }
    public DateTime ConclusionTime { get; set; }
    public bool IsFinished { get; set; }
}

public interface INewstalkerConductor
{
    public DateTime GetNextHarvestTime();
    public Task<string> SummarizeArticle(AbstractNewsOutlet.ArticleScrapeResult article);
    public Task<Dictionary<string, double>> ExtractTopics(AbstractNewsOutlet.ArticleScrapeResult article);
    public Task<int> RunGarbageCollectionAsync();
    public Task<int> QueryAmountAsync(AbstractGrader.GraderSettings settings);
    public Task<Dictionary<string, double>> GradeRelevancyAsync(AbstractGrader.GraderSettings settings);
    public Task<AbstractNewsOutlet.ArticleScrapeResult[]> QueryArticles(DateTime timeFrom, DateTime timeTo);
    public Task<AbstractNewsOutlet.ArticleScrapeResult?> QueryArticle(string articleUrl);
    public Task<string[]> QueryArticleTags(string articleUrl);
    public Task<Dictionary<string, double>> QueryArticleKeywords(string articleUrl);
    public Task<string> QuerySummarizedText(string articleUrl);
    public Task<IEnumerable<AbstractNewsOutlet.ArticleScrapeResult>> ImpromptuFrontPageScrape(string outletName);
    public Task<ScrapeSession> GetLatestSession();
    public Task<ScrapeSession> GetLatestSession(bool isFinished);
    public Task<IEnumerable<ScrapeSession>> GetSessions(DateTime timeFrom, DateTime timeTo);
    public Task<IEnumerable<ScrapeSession>> GetSessions(DateTime timeFrom, DateTime timeTo, bool isFinished);
    
}