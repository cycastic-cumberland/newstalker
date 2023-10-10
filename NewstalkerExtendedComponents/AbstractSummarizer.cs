namespace NewstalkerExtendedComponents;

public abstract class AbstractSummarizer : IDisposable
{
    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    // Summarize an article and return a list of keywords, along as their relevancy
    public abstract Task<string> SummarizeArticleAsync(AbstractNewsOutlet.ArticleScrapeResult originalArticle);
    public abstract Task<Dictionary<string, double>> ExtractTopicsAsync(AbstractNewsOutlet.ArticleScrapeResult originalArticle);
 
    public string SummarizeArticle(AbstractNewsOutlet.ArticleScrapeResult originalArticle)
    {
        var task = SummarizeArticleAsync(originalArticle);
        task.Wait();
        return task.Result;
    }
    
    public Dictionary<string, double> ExtractTopics(AbstractNewsOutlet.ArticleScrapeResult originalArticle)
    {
        var task = ExtractTopicsAsync(originalArticle);
        task.Wait();
        return task.Result;
    }
    
}