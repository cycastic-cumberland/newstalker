namespace NewstalkerExtendedComponents;

public class ExtractionFailedException : Exception
{
    public ExtractionFailedException() {}
    public ExtractionFailedException(string msg) : base(msg) {}
}

public abstract class AbstractNewsOutlet : IDisposable
{
    public struct FrontPageQueryOptions
    {
        public enum QueryType
        {
            All,
            Articles,
            Sections
        }

        public QueryType Type;
        public int Limit;
    }

    public class ScrapeResult
    {
        public string OutletUrl = "";
        public string Lang = "en_US";
        public string Url = "";
    }
    
    public class ArticleScrapeResult : ScrapeResult
    {
        public string Title = "";
        public string Author = "";
        public string Text = "";
        public string[] Tags = Array.Empty<string>();
        public DateTime TimePosted;
        public int WordCount;
    }

    public class CommentScrapeResult : ScrapeResult
    {
        public string Username = "";
        public string Comment = "";
    }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    public abstract string GetBaseUrl();
    public abstract IEnumerable<string> GetBlackListedUrls();
    public abstract Task<IEnumerable<string>> QueryFrontPageAsync(FrontPageQueryOptions options);
    public abstract Task<ArticleScrapeResult?> QueryArticleAsync(string url);
    public abstract Task<IEnumerable<CommentScrapeResult>> QueryCommentSectionAsync(string url);
    public abstract bool BelongsToThisOutlet(string url);
    public IEnumerable<string> QueryFrontPage(FrontPageQueryOptions options)
    {
        var task = QueryFrontPageAsync(options);
        task.Wait();
        return task.Result;
    }

    public ArticleScrapeResult? QueryArticle(string url)
    {
        var task = QueryArticleAsync(url);
        task.Wait();
        return task.Result;
    }
    public IEnumerable<CommentScrapeResult> QueryCommentSection(string url)
    {
        var task = QueryCommentSectionAsync(url);
        task.Wait();
        return task.Result;
    }
}