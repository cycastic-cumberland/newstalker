using System.Net;
using System.Text.RegularExpressions;
using ExtendedComponents;
using HtmlAgilityPack;
using NewstalkerExtendedComponents;

namespace NewstalkerPostgresETL;

public abstract class StandardizedScrapperBasedOutlet : AbstractNewsOutlet
{
    public class OutletSettings
    {
        public uint CacheCapacity;
    }
    private readonly ObjectPool<HtmlWeb> _webPool = new SynchronousObjectPool<HtmlWeb>(() => new ());
    private readonly LeastRecentlyUsedCache<string, HtmlDocument> _documentCache;
    private readonly OutletSettings _settings;
    protected abstract string GetContentClass();
    protected abstract string GetTitleClass();
    protected abstract string GetAuthorClass();
    protected abstract string GetTagsClass();
    protected abstract string GetDateClass();
    protected abstract string GetDefaultLanguage();
    protected abstract string GetCommentSectionClass();
    protected abstract string GetCommentClass();
    protected abstract string GetCommentUserClass();
    protected abstract string GetCommentTextClass();

    protected string HomeUrl
    {
        get
        {
            var url = GetBaseUrl();
            if (url.EndsWith('/')) return url;
            return url + '/';
        }
    }
    protected abstract TextProcessing.TextExtractionOptions GetContentScrappingOptions();

    protected StandardizedScrapperBasedOutlet(OutletSettings? settings = null)
    {
        if (settings == null)
            _settings = new()
            {
                CacheCapacity = 64,
            };
        else _settings = settings;
        _documentCache = new ThreadSafeLeastRecentlyUsedCache<string, HtmlDocument>((int)_settings.CacheCapacity);
    }
    
    public override void Dispose()
    {
        _webPool.Dispose();
        base.Dispose();
    }
    public override async Task<IEnumerable<string>> QueryFrontPageAsync(FrontPageQueryOptions options)
    {
        using var webpack = _webPool.Borrow();
        var htmlWeb = webpack.GetInstance();
        var unprocessedLinks = await Utilities.LinksScrape(GetBaseUrl(), htmlWeb);
        var homeUrl = HomeUrl;
        HashSet<string> hashSet = new(unprocessedLinks.Where(link => link.StartsWith(homeUrl) || link.StartsWith('/')).Select(link =>
        {
            if (link != homeUrl && link.StartsWith('/')) return homeUrl + link.Substring(1, link.Length - 1);
            return link;
        }));
        return hashSet;
    }

    protected async Task<HtmlDocument?> GetDocument(string url)
    {
        var cached = _documentCache.Get(url);
        if (cached != null) return cached;
        using var webpack = _webPool.Borrow();
        var htmlWeb = webpack.GetInstance();
        var doc = await htmlWeb.LoadFromWebAsync(url);
        if (doc != null) _documentCache.Add(url, doc);
        return doc;
    }

    private static bool IsValidString(string self, out string ret)
    {
        ret = self;
        return !(string.IsNullOrEmpty(self) || string.IsNullOrWhiteSpace(self));
    }
    
    public override async Task<ArticleScrapeResult?> QueryArticleAsync(string url)
    {
        string author = "";
        string title = "";
        string[] tags = Array.Empty<string>();
        string dateInput = "";
        string contentText = "";
        int totalWordCount = 0;
        
        var doc = await GetDocument(url);
        if (doc == null) return null;

        var scrapeTasks = new[]
        {
            Task.Run(() => TextProcessing.SetFirstInnerTextFromClass(doc, GetTitleClass(), ref title)),
            Task.Run(() => TextProcessing.SetFirstInnerTextFromClass(doc, GetDateClass(), ref dateInput)),
            Task.Run(() =>
            {
                TextProcessing.SetFirstInnerTextFromClass(doc, GetAuthorClass(), ref author);
                var matches = Regex.Match(author, @"[^\r\n]+");
                try
                {
                    author = matches.Captures[0].ToString().TrimEnd();
                }
                catch (Exception)
                {
                    author = "";
                }
            }),
            Task.Run(() =>
            {
                var nodes = Utilities.GetNodesFromClass(doc, GetTagsClass());
                var root = nodes.FirstOrDefault();
                if (root == null) return;
                string ret = "";
                var converted = from node in root.DescendantsAndSelf()
                    where
                        !node.HasChildNodes &&
                        !node.ParentNode.Name.Equals("script", StringComparison.OrdinalIgnoreCase) &&
                        IsValidString(node.InnerText, out ret)
                    select ret.Trim().ToLower();
                tags = converted.ToArray();
            }),
            Task.Run(async () =>
            {
                try
                {
                    var (text, wordCount) = await TextProcessing.ExtractAllSubText(doc, GetContentClass(), GetContentScrappingOptions());
                    contentText = text.Trim();
                    totalWordCount = wordCount;
                }
                catch (Exception)
                {
                    // Ignored
                }
            })
        };
        await Task.WhenAll(scrapeTasks);
        title = title.Trim();
        if (title == "" || contentText == "") return null;
        return new()
        {
            OutletUrl = GetBaseUrl(),
            Lang = GetDefaultLanguage(),
            Author = author,
            Tags = tags,
            Text = contentText,
            TimePosted = Utilities.StandardizedTimeProcess(dateInput),
            Title = WebUtility.HtmlDecode(title),
            Url = url,
            WordCount = totalWordCount,
        };
    }

    private CommentScrapeResult? QuerySingleCommentAsync(string hostUrl, HtmlNode commentNode)
    {
        var userNode = Utilities.GetFirstNodeFromClass(commentNode, GetCommentUserClass());
        if (userNode == null) return null;
        var contentNode = Utilities.GetFirstNodeFromClass(commentNode, GetCommentTextClass());
        if (contentNode == null) return null;
        var result = new CommentScrapeResult
        {
            OutletUrl = GetBaseUrl(),
            Lang = GetDefaultLanguage(),
            Url = hostUrl,
            Username = userNode.InnerText.Trim(),
            Comment = contentNode.InnerText.Trim()
        };
        return result;
    }

    public override async Task<IEnumerable<CommentScrapeResult>> QueryCommentSectionAsync(string url)
    {
        var doc = await GetDocument(url);
        if (doc == null) return Array.Empty<CommentScrapeResult>();

        var commentSection = Utilities.GetFirstNodeFromClass(doc, GetCommentSectionClass());
        if (commentSection == null) return Array.Empty<CommentScrapeResult>();

        var commentNodes = Utilities.GetNodesFromClass(commentSection, GetCommentClass());
        var comments = await Task.WhenAll(from commentNode in commentNodes
            select Task.Run(() => QuerySingleCommentAsync(url, commentNode)));
        
        return from comment in comments where comment != null select comment;
    }
}