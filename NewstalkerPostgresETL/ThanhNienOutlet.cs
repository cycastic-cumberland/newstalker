using System.Text;

namespace NewstalkerPostgresETL;

public class ThanhNienOutlet : StandardizedScrapperBasedOutlet
{
    public const string BaseUrl = "https://thanhnien.vn/";
    private const string ContentClass = "detail-content";
    private const string TitleClass = "detail-title";
    private const string AuthorClass = "author-info-top";
    private const string TagsClass = "detail-cate";
    private const string DateClass = "detail-time";
    
    private static readonly TextProcessing.TextExtractionOptions ContentOption = new()
    {
        ExcludedParentsWithClasses = (new[] { "PhotoCMS_Caption", "PhotoCMS_Author", "VideoCMS_Caption" }, 3),
        InlinedLinkAware = (new[] { "link-inline-content", "VCCTagItemInNews" }, 1),
    };
    public override string GetBaseUrl() => BaseUrl;

    public override IEnumerable<string> GetBlackListedUrls() => ArraySegment<string>.Empty;
    protected override string GetContentClass() => ContentClass;
    protected override string GetTitleClass() => TitleClass;
    protected override string GetAuthorClass() => AuthorClass;
    protected override string GetTagsClass() => TagsClass;
    protected override string GetDateClass() => DateClass;
    protected override string GetDefaultLanguage() => "vi_VN";
    protected override string GetCommentSectionClass() => "";
    protected override string GetCommentClass() => "";
    protected override string GetCommentUserClass() => "";
    protected override string GetCommentTextClass() => "";

    protected override TextProcessing.TextExtractionOptions GetContentScrappingOptions() => ContentOption;

    public override async Task<ArticleScrapeResult?> QueryArticleAsync(string url)
    {
        var ret = await base.QueryArticleAsync(url);
        if (ret == null) return null;
        StringBuilder sb = new();
        foreach (var c in ret.Author.TakeWhile(c => c is not ('\n' or '\r')))
        {
            sb.Append(c);
        }

        ret.Author = sb.ToString().TrimEnd();
        return ret;
    }

    public override Task<IEnumerable<CommentScrapeResult>> QueryCommentSectionAsync(string url)
        => Task.FromResult((IEnumerable<CommentScrapeResult>)Array.Empty<CommentScrapeResult>());
}