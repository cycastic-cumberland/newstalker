namespace NewstalkerPostgresETL;

public sealed class TuoiTreOutlet : StandardizedScrapperBasedOutlet
{
    public const string BaseUrl = "https://tuoitre.vn/";
    private const string ContentClass = "detail-content";
    private const string TitleClass = "article-title";
    private const string AuthorClass = "author-info";
    private const string TagsClass = "detail-cate";
    private const string DateClass = "detail-time";

    private static readonly TextProcessing.TextExtractionOptions ContentOption = new()
    {
        ExcludedParentsWithClasses = (new[] { "PhotoCMS_Caption", "PhotoCMS_Author", "VideoCMS_Caption" }, 2),
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
    protected override string GetCommentSectionClass() => "lst-comment";
    protected override string GetCommentClass() => "item-comment";
    protected override string GetCommentUserClass() => "name";
    protected override string GetCommentTextClass() => "contentcomment";
    protected override TextProcessing.TextExtractionOptions GetContentScrappingOptions() => ContentOption;
}