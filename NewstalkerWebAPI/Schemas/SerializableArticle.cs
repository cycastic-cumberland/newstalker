using NewstalkerExtendedComponents;

namespace NewstalkerWebAPI.Schemas;

public struct SerializableArticle
{
    public string OutletUrl { get; set; }
    public string Lang { get; set; } 
    public string Url { get; set; } 
    public string Title { get; set; } 
    public string Author { get; set; } 
    public string Text { get; set; } 
    public DateTime TimePosted{ get; set; } 
    public int WordCount { get; set; }
    public string[] Tags { get; set; }

    public static SerializableArticle From(AbstractNewsOutlet.ArticleScrapeResult article)
    {
        return new()
        {
            OutletUrl = article.OutletUrl,
            Lang = article.Lang,
            Url = article.Url,
            Title = article.Title,
            Author = article.Author,
            Text = article.Text,
            TimePosted = article.TimePosted,
            WordCount = article.WordCount,
            Tags = article.Tags ?? Array.Empty<string>(),
        };
    }

    public static AbstractNewsOutlet.ArticleScrapeResult To(SerializableArticle article)
    {
        return new()
        {
            OutletUrl = article.OutletUrl,
            Lang = article.Lang,
            Url = article.Url,
            Title = article.Title,
            Author = article.Author,
            Text = article.Text,
            TimePosted = article.TimePosted,
            WordCount = article.WordCount,
            Tags = article.Tags ?? Array.Empty<string>(),
        };
    }
}