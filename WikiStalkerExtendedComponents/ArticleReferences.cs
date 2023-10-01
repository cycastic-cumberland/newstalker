using Newtonsoft.Json;

namespace WikiStalkerExtendedComponents;

public class ArticleLink
{
    [JsonProperty("ns")] public long Ns;
    [JsonProperty("exists")] public string Exists = "";
    [JsonProperty("*")] public string Name = "";
}

public class ArticleLinks
{
    [JsonProperty("title")] public string Title = "";
    [JsonProperty("pageid")] public long PageId;
    [JsonProperty("links")] public ArticleLink[] Links = null!;
}

public class ArticleReferences
{
    [JsonProperty("parse")] public ArticleLinks Parse = null!;
}