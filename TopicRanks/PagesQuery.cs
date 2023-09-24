using Newtonsoft.Json;
using RecentChangeETL;

namespace TopicRanks;

public class SinglePage
{
    [JsonProperty("pageid")] public long PageId;
    [JsonProperty("ns")] public long Ns;
    [JsonProperty("title")] public string Title = "";
}

public class Pages
{
    [JsonProperty("pages")] public Dictionary<string, SinglePage> AllPages = new();
}

public class PagesQuery : WikiQuery<Pages> { }
