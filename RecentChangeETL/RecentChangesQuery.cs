using Newtonsoft.Json;

namespace RecentChangeETL;

public class RecentChanges
{
    [JsonProperty("recentchanges")] public RawWikipediaRecentChange[] Changes = null!;
}

public class RecentChangesQuery : WikiQuery<RecentChanges> { }