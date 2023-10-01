using Newtonsoft.Json;

namespace WikiStalkerExtendedComponents;

public class RecentChanges
{
    [JsonProperty("recentchanges")] public RawWikipediaRecentChange[] Changes = null!;
}

public class RecentChangesQuery : WikiQuery<RecentChanges> { }