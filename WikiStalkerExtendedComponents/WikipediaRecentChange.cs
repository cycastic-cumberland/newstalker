using System.Globalization;
using ExtendedComponents;
using Newtonsoft.Json;

namespace WikiStalkerExtendedComponents;

public class RawWikipediaRecentChange
{
    [JsonProperty("type")] public string Type = "";
    [JsonProperty("ns")] public long Ns;
    [JsonProperty("title")] public string Title = "";
    [JsonProperty("pageid")] public long PageId;
    [JsonProperty("revid")] public long RevId;
    [JsonProperty("old_revid")] public long OldRevId;
    [JsonProperty("rcid")] public long RcId;
    [JsonProperty("timestamp")] public string Timestamp = "";

    public WikipediaRecentChange Rectify() => WikipediaRecentChange.FromRaw(this);
}

public class WikipediaRecentChange
{
    public string Hash = "";
    public long Ns;
    public string Title = "";
    public string Type = "";
    public long PageId;
    public long RevId;
    public long OldRevId;
    public long RcId;
    public long Timestamp;

    public static WikipediaRecentChange FromRaw(RawWikipediaRecentChange fromRaw)
    {
        const string iso8601Format = "yyyy-MM-ddTHH:mm:ssZ";
        return new WikipediaRecentChange
        {
            Hash = "",
            Ns = fromRaw.Ns,
            Title = fromRaw.Title,
            Type = fromRaw.Type,
            PageId = fromRaw.PageId,
            RevId = fromRaw.RevId,
            OldRevId = fromRaw.OldRevId,
            RcId = fromRaw.RcId,
            Timestamp = new DateTimeOffset(DateTime.ParseExact(fromRaw.Timestamp, iso8601Format, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal)).ToUnixTimeMilliseconds()
        }.HashSelf();
    }

    public WikipediaRecentChange HashSelf()
    {
        var hashString = $"{Type}|{PageId}|{RevId}|{OldRevId}|{RcId}|{Timestamp}";
        Hash = Crypto.HashSha256String(hashString);
        return this;
    }
}