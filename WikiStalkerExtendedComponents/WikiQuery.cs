using Newtonsoft.Json;

namespace WikiStalkerExtendedComponents;

public class Continuation
{
    [JsonProperty("rccontinue")] public string RContinue = "";
    [JsonProperty("continue")] public string Continue = "";
}

public abstract class WikiQuery<TQuery>
{
    [JsonProperty("batchComplete")] public string BatchComplete = "";
    [JsonProperty("continue")] public Continuation Continuation = null!;
    [JsonProperty("query")] public TQuery Query = default!;
}