using System.Text;
using ExtendedComponents;
using NewstalkerExtendedComponents;
using Newtonsoft.Json;

namespace NewstalkerPostgresGrader;

public struct DelegatedSummarizerSettings
{
    public string DelegatedSummarizerAddress { get; set; }
    public string DelegatedExtractorAddress { get; set; }
    public string DelegationApiKey { get; set; }
    public string DelegationAuthorizationSchema { get; set; }
    public uint HttpClientTimeout { get; set; }
}

public class DelegatedSummarizer : AbstractSummarizer
{
    private struct SummarizationResponse
    {
        [JsonProperty("summarized")]
        public string Summarized;
    }
    private struct ExtractionResponse
    {
        [JsonProperty("keywords")]
        public Dictionary<string, double> Keywords;
    }

    private struct SummarizationRequest
    {
        [JsonProperty("text")]
        public string Text;
        [JsonProperty("lang")]
        public string Lang;
    }
    private readonly LoggingServer _logger;
    private readonly ObjectPool<HttpClient> _clientPool;
    private readonly string _delegatedSummarizerAddress;
    private readonly string _delegatedExtractorAddress;
    
    private int GetHash() => GetHashCode();
    private readonly string _header;
    private string ThreadedHeader => $"{_header}:{Environment.CurrentManagedThreadId}";

    public DelegatedSummarizer(DelegatedSummarizerSettings settings, LoggingServer logger)
    {
        _header = $"DelegatedSummarizer:{GetHash()}";
        _clientPool = new SynchronousObjectPool<HttpClient>(() =>
        {
            var ret = new HttpClient();
            ret.Timeout = TimeSpan.FromSeconds(settings.HttpClientTimeout);
            if (!string.IsNullOrEmpty(settings.DelegationAuthorizationSchema) && !string.IsNullOrEmpty(settings.DelegationApiKey))
                ret.DefaultRequestHeaders.Add("Authorization",
                    $"{settings.DelegationAuthorizationSchema} {settings.DelegationApiKey}");
            return ret;
        });
        _logger = logger;
        _delegatedSummarizerAddress = settings.DelegatedSummarizerAddress;
        _delegatedExtractorAddress = settings.DelegatedExtractorAddress;
    }

    public override void Dispose()
    {
        _clientPool.Dispose();
        base.Dispose();
    }

    public override async Task<Dictionary<string, double>> ExtractTopicsAsync(
        AbstractNewsOutlet.ArticleScrapeResult originalArticle)
    {
        using var wrapped = _clientPool.Borrow();
        var httpClient = wrapped.GetInstance();
        var payload = new SummarizationRequest
        {
            Text = originalArticle.Text,
            Lang = originalArticle.Lang
        };
        var stringPayload = JsonConvert.SerializeObject(payload);
        var httpContent = new StringContent(stringPayload, Encoding.UTF8, "application/json");
        var response = await httpClient.PostAsync(_delegatedExtractorAddress, httpContent);
        var body = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"SummarizeArticleAsync: HTTP request failed with status code: {response.StatusCode}");
        }
        var marshalled = JsonConvert.DeserializeObject<ExtractionResponse>(body);
        if (marshalled.Keywords == null!)
        {
            _logger.Write(ThreadedHeader, "SummarizeArticleAsync: Failed to convert response body into C# object",
                LogSegment.LogSegmentType.Message, body);
            throw new JsonException();
        }

        return marshalled.Keywords.ToDictionary(o => o.Key.Trim().ToLower(), o => o.Value);
    }
    public override async Task<string> SummarizeArticleAsync(AbstractNewsOutlet.ArticleScrapeResult originalArticle)
    {
        using var wrapped = _clientPool.Borrow();
        var httpClient = wrapped.GetInstance();
        var payload = new SummarizationRequest
        {
            Text = originalArticle.Text,
            Lang = originalArticle.Lang
        };
        var stringPayload = JsonConvert.SerializeObject(payload);
        var httpContent = new StringContent(stringPayload, Encoding.UTF8, "application/json");
        var response = await httpClient.PostAsync(_delegatedSummarizerAddress, httpContent);
        var body = await response.Content.ReadAsStringAsync();
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"SummarizeArticleAsync: HTTP request failed with status code: {response.StatusCode}");
        }
        var marshalled = JsonConvert.DeserializeObject<SummarizationResponse>(body);
        return marshalled.Summarized ?? "";
    }
}