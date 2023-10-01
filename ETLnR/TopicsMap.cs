using Newtonsoft.Json;

namespace ETLnR;

public readonly struct TopicsMap
{
    public readonly Dictionary<string, float> Map;

    public TopicsMap(Dictionary<string, float> map)
    {
        Map = map;
    }
}

public class TopicPair
{
    [JsonProperty("keyword")] public string Keyword = "";
    [JsonProperty("popularity")] public float Popularity;
}

public class JsonTopicsMap
{
    [JsonProperty("map")] public TopicPair[] Map = null!;

    public TopicsMap ToStruct()
    {
        return new TopicsMap(Map.ToDictionary(pair => pair.Keyword, pair => pair.Popularity));
    }
}