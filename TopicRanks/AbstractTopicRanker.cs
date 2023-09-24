using ExtendedComponents;

namespace TopicRanks;

public struct TopicRanksSettings
{
    public (long timeFrom, long timeTo) TimeWindow;

    public override string ToString()
    {
        return $"({TimeWindow.timeFrom}, {TimeWindow.timeTo})";
    }
}

public abstract class AbstractTopicRanker : AbstractDaemon
{
    protected abstract TopicsMap RankTopicsInternal(TopicRanksSettings settings);

    public async Task<TopicsMap> RankTopics(TopicRanksSettings settings)
    {
        TopicsMap ret = new();
        await Dispatch(() =>
        {
            ret = RankTopicsInternal(settings);
        });
        return ret;
    }
}
