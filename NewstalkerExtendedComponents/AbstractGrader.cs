namespace NewstalkerExtendedComponents;

public abstract class AbstractGrader : IDisposable
{
    public enum GradeType
    {
        Tags,
        Keywords,
        Articles
    }
    public struct GraderSettings
    {
        public DateTime? TimeEnd;
        public TimeSpan PopularityWindow;
        public string[] OutletSelections;
        public string TargetSelection;
        // public float NormalizedScale;
        public GradeType GradingTarget;
    }
    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }
    public abstract Task<Dictionary<string, double>> GradeRelevancyAsync(GraderSettings settings);
    public abstract Task<int> QueryAmountAsync(GraderSettings settings);
    
    public Dictionary<string, double> GradeArticles(GraderSettings settings)
    {
        var task = GradeRelevancyAsync(settings);
        task.Wait();
        return task.Result;
    }
    public int QueryAmount(GraderSettings settings)
    {
        var task = QueryAmountAsync(settings);
        task.Wait();
        return task.Result;
    }
}