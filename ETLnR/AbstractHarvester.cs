using ExtendedComponents;

namespace ETLnR;

public abstract class AbstractHarvester : AbstractDaemon
{
    protected readonly string ApiKey;
    protected AbstractHarvester(string apiKey, uint loopInterval = 10)
        : base(loopInterval)
    {
        ApiKey = apiKey;
    }
    
    protected abstract void RunGarbageCollectionInternal();
    protected abstract void RunHarvestInternal();
    protected abstract void UpdateSettingsInternal();
    public Task RunGarbageCollection() => Dispatch(RunGarbageCollectionInternal);
    public Task RunHarvest() => Dispatch(RunHarvestInternal);
    public Task UpdateSettings() => Dispatch(UpdateSettingsInternal);
    public Task CancelServerLoop() => Dispatch(CancelServerLoopMaster);
}