namespace ExtendedComponents;

public static class SettingsCatalog
{
    public const string CoreApiKey = "core/api_key";
    public const string CoreDbEndpoint = "core/db/endpoint";
    public const string CoreDbPort = "core/db/port";
    public const string CoreDbName = "core/db/name";
    public const string CoreDbUsername = "core/db/username";
    public const string CoreDbPassword = "core/db/password";
    public const string CoreDbMaxReconnectionAttempt = "core/db/reconnection_attempt";
    public const string HarvesterIterationIntervalHour = "harvester/iteration_interval_hr";
    public const string HarvesterTimeToLiveHour = "harvester/ttl_hr";

    public static readonly string[] IntValues = { CoreDbPort };
    public static string[] FloatValues = { HarvesterTimeToLiveHour, HarvesterIterationIntervalHour };
}

public class ProjectSettings
{
    private readonly IDictionary<string, object> _configs = new Dictionary<string, object>(30);
    private static ProjectSettings? _instance = null;

    public static ProjectSettings Instance => _instance ??= new ProjectSettings();

    public void Set(string key, object? value)
    {
        _configs[key] = value ?? throw new NullReferenceException();
    }

    public T Get<T>(string key, T defaultValue)
    {
        try
        {
            var raw = _configs[key];
            return raw is T raw1 ? raw1 : defaultValue;
        }
        catch (KeyNotFoundException)
        {
            return defaultValue;
        }
    }
    public T HardGet<T>(string key)
    {
        var raw = _configs[key];
        return (T)raw;
    }

    public object this[string key]
    {
        set => Set(key, value);
    }
}