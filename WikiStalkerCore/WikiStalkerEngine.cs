using System.Reflection;
using ExtendedComponents;
using Newtonsoft.Json;
using PostgresDriver;
using PostgresETL;
using PostgresTopicRanks;

namespace WikiStalkerCore;

public static class WikiStalkerEngine
{
    private const string PgConnCfg = "pg_config.json";
    private const string PgEtlCfg = "etl_config.json";
    private const string PgRankerCfg = "ranker_config.json";
    
    private static readonly string ExecutablePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!;
    
    private static DaemonManager? _manager;
    private static Dictionary<string, string> _cmdArgs = null!;

    public static DaemonManager Manager => _manager!;
    public static object? Lifetime;
    
    private static T GetSettings<T>(string configPath)
    {
        string content = File.ReadAllText(configPath);
        return JsonConvert.DeserializeObject<T>(content) ?? throw new JsonException();
    }

    public static PostgresConnectionSettings GetPgConnectionSettings()
        => GetSettings<PostgresConnectionSettings>(_cmdArgs["pg-connection-settings"]);

    public static PostgresHarvesterSettings GetPgEtlSettings()
        => GetSettings<PostgresHarvesterSettings>(_cmdArgs["etl-settings"]);
    
    public static PostgresTopicRankerSettings GetPgRankerSettings()
        => GetSettings<PostgresTopicRankerSettings>(_cmdArgs["ranker-settings"]);

    private static void CmdArgumentsParse(string[] args)
    {
        _cmdArgs = new CmdArgumentsParser(args).Parse();
        _cmdArgs.TryAdd("pg-connection-settings", Path.Join(ExecutablePath, PgConnCfg));
        _cmdArgs.TryAdd("etl-settings", Path.Join(ExecutablePath, PgEtlCfg));
        _cmdArgs.TryAdd("ranker-settings", Path.Join(ExecutablePath, PgRankerCfg));
        _cmdArgs.TryAdd("etl-enabled", "false");
        _cmdArgs.TryAdd("ranker-enabled", "false");
    }
    
    public static void Setup(string[] args, object lifetime)
    {
        Lifetime = lifetime;
        CmdArgumentsParse(args);
        _manager = new();
    }

    public static string GetSettings(string name) => _cmdArgs[name];
    public static void SetSettings(string name, string value) => _cmdArgs[name] = value;
}