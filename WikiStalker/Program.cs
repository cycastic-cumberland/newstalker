using System.Reflection;
using System.Text;
using ExtendedComponents;
using Newtonsoft.Json;
using PostgresDriver;
using PostgresETL;
using PostgresTopicRanks;

// Recent changes: https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&rcnamespace=0&rclimit=10&format=json
// More on recent changes: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Brecentchanges
// rcstart: The nearest timestamp (just take the current unix time)
// rcend: The furthest timestamp


// References (by title): https://en.wikipedia.org/w/api.php?action=parse&page=2023%20Rugby%20World%20Cup&prop=links&format=json
// References (by pageid): https://en.wikipedia.org/w/api.php?action=parse&pageid=74884249&prop=links&format=json
// Page plain text (by pageids): https://en.wikipedia.org/w/api.php?action=query&pageids=8387&prop=extracts&format=json&explaintext=true
// Page plain text (by titles): https://en.wikipedia.org/w/api.php?action=query&titles=Draught%20beer&prop=extracts&format=json&explaintext=true

namespace WikiStalker;

internal class CmdArgumentParser
{
    private readonly Dictionary<string, string> _cmdArgs = new();
    private readonly string[] _arguments;
    private StringBuilder? _combinedString;
    private string _lastKey = "";

    public CmdArgumentParser(string[] args)
    {
        _arguments = args;
    }

    private void ParseString(string argument)
    {
        if (string.IsNullOrWhiteSpace(argument)) return;

        if (argument.StartsWith("--"))
        {
            _lastKey = argument[2..];
            return;
        }
            
        if (argument.StartsWith("\""))
        {
            if (_combinedString != null)
            {
                var nextString = argument[1..];
                ParseString(" \"");
                ParseString(nextString);
            }
            else
            {
                _combinedString = new();
                _combinedString.Append(argument[1..]);
            }
            return;
        }

        if (argument.EndsWith("\""))
        {
            if (_combinedString == null)
            {
                throw new Exception("No string to close");
            }

            var success = _cmdArgs.TryAdd(_lastKey, _combinedString.ToString()[..^1]);
            if (!success)
                throw new Exception($"Repeating command line argument: {_lastKey}");
            _combinedString = null!;
        }
        if (_combinedString != null)
        {
            _combinedString.Append(argument);
            return;
        }

        {
            var success = _cmdArgs.TryAdd(_lastKey, argument);
            if (!success)
                throw new Exception($"Repeating command line argument: {_lastKey}");
        }
    }
    
    public Dictionary<string, string> Parse()
    {
        foreach (var argument in _arguments)
        {
            ParseString(argument);
        }

        if (_combinedString != null)
            throw new Exception("Unescaped string at the end of command line arguments");
        return _cmdArgs;
    }
}

public static class MainClass
{
    private const string PgConnCfg = "pg_config.json";
    private const string PgEtlCfg = "etl_config.json";
    private const string PgRankerCfg = "ranker_config.json";

    private static Dictionary<string, string> CmdArgs = null!;
    private static bool _isCanceled;
    private static string _executablePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!;

    private static T GetSettings<T>(string configPath)
    {
        string content = File.ReadAllText(configPath);
        return JsonConvert.DeserializeObject<T>(content) ?? throw new JsonException();
    }

    private static PostgresConnectionSettings GetPgConnectionSettings()
        => GetSettings<PostgresConnectionSettings>(CmdArgs["pg-connection-settings"]);

    private static PostgresHarvesterSettings GetPgEtlSettings()
        => GetSettings<PostgresHarvesterSettings>(CmdArgs["etl-settings"]);
    
    private static PostgresTopicRankerSettings GetPgRankerSettings()
        => GetSettings<PostgresTopicRankerSettings>(CmdArgs["ranker-settings"]);

    private static void CmdArgumentsParse(string[] args)
    {
        CmdArgs = new CmdArgumentParser(args).Parse();
        CmdArgs.TryAdd("pg-connection-settings", Path.Join(_executablePath, PgConnCfg));
        CmdArgs.TryAdd("etl-settings", Path.Join(_executablePath, PgEtlCfg));
        CmdArgs.TryAdd("ranker-settings", Path.Join(_executablePath, PgRankerCfg));
    }
    
    public static void Main(string[] args)
    {
        CmdArgumentsParse(args);
        // Enroll configs
        var connSettings = GetPgConnectionSettings();
        var etlSettings = GetPgEtlSettings();
        var rankerSettings = GetPgRankerSettings();
        
        var daemonManager = new DaemonManager();
        var pgLogger = new PostgresLogger(connSettings, "stalker_logs");
        var stdLogger = new StdLoggingServerDelegate();
        daemonManager.Manage("etl", new PostgresHarvester(etlSettings,
            new LoggingServerDelegate[]{ pgLogger, stdLogger }));
        daemonManager.Manage("ranker", new PostgresTopicRanker(rankerSettings,
            new LoggingServerDelegate[] { pgLogger, stdLogger }));
        Console.CancelKeyPress += delegate(object? _, ConsoleCancelEventArgs arg)
        {
            _isCanceled = true;
            arg.Cancel = true;
        };
        while (true)
        {
            if (_isCanceled) break;
            Thread.Sleep(100);
        }
    }
}