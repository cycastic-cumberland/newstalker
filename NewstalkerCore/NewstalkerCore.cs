using ExtendedComponents;
using ExtendedPostgresDriver;
using NewstalkerExtendedComponents;
using NewstalkerPostgresETL;
using NewstalkerPostgresGrader;
using PostgresDriver;

namespace NewstalkerCore;

internal class EnvironmentVariableException : Exception
{
    public EnvironmentVariableException(string msg) : base(msg) {}
    public EnvironmentVariableException() {}
}

public static class NewstalkerCore
{
    public enum MasterKeyAuthorizationNResult
    {
        Success,
        Mismatched,
        NotEnrolled
    }

    private static uint _databaseConnectionLimit = 16;
    private static PostgresConnectionSettings? _pgConnectionSettings;
    private static PostgresLogger? _pgLogger;
    private static string? _masterKey;
    private static DaemonManager? _manager;
    public static DaemonManager ActiveDaemon => _manager ??= new();
    public static PostgresConnectionSettings PostgresConnection => _pgConnectionSettings ??= GetConnectionSettings();
    public static uint DatabaseConnectionLimit => _databaseConnectionLimit;
    private static Exception EnvironmentValuePanic(string variable)
    {
        return new EnvironmentVariableException($"Environment variable {variable} is empty");
    }

    public static MasterKeyAuthorizationNResult AuthorizeMasterKey(string? key)
    {
        if (key == null) return MasterKeyAuthorizationNResult.Mismatched;
        var internedString = string.Intern(key);
        if (_masterKey == null) return MasterKeyAuthorizationNResult.NotEnrolled;
        return internedString == _masterKey ? MasterKeyAuthorizationNResult.Success : MasterKeyAuthorizationNResult.Mismatched;
    }

    private static PostgresConnectionSettings GetConnectionSettings()
    {
        _masterKey = Environment.GetEnvironmentVariable("NWA_MASTER_KEY"); 
        var host = Environment.GetEnvironmentVariable("PG_HOST");
        var port = Environment.GetEnvironmentVariable("PG_PORT");
        var dbName = Environment.GetEnvironmentVariable("PG_DB_NAME");
        var userName = Environment.GetEnvironmentVariable("PG_USERNAME");
        var password = Environment.GetEnvironmentVariable("PG_PASSWORD");
        
        if (string.IsNullOrEmpty(host)) throw EnvironmentValuePanic("PG_HOST");
        if (string.IsNullOrEmpty(port)) throw EnvironmentValuePanic("PG_PORT");
        if (string.IsNullOrEmpty(dbName)) throw EnvironmentValuePanic("PG_DB_NAME");
        if (string.IsNullOrEmpty(userName)) throw EnvironmentValuePanic("PG_USERNAME");
        if (string.IsNullOrEmpty(password)) password = "";

        var portAsInt = int.Parse(port);
        
        var sshPort = Environment.GetEnvironmentVariable("SSH_PORT");
        var sshUsername = Environment.GetEnvironmentVariable("SSH_USERNAME");
        var sshPassphrase = Environment.GetEnvironmentVariable("SSH_PASSPHRASE");
        var sshPrivateKeyPath = Environment.GetEnvironmentVariable("SSH_PRIVATE_KEY_PATH");

        var isValidSshPort = int.TryParse(sshPort, out var sshPortAsInt);
        var tunnelSettings = null as SshTunnelSettings;
        if (isValidSshPort && !string.IsNullOrEmpty(sshUsername)
                           && (!string.IsNullOrEmpty(sshPassphrase)
                               || string.IsNullOrEmpty(sshPrivateKeyPath)))
            tunnelSettings = new()
            {
                Username = sshUsername,
                Host = host,
                Port = sshPortAsInt,
                Password = sshPassphrase,
                PrivateKeyPath = sshPrivateKeyPath
            };
        return new()
        {
            Address = host,
            Port = portAsInt,
            DatabaseName = dbName,
            Password = password,
            Username = userName,
            Tunnel = tunnelSettings
        };
    }

    private static string? GetLogTableName()
    {
        var tableName = Environment.GetEnvironmentVariable("PG_LOG_TABLE");
        return tableName;
    }

    private static async Task<StandardConductorSettings> GetConductorSettings(
        PostgresConnectionSettings connectionSettings)
    {
        var gcInterval = Environment.GetEnvironmentVariable("NPC_GC_INTERVAL_HR");
        var harvestInterval = Environment.GetEnvironmentVariable("NPC_HARVEST_INTERVAL_HR");
        var scrapeLimit = Environment.GetEnvironmentVariable("NPC_SCRAPE_LIMIT");
        var summarizerAddr = Environment.GetEnvironmentVariable("NPC_SUMMARIZER_ADDRESS");
        var extractorAddr = Environment.GetEnvironmentVariable("NPC_EXTRACTOR_ADDRESS");
        var delegateApiKey = Environment.GetEnvironmentVariable("NPC_DELEGATE_API_KEY");
        var httpClientTimeout = Environment.GetEnvironmentVariable("NPC_DELEGATE_TIMEOUT");
        var tagsWeight = Environment.GetEnvironmentVariable("NPC_TAGS_WEIGHT");
        var pgChoker = Environment.GetEnvironmentVariable("NPC_MAX_CONCURRENT_DB_CONNECTION");
        var extractorChoker = Environment.GetEnvironmentVariable("NPC_MAX_CONCURRENT_EXTRACTOR_CONNECTION");
        var summarizerChoker = Environment.GetEnvironmentVariable("NPC_MAX_CONCURRENT_SUMMARIZER_CONNECTION");
        var syncMode = Environment.GetEnvironmentVariable("NPC_SYNC_MODE");

        if (string.IsNullOrEmpty(gcInterval)) throw EnvironmentValuePanic("NPC_GC_INTERVAL_HR");
        if (string.IsNullOrEmpty(harvestInterval)) throw EnvironmentValuePanic("NPC_HARVEST_INTERVAL_HR");
        if (string.IsNullOrEmpty(scrapeLimit)) scrapeLimit = "2147483647";
        if (string.IsNullOrEmpty(summarizerAddr)) throw EnvironmentValuePanic("NPC_SUMMARIZER_ADDRESS");
        if (string.IsNullOrEmpty(extractorAddr)) throw EnvironmentValuePanic("NPC_EXTRACTOR_ADDRESS");
        if (string.IsNullOrEmpty(httpClientTimeout)) httpClientTimeout = "100";
        if (string.IsNullOrEmpty(pgChoker)) throw EnvironmentValuePanic("NPC_MAX_CONCURRENT_DB_CONNECTION");
        if (string.IsNullOrEmpty(extractorChoker)) throw EnvironmentValuePanic("NPC_MAX_CONCURRENT_EXTRACTOR_CONNECTION");
        if (string.IsNullOrEmpty(summarizerChoker)) throw EnvironmentValuePanic("NPC_MAX_CONCURRENT_SUMMARIZER_CONNECTION");
        
        var outlets = new OutletSource();
        outlets["tuoitre"] = new TuoiTreOutlet();
        outlets["thanhnien"] = new ThanhNienOutlet();
        await using var db = new PostgresProvider(connectionSettings);
        Console.WriteLine("Initializing default outlets");
        await Initializer.InitializeOutlets(db);
        var outletArray = (await Initializer.QueryOutletInfo(db)).Select(r => r.url).ToArray();
        if (outletArray.Length == 0)
            throw new Exception("Could not initialize default outlets");
        return new()
        {
            ConnectionSettings = connectionSettings,
            GarbageCollectionInterval = TimeSpan.FromHours(uint.Parse(gcInterval)),
            HarvestInterval = TimeSpan.FromHours(uint.Parse(harvestInterval)),
            Outlets = outlets,
            DefaultQueryOption = new()
            {
                Limit = int.Parse(scrapeLimit),
                Type = AbstractNewsOutlet.FrontPageQueryOptions.QueryType.Articles
            },
            HarvesterSettings = new()
            {
                Outlets = outletArray
            },
            SummarizerSettings = new()
            {
                DelegatedSummarizerAddress = summarizerAddr,
                DelegatedExtractorAddress = extractorAddr,
                DelegationApiKey = delegateApiKey ?? "",
                DelegationAuthorizationSchema = "Bearer",
                HttpClientTimeout = uint.Parse(httpClientTimeout)
            },
            GraderSettings = new()
            {
                TagsWeight = string.IsNullOrEmpty(tagsWeight)
                    ? StandardConductor.StandardTagsWeight
                    : double.Parse(tagsWeight)
            },
            PostgresConnectionsLimit = uint.Parse(pgChoker),
            ExtractorConnectionsLimit = uint.Parse(extractorChoker),
            SummarizerConnectionsLimit = uint.Parse(summarizerChoker),
            UseDualSyncMode = syncMode == "DUAL",
        };
    }

    private static LoggingServerDelegate[] GetLoggers(PostgresConnectionSettings conn, string? pgLogTable)
    {
        if (pgLogTable == null) return new LoggingServerDelegate[] { new StdLoggingServerDelegate() };
        _pgLogger = new PostgresLogger(conn, pgLogTable);
        return new LoggingServerDelegate[] { new StdLoggingServerDelegate(), _pgLogger };
    }
    
    public static Task<IEnumerable<PostgresLogSegment>> GetLogs(DateTime timeFrom, DateTime timeTo, int typeFilter, uint limit)
    {
        return _pgLogger == null
            ? Task.FromResult((IEnumerable<PostgresLogSegment>)ArraySegment<PostgresLogSegment>.Empty)
            : _pgLogger.GetLogs(timeFrom, timeTo, typeFilter, limit);
    }

    public static Task<IEnumerable<PostgresLogSegment>> GetLogs(int typeFilter, uint limit)
    {
        return _pgLogger == null
            ? Task.FromResult((IEnumerable<PostgresLogSegment>)ArraySegment<PostgresLogSegment>.Empty)
            : _pgLogger.GetLogs(typeFilter, limit);
    }

    public static async Task Run()
    {
        Console.WriteLine("Enrolling database connection settings");
        _pgConnectionSettings = GetConnectionSettings();
        var logTable = GetLogTableName();
        Console.WriteLine(_masterKey == null
            ? "No master key enrolled, administrative activities restricted"
            : "Master key enrolled");
        Console.WriteLine("Database connection settings enrolled");
        Console.WriteLine($"Host: {_pgConnectionSettings.Address}");
        Console.WriteLine($"Port: {_pgConnectionSettings.Port}");
        Console.WriteLine($"Database name: {_pgConnectionSettings.DatabaseName}");
        Console.WriteLine($"Username: {_pgConnectionSettings.Username}");
        Console.WriteLine($"Password: ******");
        Console.WriteLine($"Use SSH tunnel: {_pgConnectionSettings.Tunnel != null}");
        Console.WriteLine($"Log table available: {logTable != null}");
        ActiveDaemon.Manage("tunnel", () =>
        {
            Console.WriteLine("SSH tunnel warehouse is now starting...");
            return new PostgresTunnelWarehouse(true);
        });
        Console.WriteLine();
        Console.WriteLine("Enrolling Conductor settings");
        var conductorSettings = await GetConductorSettings(_pgConnectionSettings);
        _databaseConnectionLimit = conductorSettings.PostgresConnectionsLimit;
        Console.WriteLine("Conductor settings enrolled");
        Console.WriteLine($"Garbage collection interval: {conductorSettings.GarbageCollectionInterval}");
        Console.WriteLine($"Harvest interval: {conductorSettings.HarvestInterval}");
        Console.WriteLine($"Scrape limit: {conductorSettings.DefaultQueryOption.Limit}");
        Console.WriteLine($"Summarizer address: {conductorSettings.SummarizerSettings.DelegatedSummarizerAddress}");
        Console.WriteLine($"Extractor address: {conductorSettings.SummarizerSettings.DelegatedExtractorAddress}");
        Console.WriteLine($"Delegate API key enrolled: " +
                          $"{!string.IsNullOrEmpty(conductorSettings.SummarizerSettings.DelegationApiKey)}");
        Console.WriteLine($"Delegate authorization schema: " +
                          $"{conductorSettings.SummarizerSettings.DelegationAuthorizationSchema}");
        Console.WriteLine($"Delegate timeout: {conductorSettings.SummarizerSettings.HttpClientTimeout} second(s)");
        Console.WriteLine($"Grader's tags weight: {conductorSettings.GraderSettings.TagsWeight}");
        Console.WriteLine($"Database connection limit: {conductorSettings.PostgresConnectionsLimit}");
        Console.WriteLine($"Extractor connection limit: {conductorSettings.ExtractorConnectionsLimit}");
        Console.WriteLine($"Summarizer connection limit: {conductorSettings.SummarizerConnectionsLimit}");
        Console.WriteLine($"Sync mode: {(conductorSettings.UseDualSyncMode ? "DUAL" : "SEQUENTIAL")}");
        ActiveDaemon.Manage("conductor", () =>
        {
            Console.WriteLine("Conductor daemon is now starting...");
            return new StandardConductor(conductorSettings,
                GetLoggers(_pgConnectionSettings, logTable));
        });
    }
}
