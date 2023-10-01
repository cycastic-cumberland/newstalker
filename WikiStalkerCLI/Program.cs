using System.Diagnostics;
using ExtendedComponents;
using ExtendedPostgresDriver;
using WikiStalkerPostgresETL;
using WikiStalkerPostgresTopicRanks;

// Recent changes: https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&rcnamespace=0&rclimit=10&format=json
// More on recent changes: https://en.wikipedia.org/w/api.php?action=help&modules=query%2Brecentchanges
// rcstart: The nearest timestamp (just take the current unix time)
// rcend: The furthest timestamp


// References (by title): https://en.wikipedia.org/w/api.php?action=parse&page=2023%20Rugby%20World%20Cup&prop=links&format=json
// References (by pageid): https://en.wikipedia.org/w/api.php?action=parse&pageid=74884249&prop=links&format=json
// Page plain text (by pageids): https://en.wikipedia.org/w/api.php?action=query&pageids=8387&prop=extracts&format=json&explaintext=true
// Page plain text (by titles): https://en.wikipedia.org/w/api.php?action=query&titles=Draught%20beer&prop=extracts&format=json&explaintext=true

namespace WikiStalkerCLI;

public static class MainClass
{
    private static bool _isCanceled;
    public static void Main(string[] args)
    {
        int nProcessId = Process.GetCurrentProcess().Id;
        Console.WriteLine($"WikiStalker's process ID: {nProcessId}");
        WikiStalkerEngine.Setup(args, null!);
        // Enroll configs
        var connSettings = WikiStalkerEngine.GetPgConnectionSettings();
        var etlSettings = WikiStalkerEngine.GetPgEtlSettings();
        var rankerSettings = WikiStalkerEngine.GetPgRankerSettings();
        
        int serviceEnabled = 0;
        if (bool.Parse(WikiStalkerEngine.GetSettings("etl-enabled")))
        {
            Console.WriteLine("Enabling ETL");
            Task.Run(() =>
            {
                return WikiStalkerEngine.Manager.Manage("etl", () => new PostgresHarvester(etlSettings,
                    new LoggingServerDelegate[]
                    {
                        new PostgresLogger(connSettings, "stalker_logs"),
                        new StdLoggingServerDelegate()
                    }));
            });
            serviceEnabled++;
        }

        if (bool.Parse(WikiStalkerEngine.GetSettings("ranker-enabled")))
        {
            Console.WriteLine("Enabling Ranker");
            Task.Run(() =>
            {
                WikiStalkerEngine.Manager.Manage("ranker", () => new PostgresTopicRanker(rankerSettings,
                    new LoggingServerDelegate[]
                    {
                        new PostgresLogger(connSettings, "stalker_logs"),
                        new StdLoggingServerDelegate()
                    }));
            });
            serviceEnabled++;
        }

        if (serviceEnabled == 0)
        {
            Console.Error.WriteLine("No service enabled, exiting...");
            Environment.Exit(0);
        }
        Console.WriteLine($"{serviceEnabled} service(s) have been enabled.");
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
        Environment.Exit(0);
    }
}