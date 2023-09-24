namespace ExtendedComponents;

public class DaemonManager
{
    private readonly Dictionary<string, IDaemon> _daemons = new();

    public bool Manage(string name, IDaemon daemon)
    {
        lock (this)
        {
            return _daemons.TryAdd(name, daemon);
        }
    }

    public IDaemon? Get(string name)
    {
        lock (this)
        {
            _daemons.TryGetValue(name, out var daemon);
            return daemon;
        }
    }

    public void ManualCleanUp()
    {
        lock (this)
        {
            foreach (var (name, daemon) in _daemons)
            {
                try
                {
                    daemon.CloseDaemon();
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"Exception while closing daemon '{name}', daemon might not have been closed properly.\n" +
                                            $"{e}");
                }
            }
            _daemons.Clear();
        }
    }

    public DaemonManager()
    {
        Console.CancelKeyPress += delegate(object? _, ConsoleCancelEventArgs args)
        {
            // args.Cancel = true;
            ManualCleanUp();
            Console.WriteLine("All daemons closed");
        };
    }
}