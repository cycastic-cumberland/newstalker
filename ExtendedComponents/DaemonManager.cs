namespace ExtendedComponents;

public class DaemonManager
{
    private readonly Dictionary<string, IDaemon> _daemons = new();

    public bool Manage(string name, Func<IDaemon> daemonSpawner)
    {
        lock (this)
        {
            if (_daemons.ContainsKey(name)) return false;
            _daemons[name] = daemonSpawner();
            return true;
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

    public bool DaemonEnabled(string name)
    {
        lock (this)
        {
            return _daemons.ContainsKey(name);
        }
    }

    private void CloseDaemon(string name, IDaemon daemon)
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

    public bool CloseDaemon(string name)
    {
        lock (this)
        {
            _daemons.TryGetValue(name, out var daemon);
            if (daemon == null) return false;
            CloseDaemon(name, daemon);
            _daemons.Remove(name);
            return true;
        }
    }

    public void ManualCleanUp()
    {
        lock (this)
        {
            foreach (var (name, daemon) in _daemons)
            {
                CloseDaemon(name, daemon);
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