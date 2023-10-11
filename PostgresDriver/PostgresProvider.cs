using System.Data;
using System.Text;
using Dapper;
using ExtendedComponents;
using Npgsql;
using Renci.SshNet;
using Renci.SshNet.Common;

namespace PostgresDriver;

public class PostgresTransaction : ITransaction
{
    private readonly NpgsqlTransaction _realTransaction;
    
    public PostgresTransaction(NpgsqlTransaction trans)
    {
        _realTransaction = trans;
    }
    public void Dispose()
    {
        _realTransaction.Dispose();
    }
    public void Start()
    {
        
    }
    public void RollBack()
    {
        _realTransaction.Rollback();
    }

    public void Commit()
    {
        _realTransaction.Commit();
    }

    public IDbTransaction GetRawTransaction() => _realTransaction;
}

public class SshTunnelSettings
{
    public string Username { get; set; } = "";
    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 22;
    public string? Password { get; set; }
    public string? PrivateKeyPath { get; set; }
    public string? PrivateKeyContent { get; set; }

    private static PrivateKeyFile ExtractPrivateKeyByPath(string privateKeyPath, string? passphrase)
    {
        if (passphrase != null) return new(privateKeyPath, passphrase);
        return new(privateKeyPath);
    }
    private static PrivateKeyFile ExtractPrivateKeyByContent(string privateKeyContent, string? passphrase)
    {
        var stream = new MemoryStream(Encoding.UTF8.GetBytes(privateKeyContent));
        if (passphrase != null) return new(stream, passphrase);
        return new(stream);
    }
    public SshClient CreateClient()
    {
        if (Password == null && PrivateKeyPath == null && PrivateKeyContent == null)
            throw new NullReferenceException("Either password or private key path must be provided");
        if (PrivateKeyContent != null)
            return new(Host, Port, Username, ExtractPrivateKeyByContent(PrivateKeyContent, Password));
        if (PrivateKeyPath != null)
            return new(Host, Port, Username, ExtractPrivateKeyByPath(PrivateKeyPath, Password));
        return new(Host, Port, Username, Password);
    }
}

public class PostgresConnectionSettings
{
    public string Address { get; set; } = "localhost";
    public int Port { get; set; } = 5432;
    public string DatabaseName { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public SshTunnelSettings? Tunnel { get; set; }

    public override string ToString()
    {
        StringBuilder builder = new StringBuilder();
        builder.Append($"Server={Address};");
        if (Port != 0) builder.Append($"Port={Port};");
        builder.Append($"Database={DatabaseName};Username={Username};");
        if (Password.Length != 0)
            builder.Append($"Password='{Password}';");
        return builder.ToString();
    }
}

public class PostgresTunnelWarehouse : IDisposable, IDaemon
{
    private static PostgresTunnelWarehouse? _instance;
    public static PostgresTunnelWarehouse Instance => _instance!;
    private bool _isSingleton;

    public PostgresTunnelWarehouse(bool isSingleton = false)
    {
        if (isSingleton)
        {
            if (_instance != null) _instance._isSingleton = false;
            _instance = this;
        }
        _isSingleton = isSingleton;
    }
    
    private struct ForwardedConnection : IDisposable
    {
        public PostgresConnectionSettings Connection;
        public SshClient Client;
        public ForwardedPortLocal Port;

        public void Dispose()
        {
            Port.Stop();
            Client.Dispose();
            Port.Dispose();
        }
    }
    private readonly Dictionary<string, ForwardedConnection> _tunnels = new();

    public PostgresConnectionSettings AllocateTunnel(SshTunnelSettings settings, PostgresConnectionSettings conn)
    {
        var rep = conn.ToString();
        lock (this)
        {
            if (_tunnels.TryGetValue(rep, out var ret)) return ret.Connection;
            var sshClient = settings.CreateClient();
            sshClient.Connect();
            if (!sshClient.IsConnected) throw new SshConnectionException("Failed to open an SSH tunnel");
            var fwdPort = new ForwardedPortLocal("127.0.0.1", settings.Host, (uint)conn.Port);
            sshClient.AddForwardedPort(fwdPort);
            fwdPort.Start();
            PostgresConnectionSettings newConn = new()
            {
                Address = fwdPort.BoundHost,
                DatabaseName = conn.DatabaseName,
                Password = conn.Password,
                Port = (int)fwdPort.BoundPort,
                Tunnel = settings,
                Username = conn.Username
            };
            _tunnels[rep] = new()
            {
                Connection = newConn,
                Client = sshClient,
                Port = fwdPort
            };
            return newConn;
        }
    }

    public void ClearProxyPool()
    {
        lock (this)
        {
            foreach (var (_, conn) in _tunnels)
            {
                conn.Dispose();
            }
        }
    }
    public void Dispose()
    {
        ClearProxyPool();
        lock (this)
        {
            if (_isSingleton) _instance = null!;
        }
    }

    public void CloseDaemon()
    {
        Dispose();
    }
}

public class PostgresProvider : IDisposable, IAsyncDisposable
{
    private class InnerTransaction : ITransaction
    {
        private readonly PostgresProvider _provider;
        private readonly bool _isTopLevel;

        public InnerTransaction(PostgresProvider provider, bool isTopLevel)
        {
            _provider = provider;
            _isTopLevel = isTopLevel;
        }

        public void Dispose()
        {
            if (!_isTopLevel) return;
            lock (_provider)
            {
                _provider._currentTransaction?.Dispose();
                _provider._currentTransaction = null;
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        public void Start()
        {
            if (!_isTopLevel) return;
            lock (_provider)
            {
                _provider._currentTransaction?.Start();
            }
        }

        public void RollBack()
        {
            lock (_provider)
            {
                _provider._currentTransaction?.RollBack();
                _provider._currentTransaction = null;
            }
        }

        public void Commit()
        {
            if (!_isTopLevel) return;
            lock (_provider)
            {
                _provider._currentTransaction?.Commit();
                _provider._currentTransaction = null;
            }
        }

        public IDbTransaction? GetRawTransaction()
        {
            lock (_provider)
            {
                return _provider._currentTransaction?.GetRawTransaction();
            }
        }
    }
    private readonly PostgresTunnelWarehouse? _warehouse;
    private NpgsqlConnection? _conn;
    private PostgresTransaction? _currentTransaction;
    private PostgresConnectionSettings? _lastConnection;
    private PostgresTunnelWarehouse Warehouse => _warehouse ?? PostgresTunnelWarehouse.Instance;

    public PostgresProvider(PostgresTunnelWarehouse? warehouse = null)
    {
        _warehouse = warehouse;
    }

    public PostgresProvider(PostgresConnectionSettings settings, PostgresTunnelWarehouse? warehouse = null)
    {
        _warehouse = warehouse;
        Connect(settings).Wait();
    }

    public async Task Connect(PostgresConnectionSettings settings)
    {
        _lastConnection = settings;
        for (var i = 0; i < ProjectSettings.Instance.Get(SettingsCatalog.CoreDbMaxReconnectionAttempt, 2); i++)
        {
            try
            {
                await Reconnect();
                return;
            }
            catch (NpgsqlException ex)
            {
                if (ex.ErrorCode != -2147467259)
                    throw;
                await Task.Delay(100);
            }
        }
        throw new ConnectionTimeoutException();
    }

    public async Task Reconnect()
    {
        if (_lastConnection == null) return;
        await Disconnect();
        if (_lastConnection.Tunnel != null)
        {
            _lastConnection = Warehouse.AllocateTunnel(_lastConnection.Tunnel, _lastConnection);
        }
        _conn = new NpgsqlConnection(_lastConnection.ToString());
        await _conn.OpenAsync();
    }

    public bool IsConnected()
    {
        if (_conn == null) return false;
        return _conn.State == ConnectionState.Open;
    }

    public async Task<IEnumerable<T>> MappedQuery<T>(string sql, object? param = null, IDbTransaction? transaction = null)
    {
        return await _conn.QueryAsync<T>(sql, param: param, transaction: transaction);
    }
    
    public async Task<int> Execute(string sql, object? param = null, IDbTransaction? transaction = null)
    {
        return await _conn.ExecuteAsync(sql, param: param, transaction: transaction);
    }
    public async Task<IEnumerable<T>> TryMappedQuery<T>(string sql, object? param = null, IDbTransaction? transaction = null)
    {
        for (var i = 0; i < ProjectSettings.Instance.Get(SettingsCatalog.CoreDbMaxReconnectionAttempt, 5); i++)
        {
            try
            {
                return await MappedQuery<T>(sql, param, transaction);
            }
            catch (NpgsqlException ex)
            {
                if (ex.ErrorCode != -2147467259)
                {
                    await Task.Delay(100);
                    try
                    {
                        await Reconnect();
                    }
                    catch (Exception)
                    {
                        // Ignored
                    }
                }
                else throw;
            }
        }
        throw new ConnectionTimeoutException();
    }

    public async Task<int> TryExecute(string sql, object? param = null, IDbTransaction? transaction = null)
    {
        for (var i = 0; i < ProjectSettings.Instance.Get(SettingsCatalog.CoreDbMaxReconnectionAttempt, 5); i++)
        {
            try
            {
                return await Execute(sql, param, transaction);
            }
            catch (NpgsqlException ex)
            {
                if (ex.ErrorCode != -2147467259)
                {
                    await Task.Delay(100);
                    try
                    {
                        await Reconnect();
                    }
                    catch (Exception)
                    {
                        // Ignored
                    }
                }
                else throw;
            }
        }

        throw new ConnectionTimeoutException();
    }

    public PostgresTransaction CreateTransaction()
    {
        if (!IsConnected() || _conn == null) throw new DbConnectionFailedException();
        return new PostgresTransaction(_conn.BeginTransaction());
    }
    public async Task Disconnect()
    {
        if (_conn != null) await _conn.DisposeAsync();
    }
    
    public void Dispose()
    {
        Disconnect().Wait();
    }
    public async ValueTask DisposeAsync()
    {
        await Disconnect();
    }
    private InnerTransaction SecureTransaction()
    {
        lock (this)
        {
            if (_currentTransaction != null) return new InnerTransaction(this, false);
            _currentTransaction = CreateTransaction();
            return new InnerTransaction(this, true);
        }
    }
    public T OpenTransaction<T>(Func<T> func)
    {
        using var transaction = SecureTransaction();
        var ret = func();
        transaction.Commit();
        return ret;
    }
    public T OpenTransaction<T>(Func<ITransaction, T> func)
    {
        using var transaction = SecureTransaction();
        var ret = func(transaction);
        transaction.Commit();
        return ret;
    }
    public async Task<T> OpenTransaction<T>(Func<Task<T>> func)
    {
        await using var transaction = SecureTransaction();
        var ret = await func();
        transaction.Commit();
        return ret;
    }
    public async Task<T> OpenTransaction<T>(Func<ITransaction, Task<T>> func)
    {
        await using var transaction = SecureTransaction();
        var ret = await func(transaction);
        transaction.Commit();
        return ret;
    }
    public void OpenTransaction(Action func)
    {
        using var transaction = SecureTransaction();
        func();
        transaction.Commit();
    }
    public void OpenTransaction(Action<ITransaction> func)
    {
        using var transaction = SecureTransaction();
        func(transaction);
        transaction.Commit();
    }
    public async Task OpenTransaction(Func<Task> func)
    {
        await using var transaction = SecureTransaction();
        await func();
        transaction.Commit();
    }
    public async Task OpenTransaction(Func<ITransaction, Task> func)
    {
        await using var transaction = SecureTransaction();
        await func(transaction);
        transaction.Commit();
    }
}