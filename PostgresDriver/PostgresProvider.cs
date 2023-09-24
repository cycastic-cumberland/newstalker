using System.Data;
using System.Text;
using Dapper;
using ExtendedComponents;
using Npgsql;

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

public class PostgresConnectionSettings
{
    public string Address { get; set; } = "localhost";
    public int Port { get; set; } = 5432;
    public string DatabaseName { get; set; } = "";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";

    public override string ToString()
    {
        StringBuilder builder = new StringBuilder();
        builder.Append($"Server={Address};Port={Port};")
               .Append($"Database={DatabaseName};Username={Username};");
        if (Password.Length != 0)
            builder.Append($"Password='{Password}';");
        return builder.ToString();
    }
}

public class PostgresProvider : IDisposable
{
    private NpgsqlConnection? _conn;
    private PostgresTransaction? _currentTransaction;
    private PostgresConnectionSettings? _lastConnection;
    // public NpgsqlConnection RawConnection => conn;

    public PostgresProvider()
    {
        
    }

    public PostgresProvider(PostgresConnectionSettings settings)
    {
        Connect(settings).Wait();
    }

    public async Task Connect(PostgresConnectionSettings settings)
    {
        if (_conn != null) await _conn.DisposeAsync();
        _lastConnection = settings;
        for (var i = 0; i < ProjectSettings.Instance.Get(SettingsCatalog.CoreDbMaxReconnectionAttempt, 5); i++)
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
        if (_conn != null) await _conn.DisposeAsync();
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
    public T Write<T>(Func<ITransaction, T> action)
    {
        lock (this)
        {
            PostgresTransaction transaction;
            bool isTopLevel;
            // No running transaction
            if (_currentTransaction == null)
            {
                transaction = CreateTransaction();
                isTopLevel = true;
            }
            else
            {
                transaction = _currentTransaction;
                isTopLevel = false;
            }
            
            try
            {
                if (isTopLevel) transaction.Start();
                var re = action(transaction);
                if (!isTopLevel) return re;
                transaction.Commit();
                transaction.Dispose();
                _currentTransaction = null;
                return re;
            }
            catch (Exception)
            {
                // If there's an exception, rollback top level transaction, and dispose
                _currentTransaction?.RollBack();
                _currentTransaction?.Dispose();
                _currentTransaction = null;
                throw;
            }
        }
    }
    public void Write(Action<ITransaction> action)
    {
        lock (this)
        {
            PostgresTransaction transaction;
            bool isTopLevel;
            // No running transaction
            if (_currentTransaction == null)
            {
                transaction = CreateTransaction();
                isTopLevel = true;
            }
            else
            {
                transaction = _currentTransaction;
                isTopLevel = false;
            }
            
            try
            {
                if (isTopLevel) transaction.Start();
                action(transaction);
                if (!isTopLevel) return;
                transaction.Commit();
                transaction.Dispose();
                _currentTransaction = null;
            }
            catch (Exception)
            {
                // If there's an exception, rollback top level transaction, and dispose
                _currentTransaction?.RollBack();
                _currentTransaction?.Dispose();
                _currentTransaction = null;
                throw;
            }
        }
    }
}