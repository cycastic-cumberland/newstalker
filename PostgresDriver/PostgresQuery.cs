using ExtendedComponents;

namespace PostgresDriver;

public class PostgresQueryFactory : IDisposable
{
    private readonly ObjectPool<PostgresQuery> _objectPool;

    public PostgresQueryFactory(PostgresConnectionSettings conn)
    {
        _objectPool = new SynchronousObjectPool<PostgresQuery>(() => new(conn));
    }

    public ObjectPool<PostgresQuery>.IObjectPoolInstance NewQueryInstance()
        => _objectPool.Borrow();

    public void Dispose()
    {
        _objectPool.Dispose();
    }
}

public class PostgresQuery : IDisposable, IAsyncDisposable
{
    private readonly PostgresProvider _provider;
    public PostgresProvider Db => _provider;
    public PostgresQuery(PostgresConnectionSettings settings)
    {
        _provider = new(settings);
    }

    public void Dispose()
    {
        _provider.Disconnect().Wait();
    }

    public async ValueTask DisposeAsync()
    {
        await _provider.Disconnect();
    }
}
