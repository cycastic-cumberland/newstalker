using ExtendedComponents;
using Newtonsoft.Json;
using PostgresDriver;

namespace PostgresETL;

public class PostgresLogger : LoggingServerDelegate
{
    private readonly PostgresProvider _provider;
    private readonly string _tableName;

    public PostgresLogger(PostgresConnectionSettings settings, string tableName)
    {
        _provider = new(settings);
        _tableName = tableName;
    }

    public override void Dispose()
    {
        base.Dispose();
        _provider.Disconnect().Wait();
    }

    private async Task WriteAsync(LogSegment log)
    {
        var serialized = log.Metadata == null ? "" : JsonConvert.SerializeObject(log.Metadata);
        await _provider.TryExecute($"INSERT INTO {_tableName} (timestamp, header, message, log_type, metadata) " +
                                   "VALUES (@time, @header, @msg, @type, @meta);",
            new {
                time = log.Timestamp,
                header = log.Header,
                msg = log.Message,
                type = (int)log.LogType,
                meta = serialized });
    }

    public override void Write(LogSegment log)
    {
#pragma warning disable CS4014
        WriteAsync(log); //.Wait();
#pragma warning restore CS4014
    }
}