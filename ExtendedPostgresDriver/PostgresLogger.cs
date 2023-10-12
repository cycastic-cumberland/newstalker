using ExtendedComponents;
using Newtonsoft.Json;
using PostgresDriver;

namespace ExtendedPostgresDriver;

public struct PostgresLogSegment
{
    public long Timestamp;
    public string Header;
    public string Message;
    public int Type;
    public string Meta;

    public LogSegment Convert()
    {
        return new()
        {
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(Timestamp).DateTime.ToUniversalTime(),
            Header = Header,
            Message = Message,
            LogType = (LogSegment.LogSegmentType)Type,
            Metadata = Meta
        };
    }
}

internal static class DateTimeUnix
{
    public static long ToUnixMilliseconds(this DateTime self)
    {
        return (long)(self - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
    }
}

public class PostgresLogger : LoggingServerDelegate
{
    private readonly PostgresProvider _provider;
    private readonly string _tableName;

    public PostgresLogger(PostgresConnectionSettings settings, string tableName)
    {
        _provider = new();
        try
        {
            _provider.Connect(settings).Wait();
        }
        catch (Exception)
        {
            // Ignored
        }
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
                time = new DateTimeOffset(log.Timestamp.ToUniversalTime()).ToUnixTimeMilliseconds(),
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

    public async Task<IEnumerable<PostgresLogSegment>> GetLogs(DateTime timeFrom, DateTime timeTo, int typeFilter, uint limit)
    {
        return await _provider.TryMappedQuery<PostgresLogSegment>(
            "SELECT * FROM stalker_logs " +
            "WHERE timestamp > @timeFrom AND " +
            "timestamp <= @timeTo AND " +
            "log_type & @mask != 0 ORDER BY timestamp DESC LIMIT @limit;",
            new { timeFrom = timeFrom.ToUnixMilliseconds(), timeTo = timeTo.ToUnixMilliseconds(),
                mask = typeFilter, limit = (int)limit });
    }

    public Task<IEnumerable<PostgresLogSegment>> GetLogs(int typeFilter, uint limit)
    {
        return GetLogs(DateTime.MinValue, DateTime.MaxValue, typeFilter, limit);
    }
}