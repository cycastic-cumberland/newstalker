using PostgresDriver;

namespace NewstalkerPostgresETL;

public static class Initializer
{
    private struct OutletStruct
    {
        public string Url;
        public string OutletName;
    }
    public static async Task InitializeOutlets(PostgresProvider db)
    {
        try
        {
            await db.OpenTransaction(async t =>
            {
                var transaction = t.GetRawTransaction();
                await db.TryExecute("INSERT INTO news_outlets VALUES " +
                                    "(@url, 'Tuổi trẻ') ON CONFLICT DO NOTHING;",
                    new { url = TuoiTreOutlet.BaseUrl }, transaction);
                await db.TryExecute("INSERT INTO news_outlets VALUES " +
                                    "(@url, 'Thanh niên') ON CONFLICT DO NOTHING;",
                    new { url = ThanhNienOutlet.BaseUrl }, transaction);
            });
        }
        catch (Exception)
        {
            // Ignored
        }
    }

    public static async Task<IEnumerable<(string url, string outletName)>> QueryOutletInfo(PostgresProvider db)
    {
        try
        {
            return from val in await db.TryMappedQuery<OutletStruct>(
                    "SELECT base_urls AS Url, name AS OutletName FROM news_outlets;")
                select (val.Url, val.OutletName);
        }
        catch (Exception)
        {
            return ArraySegment<(string url, string outletName)>.Empty;
        }
    }
}