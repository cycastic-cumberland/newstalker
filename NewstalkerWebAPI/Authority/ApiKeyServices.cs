using ExtendedComponents;
using PostgresDriver;

namespace NewstalkerWebAPI.Authority;

public static class ApiKeyServices
{
    private static PostgresConnectionSettings Connection => NewstalkerCore.NewstalkerCore.PostgresConnection;

    public static async Task<ApiPermission> GetApiKey(string apiKey)
    {
        await using var db = new PostgresProvider(Connection);
        var keys = await db.TryMappedQuery<ApiPermission>(
            "SELECT api_key AS ApiKey, " +
            "permission AS Permission FROM api_keys " +
            "WHERE api_key = @key;",
            new { key = apiKey });
        return keys.First();
    }
    
    public static async Task<IEnumerable<ApiPermission>> GetApiKeys()
    {
        await using var db = new PostgresProvider(Connection);
        var keys = await db.TryMappedQuery<ApiPermission>(
            "SELECT api_key AS ApiKey, " +
            "permission AS Permission FROM api_keys;");
        return keys;
    }
    public static async Task<bool> MatchPermission(string apiKey, int permissionCode)
    {
        await using var db = new PostgresProvider(Connection);
        var keys = await db.TryMappedQuery<ApiPermission>(
            "SELECT api_key AS ApiKey, " +
            "permission AS Permission FROM api_keys " +
            "WHERE api_key = @key AND permission & @permCode != 0;",
            new { key = apiKey, permCode = permissionCode });
        return keys.Any();
    }
    public static async Task<string> CreateApiKey(int permissionCode)
    {
        await using var db = new PostgresProvider(Connection);
        var apiKey = Crypto.GenerateSecureString(32);
        await db.TryExecute(
            "INSERT INTO api_keys (api_key, permission) VALUES (@key, @permCode);",
            new { key = apiKey, permCode = permissionCode });
        return apiKey;
    }
    public static async Task ModifyApiKey(string apiKey, int permissionCode)
    {
        await using var db = new PostgresProvider(Connection);
        await db.TryExecute("UPDATE api_keys SET permission = @permCode WHERE api_key = @key",
            new { permCode = permissionCode, key = apiKey });
    }
    public static async Task<bool> DeleteApiKey(string apiKey)
    {
        await using var db = new PostgresProvider(Connection);
        // TryExecute return rows affected
        return await db.TryExecute("DELETE FROM api_keys WHERE api_key = @key;",
            new { key = apiKey }) > 0;
    }
}