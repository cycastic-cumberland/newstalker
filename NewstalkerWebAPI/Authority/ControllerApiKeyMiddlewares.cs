using NewstalkerWebAPI.Middlewares;

namespace NewstalkerWebAPI.Authority;

public class GradeApiKeyMiddleware : AbstractApiKeyHandler
{
    public override int GetPermissionCode() => AllApiPermission.Grade;
}

public class QueryApiKeyMiddleware : AbstractApiKeyHandler
{
    public override int GetPermissionCode() => AllApiPermission.Query;
}

public class RealtimeApiKeyMiddleware : AbstractApiKeyHandler
{
    public override int GetPermissionCode() => AllApiPermission.Realtime;
}