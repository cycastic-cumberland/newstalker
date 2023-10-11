namespace NewstalkerWebAPI.Authority;

public static class AllApiPermission
{
    public const int Grade = 1;
    public const int Query = 2;
    public const int Realtime = 4;
}

public struct ApiPermission
{
    public string ApiKey { get; set; }
    public int Permission { get; set; }
}