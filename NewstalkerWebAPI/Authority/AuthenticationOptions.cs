using Microsoft.AspNetCore.Authentication;

namespace NewstalkerWebAPI.Authority;

public class MasterKeyAuthenticationOptions : AuthenticationSchemeOptions
{
    public const string DefaultScheme = "MasterKey";
    public const string DefaultHeaderSection = "X-APP-ID";
    public string Scheme => DefaultScheme;
    public string AuthenticationType = DefaultScheme;
}

public class ApiKeyAuthenticationOptions : AuthenticationSchemeOptions
{
    public const string DefaultScheme = "APIKey";
    public const string DefaultHeaderSection = "X-API-KEY";
    public string Scheme => DefaultScheme;
    public string AuthenticationType = DefaultScheme;
}