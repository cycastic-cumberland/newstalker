using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using NewstalkerWebAPI.Authority;

namespace NewstalkerWebAPI.Middlewares;

public abstract class AbstractApiKeyHandler
{
    public abstract int GetPermissionCode();
}

public class ApiKeyAuthenticationHandler : AuthenticationHandler<ApiKeyAuthenticationOptions>
{
    private readonly AbstractApiKeyHandler _gradeHandler = new GradeApiKeyMiddleware();
    private readonly AbstractApiKeyHandler _queryHandler = new QueryApiKeyMiddleware();
    private readonly AbstractApiKeyHandler _realtimeHandler = new RealtimeApiKeyMiddleware();
    
    public ApiKeyAuthenticationHandler(
        IOptionsMonitor<ApiKeyAuthenticationOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder,
        ISystemClock clock)
        : base(options, logger, encoder, clock)
    {
        // Constructor logic here, if needed
    }
    
    private static bool MasterKeyAuthentication(string? key)
    {
        return NewstalkerCore.NewstalkerCore.AuthorizeMasterKey(key) ==
               NewstalkerCore.NewstalkerCore.MasterKeyAuthorizationNResult.Success;
    }

    private static async Task<bool> IsValidApiKey(string? key, AbstractApiKeyHandler handler)
    {
        if (key == null) return false;
        return await ApiKeyServices.MatchPermission(key, handler.GetPermissionCode());
    }

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        AbstractApiKeyHandler handler;
        if (Request.Path.StartsWithSegments("/grade")) handler = _gradeHandler;
        else if (Request.Path.StartsWithSegments("/query")) handler = _queryHandler;
        else if (Request.Path.StartsWithSegments("/realtime")) handler = _realtimeHandler;
        else return AuthenticateResult.Fail("Unknown API route selected");
        string? activeKey;
        if (await IsValidApiKey(Request.Headers[ApiKeyAuthenticationOptions.DefaultHeaderSection], handler))
            activeKey = Request.Headers[ApiKeyAuthenticationOptions.DefaultHeaderSection];
        else if (MasterKeyAuthentication(Request.Headers[MasterKeyAuthenticationOptions.DefaultHeaderSection]))
            activeKey = Request.Headers[MasterKeyAuthenticationOptions.DefaultHeaderSection];
        else return AuthenticateResult.Fail("Invalid Master Key provided.");
        var claims = new List<Claim>
        {
            new(ClaimTypes.Authentication, activeKey!)
        };
        var identity = new ClaimsIdentity(claims, Options.AuthenticationType);
        var identities = new List<ClaimsIdentity> { identity };
        var principal = new ClaimsPrincipal(identities);
        var ticket = new AuthenticationTicket(principal, Options.Scheme);
        return AuthenticateResult.Success(ticket);
    }
}
