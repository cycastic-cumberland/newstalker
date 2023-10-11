using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Options;
using NewstalkerWebAPI.Authority;

namespace NewstalkerWebAPI.Middlewares;

public class MasterKeyAuthenticationHandler : AuthenticationHandler<MasterKeyAuthenticationOptions>
{
    public MasterKeyAuthenticationHandler(
        IOptionsMonitor<MasterKeyAuthenticationOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder,
        ISystemClock clock)
        : base(options, logger, encoder, clock)
    {
        // Constructor logic here, if needed
    }
    
    private static bool IsValidApiKey(string? key)
    {
        return NewstalkerCore.NewstalkerCore.AuthorizeMasterKey(key) ==
               NewstalkerCore.NewstalkerCore.MasterKeyAuthorizationNResult.Success;
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!IsValidApiKey(Request.Headers[MasterKeyAuthenticationOptions.DefaultHeaderSection]))
        {
            return Task.FromResult(AuthenticateResult.Fail("Invalid Master Key provided."));
        }
        var claims = new List<Claim>
        {
            new(ClaimTypes.Authentication, Request.Headers[MasterKeyAuthenticationOptions.DefaultHeaderSection]!)
        };
        var identity = new ClaimsIdentity(claims, Options.AuthenticationType);
        var identities = new List<ClaimsIdentity> { identity };
        var principal = new ClaimsPrincipal(identities);
        var ticket = new AuthenticationTicket(principal, Options.Scheme);
        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
} 
