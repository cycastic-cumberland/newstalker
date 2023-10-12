using ExtendedComponents;
using Microsoft.OpenApi.Models;
using NewstalkerWebAPI.Authority;
using NewstalkerWebAPI.Middlewares;

namespace NewstalkerWebAPI;

public static class Program
{
    private static Dictionary<string, string> _cmdArgs = null!;
    private static bool _swaggerEnabled;
    private static void CmdArgumentsParse(string[] args)
    {
        _cmdArgs = new CmdArgumentsParser(args).Parse();
        _cmdArgs.TryAdd("swagger-enabled", "false");
        _swaggerEnabled = bool.Parse(_cmdArgs["swagger-enabled"]);
    }
    public static async Task Main(string[] args)
    {
        CmdArgumentsParse(args);
        await NewstalkerCore.NewstalkerCore.Run();
        var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

        builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("v1", new OpenApiInfo { Title = "Newstalker", Version = "v1" });
            // No auth scheme
            c.AddSecurityDefinition(MasterKeyAuthenticationOptions.DefaultScheme, new OpenApiSecurityScheme
            {
                In = ParameterLocation.Header,
                Name = MasterKeyAuthenticationOptions.DefaultHeaderSection,
                Type = SecuritySchemeType.ApiKey,
            });
            c.AddSecurityDefinition(ApiKeyAuthenticationOptions.DefaultScheme, new OpenApiSecurityScheme
            {
                In = ParameterLocation.Header,
                Name = ApiKeyAuthenticationOptions.DefaultHeaderSection,
                Type = SecuritySchemeType.ApiKey,
            });
            c.AddSecurityRequirement(new OpenApiSecurityRequirement
            {
                {
                    new OpenApiSecurityScheme
                    {
                        Reference = new OpenApiReference
                            { Type = ReferenceType.SecurityScheme, Id = ApiKeyAuthenticationOptions.DefaultScheme }
                    },
                    ArraySegment<string>.Empty
                },
                {
                    new OpenApiSecurityScheme
                    {
                        Reference = new OpenApiReference
                            { Type = ReferenceType.SecurityScheme, Id = MasterKeyAuthenticationOptions.DefaultScheme }
                    },
                    ArraySegment<string>.Empty
                }
            });
        });
        builder.Services.AddAuthorization(options =>
        {
            options.AddPolicy(MasterKeyAuthenticationOptions.DefaultScheme, policy => policy.RequireClaim(MasterKeyAuthenticationOptions.DefaultScheme));
            options.AddPolicy(ApiKeyAuthenticationOptions.DefaultScheme, policy => policy.RequireClaim(ApiKeyAuthenticationOptions.DefaultScheme));
        });
        builder.Services.AddAuthentication(ApiKeyAuthenticationOptions.DefaultScheme)
            .AddScheme<ApiKeyAuthenticationOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationOptions.DefaultScheme,
                _ => {  });
        builder.Services.AddAuthentication(MasterKeyAuthenticationOptions.DefaultScheme)
            .AddScheme<MasterKeyAuthenticationOptions, MasterKeyAuthenticationHandler>(
                MasterKeyAuthenticationOptions.DefaultScheme,
                _ => { });

        var app = builder.Build();

        if (_swaggerEnabled || app.Environment.IsDevelopment())
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }
        app.UseHttpsRedirection();
        app.UseAuthorization();
        app.MapControllers();
        await app.RunAsync();
    }
}