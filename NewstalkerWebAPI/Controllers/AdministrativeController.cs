using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NewstalkerCore;
using NewstalkerWebAPI.Authority;
using PostgresDriver;

namespace NewstalkerWebAPI.Controllers;

[ApiController]
[Route("administrative")]
[Authorize(AuthenticationSchemes = MasterKeyAuthenticationOptions.DefaultScheme)]
public class AdministrativeController : ControllerBase
{
    [HttpGet("test")]
    public IActionResult Test()
    {
        return Ok("안녕하세요, 세계");
    }

    [HttpGet("api_keys")]
    public async Task<IActionResult> GetAllApiKeyInfos()
    {
        return Ok((await ApiKeyServices.GetApiKeys()).ToArray());
    }
    [HttpGet("api_key")]
    public async Task<IActionResult> GetApiKeyInfo(string key)
    {
        try
        {
            return Ok(await ApiKeyServices.GetApiKey(key));
        }
        catch (InvalidOperationException)
        {
            return NotFound();
        }
    }
    [HttpPut("api_key")]
    public async Task<IActionResult> CreateApiKey(int permissionCode)
    {
        return Ok(await ApiKeyServices.CreateApiKey(permissionCode));
    }
    [HttpPatch("api_key")]
    public async Task<IActionResult> ModifyApiKeyPermission(string apiKey, int permissionCode)
    {
        await ApiKeyServices.ModifyApiKey(apiKey, permissionCode);
        return Ok();
    }
    [HttpDelete("api_key")]
    public async Task<IActionResult> DeleteApiKey(string apiKey)
    {
        return await ApiKeyServices.DeleteApiKey(apiKey) ? Ok() : StatusCode(500, "Failed to delete API key");
    }
    [HttpPost("db/init_core")]
    public async Task<IActionResult> InitializeCoreTables()
    {
        await using var db = new PostgresProvider(NewstalkerCore.NewstalkerCore.PostgresConnection);
        await Initializer.InitializeCoreTables(db);
        return Ok();
    }
    [HttpPost("db/init_administrative")]
    public async Task<IActionResult> InitializeAdministrativeTables()
    {
        await using var db = new PostgresProvider(NewstalkerCore.NewstalkerCore.PostgresConnection);
        await Initializer.InitializeAdministrativeTables(db);
        return Ok();
    }
}