using ExtendedComponents;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NewstalkerCore;
using NewstalkerPostgresGrader;
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

    [HttpGet("apikeys")]
    public async Task<IActionResult> GetAllApiKeyInfos()
    {
        return Ok((await ApiKeyServices.GetApiKeys()).ToArray());
    }
    [HttpGet("apikey")]
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
    [HttpPut("apikey")]
    public async Task<IActionResult> CreateApiKey(int permissionCode)
    {
        return Ok(await ApiKeyServices.CreateApiKey(permissionCode));
    }
    [HttpPatch("apikey")]
    public async Task<IActionResult> ModifyApiKeyPermission(string apiKey, int permissionCode)
    {
        await ApiKeyServices.ModifyApiKey(apiKey, permissionCode);
        return Ok();
    }
    [HttpDelete("apikey")]
    public async Task<IActionResult> DeleteApiKey(string apiKey)
    {
        return await ApiKeyServices.DeleteApiKey(apiKey) ? Ok() : StatusCode(500, "Failed to delete API key");
    }
    [HttpPost("db/init-core")]
    public async Task<IActionResult> InitializeCoreTables()
    {
        await using var db = new PostgresProvider(NewstalkerCore.NewstalkerCore.PostgresConnection);
        await Initializer.InitializeCoreTables(db);
        return Ok();
    }
    [HttpPost("db/init-administrative")]
    public async Task<IActionResult> InitializeAdministrativeTables()
    {
        await using var db = new PostgresProvider(NewstalkerCore.NewstalkerCore.PostgresConnection);
        await Initializer.InitializeAdministrativeTables(db);
        return Ok();
    }

    [HttpPost("db/run-gc")]
    public async Task<IActionResult> RunGarbageCollection()
    {
        var conductor = NewstalkerCore.NewstalkerCore.ActiveDaemon.Get("conductor") as NewstalkerPostgresConductor;
        if (conductor == null)
            return StatusCode(503, "Conductor daemon is not running");
        var affected = await conductor.RunGarbageCollectionAsync();
        return Ok($"Affected row(s): {affected}");
    }

    [HttpGet("logs/by-span")]
    public async Task<IActionResult> GetLogs(DateTime timeFrom, DateTime timeTo, 
        int mask = (int)LogSegment.LogSegmentType.Message & (int)LogSegment.LogSegmentType.Exception, uint limit = 100)
    {
        var ret = await NewstalkerCore.NewstalkerCore.GetLogs(timeFrom, timeTo, mask, limit);
        return Ok(ret.Select(o => o.Convert())
            .ToDictionary(o => $"[{o.Timestamp} Type:{(int)o.LogType}]", o => $"{o.Header}: {o.Message}"));
    }
    [HttpGet("logs/latest")]
    public async Task<IActionResult> GetLogs(int mask = (int)LogSegment.LogSegmentType.Message 
                                                        & (int)LogSegment.LogSegmentType.Exception, uint limit = 100)
    {
        var ret = await NewstalkerCore.NewstalkerCore.GetLogs(mask, limit);
        return Ok(ret.Select(o => o.Convert())
            .ToDictionary(o => $"[{o.Timestamp} :{(int)o.LogType}]", o => $"{o.Header}: {o.Message}"));
    }
    [HttpGet("logs/by-latest-span")]
    public async Task<IActionResult> GetLogs(uint seconds,
        int mask = (int)LogSegment.LogSegmentType.Message & (int)LogSegment.LogSegmentType.Exception, uint limit = 100)
    {
        var now = DateTime.Now;
        var ret = await NewstalkerCore.NewstalkerCore.GetLogs(now - TimeSpan.FromSeconds(seconds), now,
            mask, limit);
        return Ok(ret.Select(o => o.Convert())
            .ToDictionary(o => $"[{o.Timestamp} :{(int)o.LogType}]", o => $"{o.Header}: {o.Message}"));
    }
}