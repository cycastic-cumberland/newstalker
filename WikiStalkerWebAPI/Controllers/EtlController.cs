using ETLnR;
using ExtendedComponents;
using ExtendedPostgresDriver;
using Microsoft.AspNetCore.Mvc;
using WikiStalkerPostgresETL;

namespace WikiStalkerWebAPI.Controllers;

[ApiController]
[Route("[controller]")]
public class EtlController : ControllerBase
{
    private static readonly DaemonManager Manager = new();
    
    [HttpGet("test")]
    public IActionResult HelloWorld()
    {
        return Ok("Ohayo, Sekai");
    }

    [HttpPost("start_etl")]
    public async Task<IActionResult> StartEtl(PostgresHarvesterSettings settings)
    {
        var result = await Task.Run(() =>
        {
            return Manager.Manage("etl", () => new PostgresHarvester(settings,
                new LoggingServerDelegate[]
                {
                    new PostgresLogger(settings.ConnectionSettings, "stalker_logs"),
                    new StdLoggingServerDelegate()
                }));
        });

        return result ? Ok("OK") : Conflict("Daemon already started");
    }

    [HttpGet("run_gc")]
    public IActionResult RunGc()
    {
        var etl = Manager.Get("etl") as AbstractHarvester;
        if (etl == null) return NotFound();
        etl.RunGarbageCollection();
        return Ok("OK");
    }

    [HttpGet("stop_etl")]
    public IActionResult Stop()
    {
        var result = Manager.CloseDaemon("etl");

        return result ? Ok("OK") : NotFound("Daemon not found");
    }
    // [HttpGet("kill_app")]
    // public IActionResult Kill()
    // {
    //     Engine.Manager.ManualCleanUp();
    //     (Engine.Lifetime as IHostApplicationLifetime)?.StopApplication();
    //     return Ok("kms rn...");
    // }
}