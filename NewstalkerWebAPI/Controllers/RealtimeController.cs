using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NewstalkerPostgresGrader;
using NewstalkerWebAPI.Authority;
using NewstalkerWebAPI.Schemas;

namespace NewstalkerWebAPI.Controllers;

[ApiController]
[Route("realtime")]
[Authorize(AuthenticationSchemes = ApiKeyAuthenticationOptions.DefaultScheme)]
public class RealtimeController : ControllerBase
{
    private static NewstalkerPostgresConductor Conductor
        => ((NewstalkerPostgresConductor)NewstalkerCore.NewstalkerCore.ActiveDaemon.Get("conductor")!)!;
    
    [HttpGet("test")]
    public IActionResult Test()
    {
        return Ok("こんにちは、世界");
    }

    [HttpGet("scrape")]
    public async Task<IActionResult> ScrapeFrontPage(string registeredOutletName)
    {
        var ret = await Conductor.ImpromptuFrontPageScrape(registeredOutletName);
        return Ok(from article in ret select SerializableArticle.From(article));
    }
    
    [HttpPost("extract")]
    public async Task<IActionResult> ExtractKeywords(SerializableArticle article)
    {
        var ret = await Conductor.ExtractTopics(SerializableArticle.To(article));
        return Ok(ret);
    }
    
    [HttpPost("summarize")]
    public async Task<IActionResult> SummarizeArticle(SerializableArticle article)
    {
        var ret = await Conductor.SummarizeArticle(SerializableArticle.To(article));
        return Ok(new SummarizationResponse { Summarized = ret });
    }
}