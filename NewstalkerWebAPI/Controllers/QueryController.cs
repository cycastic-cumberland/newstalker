using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using NewstalkerExtendedComponents;
using NewstalkerPostgresGrader;
using NewstalkerWebAPI.Authority;
using NewstalkerWebAPI.Schemas;

namespace NewstalkerWebAPI.Controllers;

[ApiController]
[Route("query")]
[Authorize(AuthenticationSchemes = ApiKeyAuthenticationOptions.DefaultScheme)]
public class QueryController : ControllerBase
{
    private struct QueryCountResponse
    {
        public long Count { get; set; }
    }

    private static NewstalkerPostgresConductor Conductor
        => ((NewstalkerPostgresConductor)NewstalkerCore.NewstalkerCore.ActiveDaemon.Get("conductor")!)!;
    
    [HttpGet("test")]
    public IActionResult Test()
    {
        return Ok("Hello world");
    }
    
    [HttpPost("tags/count")]
    public async Task<IActionResult> GetTagsCount(DateTime timeFrom, DateTime timeTo, string tag, OutletSelections? outlets = null)
    {
        var ret = await Conductor.QueryAmountAsync(new()
        {
            GradingTarget = AbstractGrader.GradeType.Tags,
            PopularityWindow = timeTo - timeFrom,
            TargetSelection =  tag,
            TimeEnd = timeTo,
            OutletSelections =  outlets?.OutletUrls!,
        });
        return Ok(new QueryCountResponse
        {
            Count = ret
        });
    }
    
    [HttpPost("keywords/count")]
    public async Task<IActionResult> GetKeywordsCount(DateTime timeFrom, DateTime timeTo, string keyword, OutletSelections? outlets = null)
    {
        var ret = await Conductor.QueryAmountAsync(new()
        {
            GradingTarget = AbstractGrader.GradeType.Keywords,
            PopularityWindow = timeTo - timeFrom,
            TargetSelection =  keyword,
            TimeEnd = timeTo,
            OutletSelections =  outlets?.OutletUrls!,
        });
        return Ok(new QueryCountResponse
        {
            Count = ret
        });
    }
    
    [HttpPost("articles/count")]
    public async Task<IActionResult> GetArticlesCount(DateTime timeFrom, DateTime timeTo, OutletSelections? outlets = null)
    {
        var ret = await Conductor.QueryAmountAsync(new()
        {
            GradingTarget = AbstractGrader.GradeType.Articles,
            PopularityWindow = timeTo - timeFrom,
            TimeEnd = timeTo,
            OutletSelections =  outlets?.OutletUrls!,
        });
        return Ok(new QueryCountResponse
        {
            Count = ret
        });
    }
    
    [HttpGet("article")]
    public async Task<IActionResult> QueryArticle(string articleUrl)
    {
        var ret = await Conductor.QueryArticle(articleUrl);
        return ret != null ? Ok(SerializableArticle.From(ret)) : NotFound();
    }
    
    [HttpGet("articles")]
    public async Task<IActionResult> QueryArticles(DateTime timeFrom, DateTime timeTo)
    {
        var ret = await Conductor.QueryArticles(timeFrom, timeTo);
        return Ok(ret);
    }
    
    [HttpGet("article/tag")]
    public async Task<IActionResult> QueryArticleTags(string articleUrl)
    {
        var ret = await Conductor.QueryArticleTags(articleUrl);
        return Ok(ret);
    }
    
    [HttpGet("article/keyword")]
    public async Task<IActionResult> QueryArticleKeywords(string articleUrl)
    {
        var ret = await Conductor.QueryArticleKeywords(articleUrl);
        return Ok(ret);
    }
    
    [HttpGet("article/summarized")]
    public async Task<IActionResult> QuerySummarizedText(string articleUrl)
    {
        var ret = await Conductor.QuerySummarizedText(articleUrl);
        return string.IsNullOrEmpty(ret) ? NotFound() : Ok(new SummarizationResponse { Summarized = ret });
    }

    [HttpGet("sessions/latest")]
    public async Task<IActionResult> GetLatestSession()
    {
        try
        {
            var ret = await Conductor.GetLatestSession();
            return Ok(ret);
        }
        catch (InvalidOperationException)
        {
            return NotFound("No session started");
        }
    }
    [HttpGet("sessions/latest/by-state")]
    public async Task<IActionResult> GetLatestSession(bool isFinished)
    {
        try
        {
            var ret = await Conductor.GetLatestSession(isFinished);
            return Ok(ret);
        }
        catch (InvalidOperationException)
        {
            return NotFound("No session with given state found");
        }
    }

    [HttpGet("sessions")]
    public async Task<IActionResult> GetSessions(DateTime timeFrom, DateTime timeTo)
    {
        return Ok((await Conductor.GetSessions(timeFrom, timeTo)).ToArray());
    }
    
    [HttpGet("sessions/by-state")]
    public async Task<IActionResult> GetSessions(DateTime timeFrom, DateTime timeTo, bool isFinished)
    {
        return Ok((await Conductor.GetSessions(timeFrom, timeTo, isFinished)).ToArray());
    }
}