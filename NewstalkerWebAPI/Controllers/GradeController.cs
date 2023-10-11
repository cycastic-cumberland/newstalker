using Microsoft.AspNetCore.Mvc;
using NewstalkerExtendedComponents;
using NewstalkerPostgresGrader;
using NewstalkerWebAPI.Schemas;

namespace NewstalkerWebAPI.Controllers;

[ApiController]
[Route("[controller]")]
public class GradeController : ControllerBase
{
    private static NewstalkerPostgresConductor Conductor
        => ((NewstalkerPostgresConductor)NewstalkerCore.NewstalkerCore.ActiveDaemon.Get("conductor")!)!;
    [HttpGet("test")]
    public IActionResult Test()
    {
        return Ok("Hallo welt");
    }

    [HttpPost("tags")]
    public async Task<IActionResult> GradeTagsRelevancy(DateTime timeFrom, DateTime timeTo, bool ascend = false, OutletSelections? outlets = null)
    {
        var ret = await Conductor.GradeRelevancyAsync(new()
        {
            GradingTarget = AbstractGrader.GradeType.Tags,
            PopularityWindow = timeTo - timeFrom,
            TimeEnd = timeTo,
            OutletSelections =  outlets?.OutletUrls!,
        });
        return Ok((ascend
            ? ret.OrderBy(x => x.Value)
            : ret.OrderByDescending(x => x.Value)).ToDictionary(o => o.Key, o => o.Value));
    }
    
    [HttpPost("keywords")]
    public async Task<IActionResult> GradeKeywordsRelevancy(DateTime timeFrom, DateTime timeTo, bool ascend = false, OutletSelections? outlets = null)
    {
        var ret = await Conductor.GradeRelevancyAsync(new()
        {
            GradingTarget = AbstractGrader.GradeType.Keywords,
            PopularityWindow = timeTo - timeFrom,
            TimeEnd = timeTo,
            OutletSelections =  outlets?.OutletUrls!,
        });
        return Ok((ascend
            ? ret.OrderBy(x => x.Value)
            : ret.OrderByDescending(x => x.Value)).ToDictionary(o => o.Key, o => o.Value));
    }
    
    [HttpPost("articles")]
    public async Task<IActionResult> GradeArticlesRelevancy(DateTime timeFrom, DateTime timeTo, bool ascend = false, double tagsWeight = 0.0, OutletSelections? outlets = null)
    {
        var ret = await Conductor.GradeRelevancyAsync(new()
        {
            GradingTarget = AbstractGrader.GradeType.Articles,
            PopularityWindow = timeTo - timeFrom,
            TimeEnd = timeTo,
            OutletSelections =  outlets?.OutletUrls!,
            NormalizedScale = Math.Clamp(tagsWeight, 0.0, 0.998)
        });
        return Ok((ascend
            ? ret.OrderBy(x => x.Value)
            : ret.OrderByDescending(x => x.Value)).ToDictionary(o => o.Key, o => o.Value));
    }
}