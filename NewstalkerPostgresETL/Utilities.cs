using System.Globalization;
using System.Text.RegularExpressions;
using HtmlAgilityPack;
using NewstalkerExtendedComponents;

namespace NewstalkerPostgresETL;

internal static class Utilities
{
    public static DateTime StandardizedTimeProcess(string text)
    {
        if (string.IsNullOrEmpty(text)) return DateTime.Today;
        try
        {
            var utcOffset = TimeZoneInfo.Local.BaseUtcOffset;
            var match = Regex.Match(text, @"(.+)\s(GMT)([+-])(\d+)");
            if (!match.Success) return DateTime.Today;
            var timeGroup = match.Groups["1"].Value;
            var operationGroup = match.Groups["3"].Value;
            var offsetGroup = match.Groups["4"].Value;
            var time = DateTime.ParseExact(timeGroup, "dd/MM/yyyy HH':'mm", CultureInfo.InvariantCulture);
            var offset = int.Parse(offsetGroup);
            // Invert the time to get UTC
            if (operationGroup == "-")
            {
                time = time.Add(new TimeSpan(0, offset, 0, 0) + utcOffset);
            }
            else
            {
                time = time.Add(utcOffset - new TimeSpan(0, offset, 0, 0));
            }
            return time;
        }
        catch (Exception)
        {
            return DateTime.Today;
        }
    }
    public static IEnumerable<HtmlNode> GetNodesFromClass(HtmlDocument doc, string className)
        => from node in doc.DocumentNode.Descendants(0)
            where node.HasClass(className) select node;
    
    public static IEnumerable<HtmlNode> GetNodesFromClass(HtmlNode parentNode, string className)
        => from node in parentNode.Descendants(0)
            where node.HasClass(className) select node;
    public static HtmlNode? GetFirstNodeFromClass(HtmlDocument doc, string className)
    {
        var titleNodes = GetNodesFromClass(doc, className);
        return titleNodes.FirstOrDefault();
    }
    public static HtmlNode? GetFirstNodeFromClass(HtmlNode node, string className)
    {
        var titleNodes = GetNodesFromClass(node, className);
        return titleNodes.FirstOrDefault();
    }
    public static async Task<IEnumerable<string>> LinksScrape(string url, HtmlWeb htmlWeb)
    {
        var htmlDoc = await htmlWeb.LoadFromWebAsync(url);
        if (htmlDoc == null) return ArraySegment<string>.Empty;
        var linkNode = htmlDoc.DocumentNode.SelectNodes("//a[@href]");
        return from node in linkNode select node.GetAttributeValue("href", "");
    }
}