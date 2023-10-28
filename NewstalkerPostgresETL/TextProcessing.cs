using System.Text;
using HtmlAgilityPack;

namespace NewstalkerPostgresETL;

public static class TextProcessing
{
    public class TextExtractionOptions
    {
        public (IEnumerable<string> classes, uint depth) ExcludedParentsWithClasses = (ArraySegment<string>.Empty, 5);
        public (IEnumerable<string> classes, uint depth) InlinedLinkAware = (ArraySegment<string>.Empty, 5);
    }
    public static void SetFirstInnerTextFromClass(HtmlDocument doc, string className, ref string target)
    {
        var node = Utilities.GetFirstNodeFromClass(doc, className);
        if (node != null)
            target = node.InnerText.Trim();
    }
    public static bool HasClassInAncestors(HtmlNode node, string className, uint depth)
    {
        uint curr = 0;
        while (node != null && curr < depth)
        {
            if (node.HasClass(className)) return true;
            node = node.ParentNode;
            curr++;
        }

        return false;
    }

    private static int EscapeCharacter(this string self, int fromIdx, int maxIdx = 10)
    {
        for (int i = fromIdx + 1, s = Math.Min(self.Length, fromIdx + maxIdx); i < s; i++)
        {
            if (self[i] == ';') return i - fromIdx;
        }
        return 0;
    }
    private static string HtmlTextTrim(this string self, out int wordCount)
    {
        var startIdx = 0;
        while (startIdx < self.Length && char.IsWhiteSpace(self[startIdx]))
            ++startIdx;
        var endIdx = self.Length - 1;
        while (endIdx >= startIdx && char.IsWhiteSpace(self[endIdx]))
            --endIdx;
        var length = endIdx - startIdx + 1;
        if (length == 0)
        {
            wordCount = 0;
            return string.Empty;
        }

        int spaceCount = 0;
        StringBuilder sb = new();
        for (var i = startIdx; i < length; i++)
        {
            var c = self[i];
            switch (c)
            {
                case '\n' or '\r':
                    spaceCount++;
                    continue;
                case ' ':
                    spaceCount++;
                    break;
                case '&':
                    i += self.EscapeCharacter(i);
                    continue;
            }

            sb.Append(c);
        }

        wordCount = spaceCount + 1;
        return sb.ToString();
    }
    public static async Task<(string text, int wordCount)> ExtractAllSubText(HtmlDocument doc, string className, TextExtractionOptions options)
    {
        StringBuilder builder = new();
        var root = Utilities.GetFirstNodeFromClass(doc, className);
        if (root == null) throw new NullReferenceException($"Can't find node with class '{className}'");
        // var prefix = "";
        LinkedList<string> strings = new();
        List<Task> tasks = new();
        int totalWordCount = 0;
        foreach (var node in root.DescendantsAndSelf())
        {
            if (node.HasChildNodes) continue;
            if (node.ParentNode.Name.Equals("script", StringComparison.OrdinalIgnoreCase)) continue;
            var text = node.InnerText;
            if (string.IsNullOrEmpty(text) || string.IsNullOrWhiteSpace(text)) continue;
            var exclude = options.ExcludedParentsWithClasses.classes.Any(excluded => 
                HasClassInAncestors(node.ParentNode, excluded, options.ExcludedParentsWithClasses.depth));
            if (exclude) continue;
            // var isInlinedLink = options.InlinedLinkAware.classes.Any(link =>
            //     HasClassInAncestors(node.ParentNode, link, options.InlinedLinkAware.depth));
            var isSpecial = node.ParentNode.Name.Equals("a", StringComparison.OrdinalIgnoreCase) ||
                            node.ParentNode.Name.Equals("i", StringComparison.OrdinalIgnoreCase) ||
                            node.ParentNode.Name.Equals("b", StringComparison.OrdinalIgnoreCase);
            strings.AddLast(string.Empty);
            var currSegment = strings.Last!;
            tasks.Add(Task.Run(() =>
            {
                var prefix = " ";
                var suffix = "\n";
                var extractedText = text.HtmlTextTrim(out var wordCount);
                if (isSpecial)
                {
                    prefix = "\b ";
                    suffix = " ";
                }

                Interlocked.Add(ref totalWordCount, wordCount);
                currSegment.Value = prefix + extractedText + suffix;
            }));
        }

        await Task.WhenAll(tasks);
        foreach (var str in strings)
        {
            if (str.StartsWith('\b') && builder.Length > 0)
            {
                builder.Remove(builder.Length - 1, 1);
                builder.Append(str.AsSpan(1, str.Length - 1));
            } else builder.Append(str);
        }
        return (builder.ToString(), totalWordCount);
    }
}