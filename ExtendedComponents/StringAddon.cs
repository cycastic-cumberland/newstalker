using System.Reflection;

namespace ExtendedComponents;

public static class StringAddon
{
    public static string Format(this string self, object anon)
    {
        var fields = anon.GetType().GetFields(BindingFlags.Instance | BindingFlags.NonPublic);
        string ret = self;
        foreach (var field in fields)
        {
            var fieldName = field.Name;
            var value = field.GetValue(anon) ?? "";
            ret = ret.Replace($"{{{fieldName}}}", value.ToString());
        }

        return ret;
    }
}