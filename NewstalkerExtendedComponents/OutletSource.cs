using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace NewstalkerExtendedComponents;

public class OutletSource : IReadOnlyDictionary<string, AbstractNewsOutlet>, IDisposable
{
    private readonly Dictionary<string, AbstractNewsOutlet> _map = new();

    public bool ContainsKey(string key) => _map.ContainsKey(key);

    public bool TryGetValue(string key, [MaybeNullWhen(false)] out AbstractNewsOutlet value)
        => _map.TryGetValue(key, out value);

    public AbstractNewsOutlet this[string name]
    {
        get => _map[name];
        set => _map[name] = value;
    }

    public IEnumerable<string> Keys => _map.Keys;
    public IEnumerable<AbstractNewsOutlet> Values => _map.Values;

    public IEnumerator<KeyValuePair<string, AbstractNewsOutlet>> GetEnumerator()
        => _map.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => _map.GetEnumerator();

    public int Count => _map.Count;

    public void Dispose()
    {
        foreach (var (_, outlet) in this)
        {
            outlet.Dispose();
        }
    }
}