using System.Text;

namespace ExtendedComponents;

public class CmdArgumentsParser
{
    private readonly Dictionary<string, string> _cmdArgs = new();
    private readonly string[] _arguments;
    private StringBuilder? _combinedString;
    private string _lastKey = "";

    public CmdArgumentsParser(string[] args)
    {
        _arguments = args;
    }

    private void ParseString(string argument)
    {
        if (string.IsNullOrWhiteSpace(argument)) return;

        if (argument.StartsWith("--"))
        {
            _lastKey = argument[2..];
            return;
        }
            
        if (argument.StartsWith("\""))
        {
            if (_combinedString != null)
            {
                var nextString = argument[1..];
                ParseString(" \"");
                ParseString(nextString);
            }
            else
            {
                _combinedString = new();
                _combinedString.Append(argument[1..]);
            }
            return;
        }

        if (argument.EndsWith("\""))
        {
            if (_combinedString == null)
            {
                throw new Exception("No string to close");
            }

            var success = _cmdArgs.TryAdd(_lastKey, _combinedString.Append(argument).ToString()[..^1]);
            if (!success)
                throw new Exception($"Repeating command line argument: {_lastKey}");
            _combinedString = null!;
        }
        if (_combinedString != null)
        {
            _combinedString.Append(argument);
            return;
        }

        {
            var success = _cmdArgs.TryAdd(_lastKey, argument);
            if (!success)
                throw new Exception($"Repeating command line argument: {_lastKey}");
        }
    }
    
    public Dictionary<string, string> Parse()
    {
        foreach (var argument in _arguments)
        {
            ParseString(argument);
        }

        if (_combinedString != null)
            throw new Exception("Unescaped string at the end of command line arguments");
        return _cmdArgs;
    }
}