using NewstalkerCore;
namespace NewstalkerCLI;

public static class Program
{
    public static async Task Main()
    {
        var exit = false;
        await NewstalkerCore.NewstalkerCore.Run();
        Console.CancelKeyPress += delegate(object? _, ConsoleCancelEventArgs args)
        {
            exit = true;
        };
        while (!exit) await Task.Delay(500);
    }
}


