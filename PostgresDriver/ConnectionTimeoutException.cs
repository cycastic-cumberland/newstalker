namespace PostgresDriver;

public class ConnectionTimeoutException : Exception
{
    public ConnectionTimeoutException(string msg) : base(msg) {}
    public ConnectionTimeoutException() {}
}