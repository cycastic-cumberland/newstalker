using System.Data;

namespace PostgresDriver;

public interface ITransaction : IDisposable
{
    public void Start();
    public void RollBack();
    public void Commit();
    public IDbTransaction? GetRawTransaction();
}