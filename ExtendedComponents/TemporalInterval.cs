namespace ExtendedComponents;

public struct TemporalInterval
{
    public DateTime Epoch;
    public TimeSpan Length;
    public DateTime End => Epoch + Length;


    public TemporalInterval(DateTime epoch, TimeSpan length)
    {
        Epoch = epoch;
        Length = length;
    }
    public bool Contains(DateTime time) => time > Epoch && time <= End;
}