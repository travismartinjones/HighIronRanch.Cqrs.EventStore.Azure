namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public interface IUtcTimeService
    {
        System.DateTime UtcNow { get; }
    }
}