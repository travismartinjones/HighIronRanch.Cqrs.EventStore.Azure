namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class UtcTimeService : IUtcTimeService
    {
        public System.DateTime UtcNow => System.DateTime.UtcNow;
    }
}