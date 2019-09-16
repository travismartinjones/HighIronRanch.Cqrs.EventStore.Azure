using SimpleCqrs.Domain;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class DomainSnapshot<T> : Snapshot where T : AggregateRoot
    {
        public DomainSnapshot()
        {
        }

        public DomainSnapshot(T entity)
        {
            Entity = entity;
            LastEventSequence = entity.LastEventSequence;
        }

        public T Entity { get; set; }
    }
}