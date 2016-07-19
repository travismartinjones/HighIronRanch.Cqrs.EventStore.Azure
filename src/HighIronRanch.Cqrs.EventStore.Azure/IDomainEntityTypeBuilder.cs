using System;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public interface IDomainEntityTypeBuilder
    {
        Type Build(string typeName);
    }
}