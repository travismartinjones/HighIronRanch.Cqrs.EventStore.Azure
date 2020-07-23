using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using SimpleCqrs.Domain;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public interface IAzureSnapshotBuilder
    {
        Task<TableEntity> Build(Snapshot snapshot);
    }
}