using System;
using System.Threading.Tasks;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public interface IAzureBlobSnapshotStore
    {
        Task Save(string filename, AzureBlobSnapshot.AzureBlobSnapshotPayload snapshot);
        Task<AzureBlobSnapshot.AzureBlobSnapshotPayload> Get(string filename);
    }
}