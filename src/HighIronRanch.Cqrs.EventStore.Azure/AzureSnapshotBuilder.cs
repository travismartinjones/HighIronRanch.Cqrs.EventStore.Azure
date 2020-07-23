using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using SimpleCqrs.Domain;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class AzureSnapshotBuilder : IAzureSnapshotBuilder
    {
        private readonly IAzureBlobSnapshotStore _azureBlobSnapshotStore;

        public AzureSnapshotBuilder(IAzureBlobSnapshotStore azureBlobSnapshotStore)
        {
            _azureBlobSnapshotStore = azureBlobSnapshotStore;
        }

        public async Task<TableEntity> Build(Snapshot snapshot)
        {
            var snapshotData = snapshot.ToTableBson();

            if (snapshotData.Length > BsonPayloadTableEntity.MaxByteCapacity)
            {
                var blobSnapshot = new AzureBlobSnapshot();
                await blobSnapshot.ApplySnapshot(_azureBlobSnapshotStore,snapshot).ConfigureAwait(false);
                return blobSnapshot;
            }

            return new AzureTableSnapshotStore.AzureSnapshot(snapshot);
        }
    }
}