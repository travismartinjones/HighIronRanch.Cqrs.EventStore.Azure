using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;
using SimpleCqrs.Domain;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class AzureBlobSnapshot : TableEntity
    {
        public class AzureBlobSnapshotPayload
        {
            public string Json { get; set; }
            public string TypeName { get; set; }
        }

        public string Url { get; set; }

        public AzureBlobSnapshot() {}

        private string GetBlobName(Guid aggregateRootId)
        {
            return "snapshots/" + aggregateRootId.ToString().Replace("-", "") + ".json";
        }

        public async Task<Snapshot> GetSnapshot(
            IAzureBlobSnapshotStore azureBlobSnapshotStore,
            IDomainEntityTypeBuilder domainEntityTypeBuilder)
        {
            var blob = await azureBlobSnapshotStore.Get(GetBlobName(new Guid(PartitionKey))).ConfigureAwait(false);
            return blob.Json.FromJson(domainEntityTypeBuilder.Build(blob.TypeName)) as Snapshot;
        }

        public async Task ApplySnapshot(IAzureBlobSnapshotStore azureBlobSnapshotStore, Snapshot snapshot)
        {
            PartitionKey = snapshot.AggregateRootId.ToString();
            RowKey = snapshot.GetType().FullName;
            Url = GetBlobName(snapshot.AggregateRootId);
            var payload = new AzureBlobSnapshotPayload
            {
                Json = snapshot.ToJson(),
                TypeName = snapshot.GetType().FullName
            };
            await azureBlobSnapshotStore.Save(Url, payload).ConfigureAwait(false);
        }
    }
}