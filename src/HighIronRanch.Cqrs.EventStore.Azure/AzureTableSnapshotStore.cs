using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HighIronRanch.Azure.TableStorage;
using Microsoft.Azure.Cosmos.Table;
using SimpleCqrs.Domain;
using SimpleCqrs.Eventing;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class AzureTableSnapshotStore : ISnapshotStore
    {
        public const string EVENT_STORE_TABLE_NAME = "Snapshots";

        private readonly IAzureBlobSnapshotStore _azureBlobSnapshotStore;
        private readonly IAzureSnapshotBuilder _azureSnapshotBuilder;
        protected readonly IAzureTableService _tableService;
        private readonly IDomainEntityTypeBuilder _domainEntityTypeBuilder;
        protected string _eventStoreTableName; // Used so it can be overridden for tests

        public AzureTableSnapshotStore(
            IAzureBlobSnapshotStore azureBlobSnapshotStore,
            IAzureSnapshotBuilder azureSnapshotBuilder,
            IAzureTableService tableService,
            IDomainEntityTypeBuilder domainEntityTypeBuilder)
        {
            _azureBlobSnapshotStore = azureBlobSnapshotStore;
            _azureSnapshotBuilder = azureSnapshotBuilder;
            _tableService = tableService;
            _domainEntityTypeBuilder = domainEntityTypeBuilder;
            _eventStoreTableName = EVENT_STORE_TABLE_NAME;
        }

        public async Task<Snapshot> GetSnapshot(Guid aggregateRootId)
        {
            var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);

            var query = new TableQuery()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, aggregateRootId.ToString()));

            var snapshots = new List<DynamicTableEntity>();
            TableContinuationToken continuationToken = null;
            do
            {
                var result = await table.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                if (result.Results?.Count > 0)
                {
                    snapshots.AddRange(result.Results);
                }

                continuationToken = result.ContinuationToken;
            } while (continuationToken != null);

            var dynamicTableEntity = snapshots.FirstOrDefault();

            if (dynamicTableEntity == null) return null;

            if (dynamicTableEntity.Properties.ContainsKey(nameof(AzureBlobSnapshot.Url)))
            {
                var azureBlobSnapshot = new AzureBlobSnapshot
                {
                    PartitionKey = dynamicTableEntity.PartitionKey,
                    RowKey = dynamicTableEntity.RowKey,
                    Url = dynamicTableEntity.Properties[nameof(AzureBlobSnapshot.Url)].StringValue
                };
                return await azureBlobSnapshot.GetSnapshot(_azureBlobSnapshotStore, _domainEntityTypeBuilder).ConfigureAwait(false);
            }

            var azureSnapshot = new AzureSnapshot
            {
                PartitionKey = dynamicTableEntity.PartitionKey,
                RowKey = dynamicTableEntity.RowKey,
                P0 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P0)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P0)].BinaryValue : new byte[0],
                P1 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P1)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P1)].BinaryValue : new byte[0],
                P2 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P2)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P2)].BinaryValue : new byte[0],
                P3 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P3)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P3)].BinaryValue : new byte[0],
                P4 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P4)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P4)].BinaryValue : new byte[0],
                P5 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P5)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P5)].BinaryValue : new byte[0],
                P6 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P6)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P6)].BinaryValue : new byte[0],
                P7 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P7)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P7)].BinaryValue : new byte[0],
                P8 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P8)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P8)].BinaryValue : new byte[0],
                P9 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P9)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P9)].BinaryValue : new byte[0],
                P10 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P10)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P10)].BinaryValue : new byte[0],
                P11 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P11)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P11)].BinaryValue : new byte[0],
                P12 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P12)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P12)].BinaryValue : new byte[0],
                P13 = dynamicTableEntity.Properties.ContainsKey(nameof(AzureSnapshot.P13)) ? dynamicTableEntity.Properties[nameof(AzureSnapshot.P13)].BinaryValue : new byte[0],
            };
            return azureSnapshot.GetData().FromTableBson(_domainEntityTypeBuilder.Build(dynamicTableEntity.RowKey)) as Snapshot;
        }

        public async Task SaveSnapshot<TSnapshot>(TSnapshot snapshot) where TSnapshot : Snapshot
        {
            var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);
            var tableEntity = await _azureSnapshotBuilder.Build(snapshot).ConfigureAwait(false);
            await table.ExecuteAsync(TableOperation.InsertOrReplace(tableEntity)).ConfigureAwait(false);
        }

        public class AzureSnapshot : BsonPayloadTableEntity
        {
            protected override int AdditionalPropertySizes => 0;

            public AzureSnapshot() { }

            public AzureSnapshot(Snapshot snapshot)
            {
                PartitionKey = snapshot.AggregateRootId.ToString();
                RowKey = snapshot.GetType().FullName;

                var snapshotData = snapshot.ToTableBson();

                if (snapshotData.Length > MaxByteCapacity)
                {
                    throw new ArgumentException($"Snapshot size of {snapshotData.Length} when stored as json exceeds Azure property limit of 960K");
                }

                SetData(snapshotData);
            }
        }
    }
}