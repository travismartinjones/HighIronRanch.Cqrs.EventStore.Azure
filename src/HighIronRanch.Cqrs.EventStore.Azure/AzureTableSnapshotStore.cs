using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using HighIronRanch.Azure.TableStorage;
using Microsoft.WindowsAzure.Storage.Table;
using SimpleCqrs.Domain;
using SimpleCqrs.Eventing;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class AzureTableSnapshotStore : ISnapshotStore
    {
        public const string EVENT_STORE_TABLE_NAME = "Snapshots";

        protected readonly IAzureTableService _tableService;
        private readonly IDomainEntityTypeBuilder _domainEntityTypeBuilder;
        protected string _eventStoreTableName; // Used so it can be overridden for tests

        public AzureTableSnapshotStore(
            IAzureTableService tableService,
            IDomainEntityTypeBuilder domainEntityTypeBuilder)
        {
            _tableService = tableService;
            _domainEntityTypeBuilder = domainEntityTypeBuilder;
            _eventStoreTableName = EVENT_STORE_TABLE_NAME;
        }

        public async Task<Snapshot> GetSnapshot(Guid aggregateRootId)
        {
            var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);

            var query = new TableQuery<AzureSnapshot>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, aggregateRootId.ToString()));

            var snapshots = new List<AzureSnapshot>();
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

            var azureSnapshot = snapshots.FirstOrDefault();

            return azureSnapshot?.GetData().FromTableBson(_domainEntityTypeBuilder.Build(azureSnapshot.RowKey)) as Snapshot;
        }

        public async Task SaveSnapshot<TSnapshot>(TSnapshot snapshot) where TSnapshot : Snapshot
        {
            var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);
            await table.ExecuteAsync(TableOperation.InsertOrReplace(new AzureSnapshot(snapshot))).ConfigureAwait(false);
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