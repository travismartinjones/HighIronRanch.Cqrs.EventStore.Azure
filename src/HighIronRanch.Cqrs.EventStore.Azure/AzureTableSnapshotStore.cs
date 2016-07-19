using System;
using System.Linq;
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

        public Snapshot GetSnapshot(Guid aggregateRootId)
        {
            var table = _tableService.GetTable(_eventStoreTableName);

            var query = new TableQuery<AzureSnapshot>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal,aggregateRootId.ToString()));

            var azureSnapshot = table.ExecuteQuery(query).FirstOrDefault();

            return azureSnapshot?.GetData().FromBson(_domainEntityTypeBuilder.Build(azureSnapshot.RowKey)) as Snapshot;
        }

        public void SaveSnapshot<TSnapshot>(TSnapshot snapshot) where TSnapshot : Snapshot
        {
            var table = _tableService.GetTable(_eventStoreTableName);            
            table.Execute(TableOperation.InsertOrReplace(new AzureSnapshot(snapshot)));            
        }

        public class AzureSnapshot : BsonPayloadTableEntity
        {            
            protected override int AdditionalPropertySizes => 0;

            public AzureSnapshot() { }

            public AzureSnapshot(Snapshot snapshot)
            {
                PartitionKey = snapshot.AggregateRootId.ToString();
                RowKey = snapshot.GetType().FullName;                

                var snapshotData = snapshot.ToBson();

                if (snapshotData.Length > MaxByteCapacity)
                {
                    throw new ArgumentException($"Snapshot size of {snapshotData.Length} when stored as json exceeds Azure property limit of 960K");
                }

                SetData(snapshotData);
            }
        }
    }
}