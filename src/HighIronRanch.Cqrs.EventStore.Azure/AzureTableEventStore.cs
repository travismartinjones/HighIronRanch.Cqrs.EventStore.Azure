using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using HighIronRanch.Azure.TableStorage;
using Microsoft.WindowsAzure.Storage.Table;
using SimpleCqrs.Eventing;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class AzureTableEventStore : IEventStore
    {
        public const string EVENT_STORE_TABLE_NAME = "Events";
        public const string SEQUENCE_FORMAT_STRING = "0000000000";

        protected readonly IAzureTableService _tableService;
        private readonly IDomainEntityTypeBuilder _domainEntityTypeBuilder;
        protected string _eventStoreTableName; // Used so it can be overridden for tests

        public AzureTableEventStore(
            IAzureTableService tableService,
            IDomainEntityTypeBuilder domainEntityTypeBuilder)
        {
            _tableService = tableService;
            _domainEntityTypeBuilder = domainEntityTypeBuilder;
            _eventStoreTableName = EVENT_STORE_TABLE_NAME;
        }

        /// <summary>This entity is basically a workaround the 64KB limitation
        /// for entity properties. 15 properties represents a total storage
        /// capability of 896KB (entity limit is at 1024KB).</summary>        
        /// <remarks>        
        /// This class is basically a hack against the Table Storage
        /// to work-around the 64KB limitation for properties.
        /// Idea adapted from the Locad Cloud Storage project.
        /// https://github.com/Lokad/lokad-cloud-storage/blob/master/Source/Lokad.Cloud.Storage/Azure/FatEntity.cs
        /// </remarks>
        public class AzureDomainEvent : BsonPayloadTableEntity
        {
            private const int EventDateInIsoFormatSize = 25;
            public DateTime EventDate { get; set; }
            public string EventType { get; set; }
            protected override int AdditionalPropertySizes => EventDateInIsoFormatSize + EventType.Length;

            public AzureDomainEvent() { }

            public AzureDomainEvent(DomainEvent evt)
            {
                PartitionKey = evt.AggregateRootId.ToString();
                RowKey = evt.Sequence.ToString(SEQUENCE_FORMAT_STRING);
                EventDate = evt.EventDate.ToUniversalTime();
                EventType = evt.GetType().FullName;

                var domainEventData = evt.ToBson();

                if (domainEventData.Length > MaxByteCapacity)
                {
                    throw new ArgumentException($"Event size of {domainEventData.Length} when stored as json exceeds Azure property limit of 960K");
                }

                SetData(domainEventData);
            }
        }

        public async Task<IEnumerable<DomainEvent>> GetEvents(Guid aggregateRootId, int startSequence)
        {
            var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);

            var query = new TableQuery<AzureDomainEvent>()
                .Where(TableQuery
                    .CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, aggregateRootId.ToString()),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, startSequence.ToString(SEQUENCE_FORMAT_STRING))
                    )
                );

            var results = new List<AzureDomainEvent>();
            TableContinuationToken continuationToken = null;
            do
            {
                var result = await table.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                if (result.Results?.Count > 0)
                {
                    results.AddRange(result.Results);
                }

                continuationToken = result.ContinuationToken;
            } while (continuationToken != null);

            var ret = ConvertToDomainEvent(results);
            return ret;
        }

        private IEnumerable<DomainEvent> ConvertToDomainEvent(IEnumerable<AzureDomainEvent> events)
        {
            return events.Select(entity => entity.GetData().FromBson(_domainEntityTypeBuilder.Build(entity.EventType)) as DomainEvent);
        }

        public async Task Insert(IEnumerable<DomainEvent> domainEvents)
        {
            var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);

            var batchOperation = new TableBatchOperation();
            var batchCount = 0;
            var batchSize = 0L;
            var currentAggregateRootId = Guid.Empty;

            var sortedEvents = domainEvents
                                    // sort by the aggregate root id since it is going to be the partition key
                                    // you can't batch across parititions, so the most optimal batches are via 
                                    // grouped aggregates
                                    .OrderBy(de => de.AggregateRootId)
                                    .ThenBy(de => de.EventDate);

            foreach (var domainEvent in sortedEvents)
            {
                // Azure batches to chunk of 100, or under 4MB, and by the patition key (aggregate root id)
                if (batchCount >= 100 ||
                    batchSize >= 3900000 || // give ~10% buffer
                    (currentAggregateRootId != domainEvent.AggregateRootId && batchCount > 0))
                {
                    await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
                    batchOperation = new TableBatchOperation();
                    batchCount = 0;
                    batchSize = 0;
                }

                var azureDomainEvent = new AzureDomainEvent(domainEvent);

                batchSize += azureDomainEvent.EstimatedSize;
                batchOperation.Insert(azureDomainEvent);
                currentAggregateRootId = domainEvent.AggregateRootId;
                batchCount++;
            }

            if (batchCount > 0)
                await table.ExecuteBatchAsync(batchOperation).ConfigureAwait(false);
        }

        public async Task<IEnumerable<DomainEvent>> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes)
        {
            var events = new List<DomainEvent>();
            foreach (var domainEventType in domainEventTypes)
            {
                var jsonDomainEventType = domainEventType.FullName;
                var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);

                var query = new TableQuery<AzureDomainEvent>()
                    .Where(TableQuery.GenerateFilterCondition("EventType", QueryComparisons.Equal, jsonDomainEventType));

                var domainEvents = new List<AzureDomainEvent>();
                TableContinuationToken continuationToken = null;
                do
                {
                    var result = await table.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                    if (result.Results?.Count > 0)
                    {
                        domainEvents.AddRange(result.Results);
                    }

                    continuationToken = result.ContinuationToken;
                } while (continuationToken != null);
                
                events.AddRange(ConvertToDomainEvent(domainEvents));
            }

            return events;
        }

        public async Task<IEnumerable<DomainEvent>> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, Guid aggregateRootId)
        {
            var partitionKey = aggregateRootId.ToString();
            var events = new List<DomainEvent>();
            foreach (var domainEventType in domainEventTypes)
            {
                var jsonDomainEventType = domainEventType.FullName;
                var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);

                var query = new TableQuery<AzureDomainEvent>()
                    .Where(
                        TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("EventType", QueryComparisons.Equal, jsonDomainEventType))
                    );

                var domainEvents = new List<AzureDomainEvent>();
                TableContinuationToken continuationToken = null;
                do
                {
                    var result = await table.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                    if (result.Results?.Count > 0)
                    {
                        domainEvents.AddRange(result.Results);
                    }

                    continuationToken = result.ContinuationToken;
                } while (continuationToken != null);
                
                events.AddRange(ConvertToDomainEvent(domainEvents));
            }

            return events;
        }

        public async Task<IEnumerable<DomainEvent>> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, DateTime startDate, DateTime endDate)
        {
            var events = new List<DomainEvent>();
            foreach (var domainEventType in domainEventTypes)
            {
                var jsonDomainEventType = domainEventType.FullName;
                var table = await _tableService.GetTable(_eventStoreTableName, false).ConfigureAwait(false);

                var query = new TableQuery<AzureDomainEvent>()
                    .Where(
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("EventType", QueryComparisons.Equal, jsonDomainEventType),
                            TableOperators.And,
                            TableQuery.CombineFilters(
                                TableQuery.GenerateFilterCondition("EventDate", QueryComparisons.GreaterThanOrEqual, startDate.ToString("O")),
                                TableOperators.And,
                                TableQuery.GenerateFilterCondition("EventDate", QueryComparisons.LessThanOrEqual, endDate.ToString("O"))
                            ))
                    );

                var domainEvents = new List<AzureDomainEvent>();
                TableContinuationToken continuationToken = null;
                do
                {
                    var result = await table.ExecuteQuerySegmentedAsync(query, continuationToken).ConfigureAwait(false);
                    if (result.Results?.Count > 0)
                    {
                        domainEvents.AddRange(result.Results);
                    }

                    continuationToken = result.ContinuationToken;
                } while (continuationToken != null);
                
                events.AddRange(ConvertToDomainEvent(domainEvents));
            }

            return events;
        }
    }
}
