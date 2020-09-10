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
    public class TableStorageEventTrackingService : IEventTrackingService
    {
        private readonly IAzureTableService _tableService;
        private readonly IEventStore _eventStore;
        private readonly IUtcTimeService _utcTimeService;
        public const string EventTrackingTableName = "PendingEvents";
        private const int SecondsToConsiderOld = 60;
        private DateTime? _previousGetHungDate;

        public TableStorageEventTrackingService(
            IAzureTableService tableService,
            IEventStore eventStore,
            IUtcTimeService utcTimeService
        )
        {
            _tableService = tableService;
            _eventStore = eventStore;
            _utcTimeService = utcTimeService;
        }

        public async Task StartTracking(IEnumerable<DomainEvent> events)
        {
            var evts = events
                .Where(x => !x.GetType().IsDefined(typeof(DisableEventTrackingAttribute),false)) // don't track events with the DisableEventTrackingAttribute
                .ToList();
            if (evts.Count == 0) return;

            var table = await _tableService.GetTable(EventTrackingTableName, false).ConfigureAwait(false);

            foreach (var item in evts)
            {
                var processingEvent = new ProcessingEvent
                {
                    EventDate = item.EventDate,
                    AggregateRootId = item.AggregateRootId,
                    Sequence = item.Sequence
                };
                await table.ExecuteAsync(TableOperation.InsertOrReplace(processingEvent)).ConfigureAwait(false);
            }
        }

        public async Task<IEnumerable<DomainEvent>> GetHungEvents()
        {
            var now = _utcTimeService.UtcNow;
            _previousGetHungDate = now;
            var query = new TableQuery<ProcessingEvent>()
                .Where(TableQuery.GenerateFilterCondition(nameof(ProcessingEvent.PartitionKey), QueryComparisons.LessThan, now.AddSeconds(-SecondsToConsiderOld).ToString(ProcessingEvent.EventDateFormat)));

            var table = await _tableService.GetTable(EventTrackingTableName, false).ConfigureAwait(false);
            var existingItems = (await table.ExecuteQueryAsync(query).ConfigureAwait(false)).ToList();

            if (existingItems.Count == 0) return new List<DomainEvent>(0);

            var hungEvents = new List<DomainEvent>();

            foreach (var existingItem in existingItems)
            {
                var evt = await _eventStore.GetEvent(existingItem.AggregateRootId, existingItem.Sequence).ConfigureAwait(false);
                if(evt != null)
                    hungEvents.Add(evt);
            }

            return hungEvents;
        }

        public async Task ClearHungEvents()
        {
            var query = new TableQuery<ProcessingEvent>()
                .Where(TableQuery.GenerateFilterCondition(nameof(ProcessingEvent.PartitionKey), QueryComparisons.LessThan, 
                    _previousGetHungDate.GetValueOrDefault(_utcTimeService.UtcNow)
                        .AddSeconds(-SecondsToConsiderOld).ToString(ProcessingEvent.EventDateFormat)));

            var table = await _tableService.GetTable(EventTrackingTableName, false).ConfigureAwait(false);
            var existingItems = (await table.ExecuteQueryAsync(query).ConfigureAwait(false)).ToList();

            foreach (var item in existingItems)
            {
                await table.ExecuteAsync(TableOperation.Delete(item)).ConfigureAwait(false);
            }
        }

        public async Task StopTracking(IEnumerable<DomainEvent> events)
        {
            var evts = events
                .Where(x => !x.GetType().IsDefined(typeof(DisableEventTrackingAttribute),false)) // don't track events with the DisableEventTrackingAttribute
                .ToList();
            if (evts.Count == 0) return;

            var table = await _tableService.GetTable(EventTrackingTableName, false).ConfigureAwait(false);

            foreach (var item in evts)
            {
                var existingItem = await GetEvent(item).ConfigureAwait(false);
                if (existingItem != null)
                {
                    await table.ExecuteAsync(TableOperation.Delete(existingItem)).ConfigureAwait(false);
                }
            }
        }

        private async Task<ProcessingEvent> GetEvent(DomainEvent evt)
        {
            var query = new TableQuery<ProcessingEvent>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(nameof(ProcessingEvent.PartitionKey), QueryComparisons.Equal, evt.EventDate.ToUniversalTime().ToString(ProcessingEvent.EventDateFormat))
                        , TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition(nameof(ProcessingEvent.RowKey), QueryComparisons.Equal, evt.AggregateRootId.ToString()),
                            TableOperators.And,
                            TableQuery.GenerateFilterConditionForInt(nameof(ProcessingEvent.Sequence), QueryComparisons.Equal, evt.Sequence)
                        )
                    ));

            var table = await _tableService.GetTable(EventTrackingTableName, false).ConfigureAwait(false);
            var existingItems = (await table.ExecuteQueryAsync(query).ConfigureAwait(false)).ToList();
            return existingItems.FirstOrDefault();
        }
    }
}