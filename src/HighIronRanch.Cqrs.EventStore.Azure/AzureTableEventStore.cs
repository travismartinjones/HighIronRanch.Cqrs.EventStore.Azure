using System;
using System.Collections.Generic;
using System.Linq;
using HighIronRanch.Azure;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using SimpleCqrs.Eventing;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
	public class AzureTableEventStore : IEventStore
	{
		public const string EVENT_STORE_TABLE_NAME = "Events";
		public const string SEQUENCE_FORMAT_STRING = "0000000000";

		protected readonly IAzureTableService _tableService;
		protected string _eventStoreTableName; // Used so it can be overridden for tests

		public AzureTableEventStore(IAzureTableService tableService)
		{
			_tableService = tableService;
			_eventStoreTableName = EVENT_STORE_TABLE_NAME;
		}

		public class AzureDomainEvent : TableEntity
		{
			public string DomainEventAsJson { get; set; }
			public string DomainEventTypeAsJson { get; set; }

			public AzureDomainEvent() { }

			public AzureDomainEvent(DomainEvent evt)
			{
				PartitionKey = evt.AggregateRootId.ToString();
				RowKey = evt.Sequence.ToString(SEQUENCE_FORMAT_STRING);
				DomainEventAsJson = JsonConvert.SerializeObject(evt);
				DomainEventTypeAsJson = JsonConvert.SerializeObject(evt.GetType());
			}
		}

		public IEnumerable<DomainEvent> GetEvents(Guid aggregateRootId, int startSequence)
		{
			var table = _tableService.GetTable(_eventStoreTableName);

			var query = new TableQuery<AzureDomainEvent>()
				.Where(TableQuery
					.CombineFilters(
						TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, aggregateRootId.ToString()),
						TableOperators.And,
						TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, startSequence.ToString(SEQUENCE_FORMAT_STRING))
					)
				);

			var entities = table.ExecuteQuery(query);

			var list = new List<DomainEvent>();
			foreach (var entity in entities)
			{
				var type = JsonConvert.DeserializeObject<Type>(entity.DomainEventTypeAsJson);
				var evt = JsonConvert.DeserializeObject(entity.DomainEventAsJson, type) as DomainEvent;
				list.Add(evt);
			}

			return list;
		}

		public void Insert(IEnumerable<DomainEvent> domainEvents)
		{
			var table = _tableService.GetTable(_eventStoreTableName);

			var batchOperation = new TableBatchOperation();
			int i = 0;
			foreach (var domainEvent in domainEvents)
			{
				if (i >= 100)
				{
					table.ExecuteBatch(batchOperation);
					i = 0;
				}
				batchOperation.Insert(new AzureDomainEvent(domainEvent));
				i++;
			}
			if(i > 0)
				table.ExecuteBatch(batchOperation);
		}

		public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, Guid aggregateRootId)
		{
			throw new NotImplementedException();
		}

		public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, DateTime startDate, DateTime endDate)
		{
			throw new NotImplementedException();
		}
	}
}
