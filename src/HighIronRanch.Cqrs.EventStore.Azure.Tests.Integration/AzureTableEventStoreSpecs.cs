using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using HighIronRanch.Azure;
using HighIronRanch.Azure.TableStorage;
using HighIronRanch.Cqrs.EventStore.Azure;
using Machine.Specifications;
using Microsoft.WindowsAzure.Storage.Table;
using SimpleCqrs.Eventing;

namespace HighIronRanch.Cqrs.Azure.Tests.Integration
{
	[Subject(typeof(AzureTableEventStore))]
	public class AzureTableEventStoreSpecs
	{
		public class AppSettings : IAzureTableSettings
		{
			protected static string connectionString = "UseDevelopmentStorage=true;";
			public string AzureStorageConnectionString { get { return connectionString; } }
		}

		public class when_inserting_a_domainevent
		{
			protected static AppSettings appSettings;
			protected static IAzureTableService tableService;
			protected static string testTableName;
			protected static DomainEvent testDomainEvent;
			protected static IEnumerable<DomainEvent> domainEvents;

			private Establish context = () =>
			{
				appSettings = new AppSettings();
				testTableName = "TESTTABLE" + DateTime.Now.Millisecond.ToString(CultureInfo.InvariantCulture);
				tableService = new AzureTableService(appSettings);
				var table = tableService.GetTable(testTableName, false);
				table.DeleteIfExists();

				sut = new TestableAzureTableEventStore(tableService);
				sut.EventStoreTableName = testTableName;

				testDomainEvent = new DomainEvent() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 99 };
				domainEvents = new List<DomainEvent>() { testDomainEvent };
			};

			private Because of = () =>
			{
				sut.Insert(domainEvents);
			};

			private It should_have_one_event_in_table = () =>
			{
				var table = tableService.GetTable(testTableName);
				var query = new TableQuery<AzureTableEventStore.AzureDomainEvent>();
				var events = table.ExecuteQuery(query);
				events.Count().ShouldEqual(1);
			};

			private Cleanup after = () =>
			{
				var table = tableService.GetTable(testTableName, false);
				table.DeleteIfExists();
			};

			private static TestableAzureTableEventStore sut;
		}

		public class when_querying_the_eventstore
		{
			protected static AppSettings appSettings;
			protected static IAzureTableService tableService;
			protected static string testTableName;
			protected static DomainEvent testDomainEvent;
			protected static IEnumerable<DomainEvent> results;

			private Establish context = () =>
			{
				appSettings = new AppSettings();
				testTableName = "TESTTABLE" + DateTime.Now.Millisecond.ToString(CultureInfo.InvariantCulture);
				tableService = new AzureTableService(appSettings);
				var table = tableService.GetTable(testTableName, false);
				table.DeleteIfExists();

				sut = new TestableAzureTableEventStore(tableService);
				sut.EventStoreTableName = testTableName;

				testDomainEvent = new DomainEvent() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 99 };
				var domainEvents = new List<DomainEvent>() { testDomainEvent };
				sut.Insert(domainEvents);
			};

			private Because of = () =>
			{
				results = sut.GetEvents(testDomainEvent.AggregateRootId, 0);
			};

			private It should_have_one_event_in_table = () =>
			{
				results.Count().ShouldEqual(1);
				results.FirstOrDefault().AggregateRootId.ShouldEqual(testDomainEvent.AggregateRootId);
			};

			private Cleanup after = () =>
			{
				var table = tableService.GetTable(testTableName, false);
				table.DeleteIfExists();
			};

			private static TestableAzureTableEventStore sut;
		}
	}

	public class TestableAzureTableEventStore : AzureTableEventStore
	{
		public TestableAzureTableEventStore(IAzureTableService tableService) : base(tableService)
		{ }

		public string EventStoreTableName { get { return _eventStoreTableName; } set { _eventStoreTableName = value; } }
	}
}
