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
			protected static string ConnectionString = "UseDevelopmentStorage=true;";
			public string AzureStorageConnectionString => ConnectionString;
		}

		public class when_inserting_a_domainevent
		{
			protected static AppSettings appSettings;
			protected static IAzureTableService tableService;
			protected static string testTableName;
			protected static DomainEventWithData testDomainEvent;
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

				testDomainEvent = new DomainEventWithData { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 99, Id = Guid.NewGuid()};
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

            private It should_save_the_information_as_it_was_provided = () =>
            {
                var deserializedEvent = (DomainEventWithData)sut.GetEvents(testDomainEvent.AggregateRootId, 0).FirstOrDefault();
                deserializedEvent.ShouldNotBeNull();
                deserializedEvent.AggregateRootId.ShouldEqual(testDomainEvent.AggregateRootId);
                deserializedEvent.EventDate.ToString().ShouldEqual(testDomainEvent.EventDate.ToString());
                deserializedEvent.Sequence.ShouldEqual(testDomainEvent.Sequence);
                deserializedEvent.Id.ShouldEqual(testDomainEvent.Id);
            };

            private Cleanup after = () =>
			{
				var table = tableService.GetTable(testTableName, false);
				table.DeleteIfExists();
			};

		    protected class DomainEventWithData : DomainEvent
		    {
		        public Guid Id { get; set; }
		    }
			private static TestableAzureTableEventStore sut;
		}

	    public class when_inserting_multiple_domainevents
	    {
            protected static AppSettings appSettings;
            protected static IAzureTableService tableService;
            protected static string testTableName;
            protected static DomainEvent testDomainEvent;
            protected static DomainEvent testDomainEvent2;
            protected static IList<DomainEvent> domainEvents;
	        protected static int numEvents;

            private Establish context = () =>
            {
                appSettings = new AppSettings();
                testTableName = "TESTTABLE" + DateTime.Now.Millisecond.ToString(CultureInfo.InvariantCulture);
                tableService = new AzureTableService(appSettings);
                var table = tableService.GetTable(testTableName, false);
                table.DeleteIfExists();

                sut = new TestableAzureTableEventStore(tableService);
                sut.EventStoreTableName = testTableName;

                domainEvents = new List<DomainEvent>();
                numEvents = 10;
                for (int i = 0; i < numEvents; i++)
                {
                    domainEvents.Add(new DomainEvent()
                    {
                        AggregateRootId = Guid.NewGuid(),
                        EventDate = DateTime.Now,
                        Sequence = 99
                    });
                }
            };

            private Because of = () =>
            {
                sut.Insert(domainEvents);
            };

            private It should_have_all_events_in_table = () =>
            {
                var table = tableService.GetTable(testTableName);
                var query = new TableQuery<AzureTableEventStore.AzureDomainEvent>();
                var events = table.ExecuteQuery(query);
                events.Count().ShouldEqual(numEvents);
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

        public class when_querying_the_eventstore_for_event_types
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
                
                var domainEvents = new List<DomainEvent>()
                {
                    new DomainEvent1() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 99 },
                    new DomainEvent2() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 100 },
                    new DomainEvent3() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 101 },
                };
                sut.Insert(domainEvents);
            };

            private Because of = () =>
            {
                results = sut.GetEventsByEventTypes(new [] {typeof(DomainEvent1), typeof(DomainEvent3)});
            };

            private It should_pull_back_only_matching_event_types = () =>
            {
                results.Count().ShouldEqual(2);
                results.FirstOrDefault().Sequence.ShouldEqual(99);
                results.LastOrDefault().Sequence.ShouldEqual(101);
            };

            private Cleanup after = () =>
            {
                var table = tableService.GetTable(testTableName, false);
                table.DeleteIfExists();
            };

            private static TestableAzureTableEventStore sut;
        }

        public class when_querying_the_eventstore_for_event_types_by_aggregate_root_id
        {
            protected static AppSettings appSettings;
            protected static IAzureTableService tableService;
            protected static string testTableName;
            protected static DomainEvent testDomainEvent;
            protected static IEnumerable<DomainEvent> results;
            protected static Guid aggregateRootId;

            private Establish context = () =>
            {
                appSettings = new AppSettings();
                testTableName = "TESTTABLE" + DateTime.Now.Millisecond.ToString(CultureInfo.InvariantCulture);
                tableService = new AzureTableService(appSettings);
                var table = tableService.GetTable(testTableName, false);
                aggregateRootId = Guid.NewGuid();
                table.DeleteIfExists();

                sut = new TestableAzureTableEventStore(tableService);
                sut.EventStoreTableName = testTableName;

                var domainEvents = new List<DomainEvent>()
                {
                    new DomainEvent1() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 99 },
                    new DomainEvent2() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 100 },
                    new DomainEvent3() { AggregateRootId = Guid.NewGuid(), EventDate = DateTime.Now, Sequence = 101 },
                    new DomainEvent1() { AggregateRootId = aggregateRootId, EventDate = DateTime.Now, Sequence = 102 },
                    new DomainEvent2() { AggregateRootId = aggregateRootId, EventDate = DateTime.Now, Sequence = 103 },
                    new DomainEvent3() { AggregateRootId = aggregateRootId, EventDate = DateTime.Now, Sequence = 104 },
                };
                sut.Insert(domainEvents);
            };

            private Because of = () =>
            {
                results = sut.GetEventsByEventTypes(new[] { typeof(DomainEvent1), typeof(DomainEvent3) }, aggregateRootId);
            };

            private It should_pull_back_only_matching_event_types_and_id = () =>
            {
                results.Count().ShouldEqual(2);
                results.FirstOrDefault().Sequence.ShouldEqual(102);
                results.LastOrDefault().Sequence.ShouldEqual(104);
            };

            private Cleanup after = () =>
            {
                var table = tableService.GetTable(testTableName, false);
                table.DeleteIfExists();
            };

            private static TestableAzureTableEventStore sut;
        }

        public class when_querying_the_eventstore_for_event_types_by_date_range
        {
            protected static AppSettings appSettings;
            protected static IAzureTableService tableService;
            protected static string testTableName;
            protected static DomainEvent testDomainEvent;
            protected static IEnumerable<DomainEvent> results;
            protected static Guid aggregateRootId;

            private Establish context = () =>
            {
                appSettings = new AppSettings();
                testTableName = "TESTTABLE" + DateTime.Now.Millisecond.ToString(CultureInfo.InvariantCulture);
                tableService = new AzureTableService(appSettings);
                var table = tableService.GetTable(testTableName, false);
                table.DeleteIfExists();

                sut = new TestableAzureTableEventStore(tableService);
                sut.EventStoreTableName = testTableName;

                var domainEvents = new List<DomainEvent>()
                {
                    new DomainEvent1() { AggregateRootId = Guid.NewGuid(), EventDate = new DateTime(2016, 1, 5), Sequence = 99 },
                    new DomainEvent2() { AggregateRootId = Guid.NewGuid(), EventDate = new DateTime(2016, 1, 7), Sequence = 100 },
                    new DomainEvent3() { AggregateRootId = Guid.NewGuid(), EventDate = new DateTime(2016, 1, 9), Sequence = 101 },
                    new DomainEvent1() { AggregateRootId = Guid.NewGuid(), EventDate = new DateTime(2016, 1, 11), Sequence = 102 },
                    new DomainEvent2() { AggregateRootId = Guid.NewGuid(), EventDate = new DateTime(2016, 1, 13), Sequence = 103 },
                    new DomainEvent3() { AggregateRootId = Guid.NewGuid(), EventDate = new DateTime(2016, 1, 15), Sequence = 104 },
                };
                sut.Insert(domainEvents);
            };

            private Because of = () =>
            {
                results = sut.GetEventsByEventTypes(new[] { typeof(DomainEvent1), typeof(DomainEvent3) }, new DateTime(2016, 1, 6), new DateTime(2016, 1, 14));
            };

            private It should_pull_back_only_matching_event_types_and_date = () =>
            {
                results.Count().ShouldEqual(2);                
                results.Count(x => x.Sequence == 101).ShouldEqual(1);
                results.Count(x => x.Sequence == 102).ShouldEqual(1);                
            };

            private Cleanup after = () =>
            {
                var table = tableService.GetTable(testTableName, false);
                table.DeleteIfExists();
            };

            private static TestableAzureTableEventStore sut;
        }
    }

    public class DomainEvent1 : DomainEvent { }
    public class DomainEvent2 : DomainEvent { }
    public class DomainEvent3 : DomainEvent { }

	public class TestableAzureTableEventStore : AzureTableEventStore
	{
		public TestableAzureTableEventStore(IAzureTableService tableService) : base(tableService)
		{ }

		public string EventStoreTableName { get { return _eventStoreTableName; } set { _eventStoreTableName = value; } }
	}
}
