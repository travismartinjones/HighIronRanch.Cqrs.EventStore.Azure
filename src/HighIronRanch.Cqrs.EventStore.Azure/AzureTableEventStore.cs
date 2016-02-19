using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using HighIronRanch.Azure.TableStorage;
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

        /// <summary>This entity is basically a workaround the 64KB limitation
        /// for entity properties. 15 properties represents a total storage
        /// capability of 896KB (entity limit is at 1024KB).</summary>        
        /// <remarks>        
        /// This class is basically a hack against the Table Storage
        /// to work-around the 64KB limitation for properties.
        /// Idea adapted from the Locad Cloud Storage project.
        /// https://github.com/Lokad/lokad-cloud-storage/blob/master/Source/Lokad.Cloud.Storage/Azure/FatEntity.cs
        /// </remarks>
        public class AzureDomainEvent : TableEntity
        {            
            /// <summary>
            /// Maximal entity size is 1MB. Out of that, we keep only
            /// 896kb (1MB - 64kb as a safety margin). Then, it should be taken
            /// into account that byte[] are Base64 encoded which represent
            /// a penalty overhead of 4/3 - hence the reduced capacity.
            /// </summary>
            public const int MaxByteCapacity = (896 * 1024 * 3) / 4;

            public DateTime EventDate { get; set; }
            public string EventType { get; set; }
            public byte[] P0 { get; set; }
            public byte[] P1 { get; set; }
            public byte[] P2 { get; set; }
            public byte[] P3 { get; set; }
            public byte[] P4 { get; set; }
            public byte[] P5 { get; set; }
            public byte[] P6 { get; set; }
            public byte[] P7 { get; set; }
            public byte[] P8 { get; set; }
            public byte[] P9 { get; set; }
            public byte[] P10 { get; set; }
            public byte[] P11 { get; set; }
            public byte[] P12 { get; set; }
            public byte[] P13 { get; set; }            

            IEnumerable<byte[]> GetProperties()
            {
                if (null != P0) yield return P0;
                if (null != P1) yield return P1;
                if (null != P2) yield return P2;
                if (null != P3) yield return P3;
                if (null != P4) yield return P4;
                if (null != P5) yield return P5;
                if (null != P6) yield return P6;
                if (null != P7) yield return P7;
                if (null != P8) yield return P8;
                if (null != P9) yield return P9;
                if (null != P10) yield return P10;
                if (null != P11) yield return P11;
                if (null != P12) yield return P12;
                if (null != P13) yield return P13;
            }            

            public AzureDomainEvent() { }

            public AzureDomainEvent(DomainEvent evt)
            {
                PartitionKey = evt.AggregateRootId.ToString();
                RowKey = evt.Sequence.ToString(SEQUENCE_FORMAT_STRING);
                EventDate = evt.EventDate;
                EventType = JsonConvert.SerializeObject(evt.GetType().FullName);
              
                var domainEventJson = JsonConvert.SerializeObject(evt);

                var domainEventData = Encoding.UTF8.GetBytes(domainEventJson);
                
                if (domainEventData.Length > MaxByteCapacity)
                {
                    throw new ArgumentException($"Event size of {domainEventData.Length} when stored as json exceeds Azure property limit of 960K");
                }

                SetData(domainEventData);
            }

            /// <summary>Returns the concatenated stream contained in the fat entity.</summary>
            public byte[] GetData()
            {
                var arrays = GetProperties().ToArray();
                var buffer = new byte[arrays.Sum(a => a.Length)];

                var i = 0;
                foreach (var array in arrays)
                {
                    Buffer.BlockCopy(array, 0, buffer, i, array.Length);
                    i += array.Length;
                }

                return buffer;
            }

            /// <summary>Split the stream as a fat entity.</summary>
            public void SetData(byte[] data)
            {
                if (null == data) throw new ArgumentNullException(nameof(data));
                if (data.Length >= MaxByteCapacity) throw new ArgumentOutOfRangeException(nameof(data));

                var setters = new Action<byte[]>[]
                    {
                    b => P0 = b,
                    b => P1 = b,
                    b => P2 = b,
                    b => P3 = b,
                    b => P4 = b,
                    b => P5 = b,
                    b => P6 = b,
                    b => P7 = b,
                    b => P8 = b,
                    b => P9 = b,
                    b => P10 = b,
                    b => P11 = b,
                    b => P12 = b,
                    b => P13 = b,
                    };

                for (var i = 0; i < 14; i++)
                {
                    if (i * 64 * 1024 < data.Length)
                    {
                        var start = i * 64 * 1024;
                        var length = Math.Min(64 * 1024, data.Length - start);
                        var buffer = new byte[length];

                        Buffer.BlockCopy(data, start, buffer, 0, buffer.Length);
                        setters[i](buffer);
                    }
                    else
                    {
                        setters[i](null); // discarding potential leftover
                    }
                }
            }

            public long EstimatedSize => PartitionKey.Length +
                                         RowKey.Length +
                                         25 + // EventDate in ISO format
                                         EventType.Length +
                                         (GetProperties().Sum(a => a.Length) * 4 + 3) / 3;
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

            return ConvertToDomainEvent(table.ExecuteQuery(query));            
        }

        private IEnumerable<DomainEvent> ConvertToDomainEvent(IEnumerable<AzureDomainEvent> events)
        {
            var list = new List<DomainEvent>();
            foreach (var entity in events)
            {
                using (var stream = new MemoryStream(entity.GetData()) { Position = 0 })
                {
                    var val = JsonConvert.DeserializeObject<DomainEvent>(Encoding.UTF8.GetString(stream.ToArray()));
                    list.Add(val);
                }
            }

            return list;
        }

        public void Insert(IEnumerable<DomainEvent> domainEvents)
        {
            var table = _tableService.GetTable(_eventStoreTableName);

            var batchOperation = new TableBatchOperation();
            int batchCount = 0;
            long batchSize = 0;
            var currentAggregateRootId = Guid.Empty;

            foreach (var domainEvent in domainEvents.OrderBy(de => de.EventDate))
            {
                // Azure batches limited to 100 and 4MB
                if (batchCount >= 1 ||
                    batchSize >= 3900000 || // give ~10% buffer
                    (currentAggregateRootId != domainEvent.AggregateRootId && batchCount > 0))
                {
                    table.ExecuteBatch(batchOperation);
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
                table.ExecuteBatch(batchOperation);
        }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes)
        {
            return domainEventTypes.SelectMany(x =>
            {
                var jsonDomainEventType = JsonConvert.SerializeObject(x.FullName);
                var domainEvents =_tableService.GetTable(_eventStoreTableName)
                    .CreateQuery<AzureDomainEvent>()
                    .Where(ade => ade.EventType == jsonDomainEventType);
                return ConvertToDomainEvent(domainEvents);
            });
        }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, Guid aggregateRootId)
        {
            return domainEventTypes.SelectMany(x =>
            {
                var partitionKey = aggregateRootId.ToString();
                var jsonDomainEventType = JsonConvert.SerializeObject(x.FullName);
                var domainEvents = _tableService.GetTable(_eventStoreTableName)
                    .CreateQuery<AzureDomainEvent>()
                    .Where(ade => ade.PartitionKey == partitionKey && ade.EventType == jsonDomainEventType);
                return ConvertToDomainEvent(domainEvents);
            });
        }

        public IEnumerable<DomainEvent> GetEventsByEventTypes(IEnumerable<Type> domainEventTypes, DateTime startDate, DateTime endDate)
        {
            return domainEventTypes.SelectMany(x =>
            {
                var jsonDomainEventType = JsonConvert.SerializeObject(x.FullName);
                var domainEvents = _tableService.GetTable(_eventStoreTableName)
                    .CreateQuery<AzureDomainEvent>()
                    .Where(ade => ade.EventType == jsonDomainEventType && ade.EventDate >= startDate && ade.EventDate <= endDate);
                return ConvertToDomainEvent(domainEvents);
            });
        }
    }
}
