using System;
using Microsoft.Azure.Cosmos.Table;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    [Serializable]
    public class ProcessingEvent : TableEntity
    {
        public static string EventDateFormat = "yyyy-MM-ddTHH:mm:ssK";

        [IgnoreProperty]
        public System.DateTime EventDate {
            get => System.DateTime.Parse(PartitionKey);
            set => PartitionKey = value.ToString(EventDateFormat);
        }
        [IgnoreProperty]
        public Guid AggregateRootId { 
            get => new Guid(RowKey);
            set => RowKey = value.ToString();
        }
        public int Sequence { get; set; }
    }
}