using System;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public static class BsonSerializationExtensions
    {
        public static byte[] ToBson(this object entity)
        {
            var ms = new MemoryStream();
            using(var writer = new BsonWriter(ms))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(writer, entity);
            }
            
            return ms.ToArray();
        }

        public static object FromBson(this byte[] bson, Type type)
        {
            var ms = new MemoryStream(bson);
            using (var reader = new BsonReader(ms))
            {
                var serializer = new JsonSerializer();
                return serializer.Deserialize(reader, type);
            }
        }

        public static T FromBson<T>(this byte[] bson)
        {
            return (T)FromBson(bson, typeof (T));
        }
    }
}