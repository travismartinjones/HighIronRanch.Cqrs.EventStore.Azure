using System;
using Newtonsoft.Json;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public static class Json
    {
        private static JsonSerializerSettings Settings => new JsonSerializerSettings
        {
            PreserveReferencesHandling = PreserveReferencesHandling.Objects,
            ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };
        public static string ToJson(this object obj)
        {
            return JsonConvert.SerializeObject(obj, Formatting.None, Settings);
        }

        public static object FromJson(this string json, Type type)
        {
            return JsonConvert.DeserializeObject(json, type, Settings);
        }
    }
}