using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public class Telemetry : ITelemetry
    {
        private readonly TelemetryClient _client;
        public Telemetry()
        {
            _client = new TelemetryClient();
        }
        public TelemetryClient GetClient()
        {
            return _client;
        }
    }
}
