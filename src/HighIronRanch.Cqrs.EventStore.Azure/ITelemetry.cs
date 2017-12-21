using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ApplicationInsights;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    public interface ITelemetry
    {
        TelemetryClient GetClient();
    }
}
