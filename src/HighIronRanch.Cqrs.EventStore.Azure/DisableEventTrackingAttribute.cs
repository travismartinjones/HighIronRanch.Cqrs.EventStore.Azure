using System;

namespace HighIronRanch.Cqrs.EventStore.Azure
{
    /// <summary>
    /// Add this attribute to disable tracking of the event to ensure that the bus event is correctly published.
    /// This is suggested for high volume events where a missed bus event is not detrimental.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class DisableEventTrackingAttribute : Attribute
    {

    }
}