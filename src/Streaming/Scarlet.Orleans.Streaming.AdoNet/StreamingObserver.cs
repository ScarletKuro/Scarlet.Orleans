using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// Traces and measures the queue operations of one stream provider, on a silo or a client.
/// </summary>
/// <remarks>
/// Instruments are tagged with <c>orleans.stream.provider</c>. Failures propagate to Orleans, which logs them; the
/// receiver logs what it handles itself.
/// </remarks>
internal sealed class StreamingObserver(string provider, TimeProvider timeProvider)
{
    private const string ProviderTag = "orleans.stream.provider";

    private static readonly Histogram<double> Duration = Diagnostics.Meter.CreateHistogram<double>(
        "scarlet.orleans.streaming.duration",
        unit: "s",
        description: "The duration of stream queue operations, by provider, operation and outcome.");

    private static readonly Counter<long> Messages = Diagnostics.Meter.CreateCounter<long>(
        "scarlet.orleans.streaming.messages",
        unit: "{message}",
        description: "Stream messages by provider and change: enqueued, dequeued, redelivered, confirmed, released, retried, stale, dead_lettered and removed.");

    private static readonly Histogram<long> PayloadSize = Diagnostics.Meter.CreateHistogram<long>(
        "scarlet.orleans.streaming.payload.size",
        unit: "By",
        description: "The serialized bytes of each enqueued batch, by provider.");

    /// <summary>The factories of this process, whose receivers' held messages the gauge reports; a stopped silo's go.</summary>
    private static readonly ConditionalWeakTable<RelationalQueueAdapterFactory, string> Factories = [];

    private static readonly ObservableGauge<int> Held = Diagnostics.Meter.CreateObservableGauge(
        "scarlet.orleans.streaming.messages.held",
        static () => Factories.Select(static f => new Measurement<int>(f.Key.HeldMessages, new KeyValuePair<string, object?>(ProviderTag, f.Value))),
        unit: "{message}",
        description: "Messages this process's receivers hold, dequeued and not yet confirmed or released, by provider.");

    public string Provider => provider;

    public static void Track(RelationalQueueAdapterFactory factory, string provider) => Factories.AddOrUpdate(factory, provider);

    public void Count(string change, long count)
    {
        if (count > 0)
        {
            Messages.Add(count, new KeyValuePair<string, object?>(ProviderTag, provider), new KeyValuePair<string, object?>("change", change));
        }
    }

    public void RecordPayload(long bytes) => PayloadSize.Record(bytes, new KeyValuePair<string, object?>(ProviderTag, provider));

    /// <summary>Runs one queue operation in a trace span, and records its duration and outcome.</summary>
    public async Task<T> ObserveAsync<T>(string operation, StreamQueueKey queue, Func<Task<T>> body)
    {
        using var activity = Diagnostics.ActivitySource.StartActivity($"streaming {operation}");
        activity?.SetTag(ProviderTag, provider);
        activity?.SetTag("orleans.stream.operation", operation);
        activity?.SetTag("orleans.stream.queue", queue.QueueId);

        var started = timeProvider.GetTimestamp();
        var outcome = "error";
        try
        {
            var result = await body().ConfigureAwait(false);
            outcome = "success";
            return result;
        }
        finally
        {
            Duration.Record(
                timeProvider.GetElapsedTime(started).TotalSeconds,
                new KeyValuePair<string, object?>(ProviderTag, provider),
                new KeyValuePair<string, object?>("orleans.stream.operation", operation),
                new KeyValuePair<string, object?>("outcome", outcome));
            if (outcome != "success")
            {
                activity?.SetStatus(ActivityStatusCode.Error);
            }
        }
    }

    public async Task ObserveAsync(string operation, StreamQueueKey queue, Func<Task> body) =>
        await ObserveAsync(operation, queue, async () =>
        {
            await body().ConfigureAwait(false);
            return true;
        }).ConfigureAwait(false);
}
