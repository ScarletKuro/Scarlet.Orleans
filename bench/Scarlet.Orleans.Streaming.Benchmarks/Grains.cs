using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.Benchmarks;

public static class Providers
{
    public const string Scarlet = "scarlet";

    public const string InBox = "inbox";

    /// <summary>The stream namespace whose implicit consumer reads <paramref name="provider"/>.</summary>
    public static string Namespace(string provider) => provider + "-bench";
}

/// <summary>
/// The deliveries of the provider being measured: how many, and how long each event took from send to consumer. Events
/// carry their send time as a <see cref="Stopwatch"/> timestamp; the cluster runs in this process.
/// </summary>
public static class Deliveries
{
    private static ConcurrentQueue<double> latencies = new();

    public static int Count => latencies.Count;

    public static void Reset() => latencies = new();

    public static void Record(long sentAt) => latencies.Enqueue(Stopwatch.GetElapsedTime(sentAt).TotalMilliseconds);

    public static double[] Latencies() => [.. latencies.Order()];
}

public interface IConsumerGrain : IGrainWithGuidKey
{
    Task Ping();
}

public abstract class ConsumerGrain(string provider) : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(provider).GetStream<long>(StreamId.Create(Providers.Namespace(provider), this.GetPrimaryKey()))
            .SubscribeAsync((sentAt, _) =>
            {
                Deliveries.Record(sentAt);
                return Task.CompletedTask;
            });

    public Task Ping() => Task.CompletedTask;
}

[ImplicitStreamSubscription(Providers.Scarlet + "-bench")]
public sealed class ScarletConsumerGrain() : ConsumerGrain(Providers.Scarlet);

[ImplicitStreamSubscription(Providers.InBox + "-bench")]
public sealed class InBoxConsumerGrain() : ConsumerGrain(Providers.InBox);
