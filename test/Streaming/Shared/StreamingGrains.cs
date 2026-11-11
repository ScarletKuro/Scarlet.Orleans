using System.Collections.Concurrent;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.Tests;

public static class StreamNamespaces
{
    public const string Provider = "Scarlet";

    public const string Implicit = "implicit";

    public const string Explicit = "explicit";

    public const string Failing = "failing";

    public const string Slow = "slow";
}

/// <summary>
/// What the consumer grains received, per stream. The cluster runs in the test's process, so the log outlives a grain
/// that a stopped silo moves elsewhere.
/// </summary>
public static class DeliveryLog
{
    private static readonly ConcurrentDictionary<Guid, ConcurrentQueue<int>> Received = new();

    private static readonly ConcurrentDictionary<(Guid, int), int> Attempts = new();

    public static void Record(Guid stream, int item) => Received.GetOrAdd(stream, static _ => new()).Enqueue(item);

    /// <summary>Counts an attempt to deliver <paramref name="item"/>, returning how many there have been.</summary>
    public static int Attempt(Guid stream, int item) => Attempts.AddOrUpdate((stream, item), 1, static (_, n) => n + 1);

    public static int AttemptsOf(Guid stream, int item) => Attempts.TryGetValue((stream, item), out var n) ? n : 0;

    public static int[] Of(Guid stream) => Received.TryGetValue(stream, out var items) ? [.. items] : [];

    /// <summary>Waits until <paramref name="stream"/> received every item of <paramref name="expected"/>.</summary>
    public static async Task WaitForAsync(Guid stream, IEnumerable<int> expected, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var missing = expected.ToHashSet();
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(timeout);
        try
        {
            while (!missing.IsSubsetOf(Of(stream)))
            {
                await Task.Delay(50, deadline.Token);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            missing.ExceptWith(Of(stream));
            throw new TimeoutException($"Stream {stream} lacked {missing.Count} items after {timeout}: {string.Join(", ", missing.Order().Take(20))}.");
        }
    }
}

public interface IConsumerGrain : IGrainWithGuidKey
{
    Task Subscribe();
}

public interface IProducerGrain : IGrainWithGuidKey
{
    Task Produce(string streamNamespace, Guid stream, int[] items);

    Task ProduceBatch(string streamNamespace, Guid stream, int[] items);
}

/// <summary>Records every item of its implicitly subscribed stream.</summary>
[ImplicitStreamSubscription(StreamNamespaces.Implicit)]
public sealed class ImplicitConsumerGrain : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Implicit, this.GetPrimaryKey()))
            .SubscribeAsync((item, _) => Record(item));

    public Task Subscribe() => Task.CompletedTask;

    private Task Record(int item)
    {
        DeliveryLog.Record(this.GetPrimaryKey(), item);
        return Task.CompletedTask;
    }
}

/// <summary>Fails the first delivery of every negative item, and records every item it accepts.</summary>
[ImplicitStreamSubscription(StreamNamespaces.Failing)]
public sealed class FailingConsumerGrain : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Failing, this.GetPrimaryKey()))
            .SubscribeAsync((item, _) =>
            {
                if (item < 0 && DeliveryLog.Attempt(this.GetPrimaryKey(), item) == 1)
                {
                    throw new InvalidOperationException($"Failing the first delivery of {item}.");
                }

                DeliveryLog.Record(this.GetPrimaryKey(), item);
                return Task.CompletedTask;
            });

    public Task Subscribe() => Task.CompletedTask;
}

/// <summary>Takes a while over every item, so a silo stopping holds messages that are cached but not yet delivered.</summary>
[ImplicitStreamSubscription(StreamNamespaces.Slow)]
public sealed class SlowConsumerGrain : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Slow, this.GetPrimaryKey()))
            .SubscribeAsync(async (item, _) =>
            {
                await Task.Delay(100);
                DeliveryLog.Record(this.GetPrimaryKey(), item);
            });

    public Task Subscribe() => Task.CompletedTask;
}

/// <summary>Subscribes explicitly, through pub-sub, when asked.</summary>
public sealed class ExplicitConsumerGrain : Grain, IConsumerGrain
{
    public async Task Subscribe() =>
        await this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Explicit, this.GetPrimaryKey()))
            .SubscribeAsync((item, _) =>
            {
                DeliveryLog.Record(this.GetPrimaryKey(), item);
                return Task.CompletedTask;
            });
}

public sealed class ProducerGrain : Grain, IProducerGrain
{
    public async Task Produce(string streamNamespace, Guid stream, int[] items)
    {
        var target = this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(streamNamespace, stream));
        foreach (var item in items)
        {
            await target.OnNextAsync(item);
        }
    }

    public Task ProduceBatch(string streamNamespace, Guid stream, int[] items) =>
        this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(streamNamespace, stream)).OnNextBatchAsync(items);
}
