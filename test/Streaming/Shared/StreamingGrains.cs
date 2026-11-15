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

    public const string Context = "context";
}

/// <summary>
/// What the consumer grains received, per stream. The cluster runs in the test's process, so the log outlives a grain
/// that a stopped silo moves elsewhere.
/// </summary>
public static class DeliveryLog
{
    private static readonly ConcurrentDictionary<Guid, ConcurrentQueue<Delivery>> Received = new();

    private static readonly ConcurrentDictionary<(Guid, int), int> Attempts = new();

    private static readonly ConcurrentDictionary<(Guid, int), string?> Contexts = new();

    public static void RecordContext(Guid stream, int item, string? trace) => Contexts[(stream, item)] = trace;

    public static string? ContextOf(Guid stream, int item) => Contexts.GetValueOrDefault((stream, item));

    public static void Record(Guid stream, int item, StreamSequenceToken? token = null, string? consumer = null) =>
        Received.GetOrAdd(stream, static _ => new()).Enqueue(new(item, DateTimeOffset.UtcNow, token?.SequenceNumber, token?.EventIndex, consumer));

    /// <summary>Counts an attempt to deliver <paramref name="item"/>, returning how many there have been.</summary>
    public static int Attempt(Guid stream, int item) => Attempts.AddOrUpdate((stream, item), 1, static (_, n) => n + 1);

    public static int AttemptsOf(Guid stream, int item) => Attempts.TryGetValue((stream, item), out var n) ? n : 0;

    public static int[] Of(Guid stream) => Received.TryGetValue(stream, out var items) ? [.. items.Select(d => d.Item)] : [];

    public static void AssertInOrder(Guid stream, IEnumerable<int> expected)
    {
        var wanted = expected.ToArray();
        var actual = Of(stream);
        if (!wanted.SequenceEqual(actual)) Assert.Fail(FailureDetails(stream, wanted, actual));
    }

    private static string FailureDetails(Guid stream, int[] expected, int[] actual)
    {
        var arrivals = Received.TryGetValue(stream, out var entries) ? entries.ToArray() : [];
        return $"Stream/consumer {stream}\nExpected: [{string.Join(", ", expected)}]\nActual: [{string.Join(", ", actual)}]" +
            $"\nMissing: [{string.Join(", ", expected.Except(actual))}]" +
            $"\nDuplicates: [{string.Join(", ", actual.GroupBy(i => i).Where(g => g.Count() > 1).Select(g => $"{g.Key} x{g.Count()}"))}]" +
            "\nArrivals (observed order):\n" + string.Join("\n", arrivals.Select((d, i) => $"{i}: {d.Time:O} item={d.Item} token={d.Sequence}/{d.EventIndex} consumer_silo={d.Consumer}")) +
            $"\nProvider warnings:\n{WarningLog.Recent}";
    }

    private sealed record Delivery(int Item, DateTimeOffset Time, long? Sequence, int? EventIndex, string? Consumer);

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
            throw new TimeoutException($"Delivery deadline {timeout} expired.\n{FailureDetails(stream, expected.ToArray(), Of(stream))}");
        }
    }
}

public interface IConsumerGrain : IGrainWithGuidKey
{
    Task Subscribe();

    Task Deactivate();
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
            .SubscribeAsync((item, token) => Record(item, token));

    public Task Subscribe() => Task.CompletedTask;

    public Task Deactivate()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }

    private Task Record(int item, StreamSequenceToken? token)
    {
        DeliveryLog.Record(this.GetPrimaryKey(), item, token, RuntimeIdentity);
        return Task.CompletedTask;
    }
}

/// <summary>Fails the first delivery of every negative item, and records every item it accepts.</summary>
[ImplicitStreamSubscription(StreamNamespaces.Failing)]
public sealed class FailingConsumerGrain : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Failing, this.GetPrimaryKey()))
            .SubscribeAsync((item, token) =>
            {
                if (item < 0 && DeliveryLog.Attempt(this.GetPrimaryKey(), item) == 1)
                {
                    throw new InvalidOperationException($"Failing the first delivery of {item}.");
                }

                DeliveryLog.Record(this.GetPrimaryKey(), item, token, RuntimeIdentity);
                return Task.CompletedTask;
            });

    public Task Subscribe() => Task.CompletedTask;

    public Task Deactivate()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }
}

/// <summary>Takes a while over every item, so a silo stopping holds messages that are cached but not yet delivered.</summary>
[ImplicitStreamSubscription(StreamNamespaces.Slow)]
public sealed class SlowConsumerGrain : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Slow, this.GetPrimaryKey()))
            .SubscribeAsync(async (item, token) =>
            {
                await Task.Delay(100);
                DeliveryLog.Record(this.GetPrimaryKey(), item, token, RuntimeIdentity);
            });

    public Task Subscribe() => Task.CompletedTask;

    public Task Deactivate()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }
}

/// <summary>Subscribes explicitly, through pub-sub, when asked.</summary>
public sealed class ExplicitConsumerGrain : Grain, IConsumerGrain
{
    public async Task Subscribe() =>
        await this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Explicit, this.GetPrimaryKey()))
            .SubscribeAsync((item, token) =>
            {
                DeliveryLog.Record(this.GetPrimaryKey(), item, token, RuntimeIdentity);
                return Task.CompletedTask;
            });

    public Task Deactivate()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }
}

/// <summary>
/// Subscribes explicitly to any stream, recording under its own key, so that several can read one stream; and
/// unsubscribes when asked.
/// </summary>
public interface ISubscriberGrain : IGrainWithGuidKey
{
    Task Subscribe(Guid stream);

    Task Unsubscribe();
}

public sealed class SubscriberGrain : Grain, ISubscriberGrain
{
    private StreamSubscriptionHandle<int>? subscription;

    public async Task Subscribe(Guid stream) =>
        subscription = await this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Explicit, stream))
            .SubscribeAsync((item, token) =>
            {
                DeliveryLog.Record(this.GetPrimaryKey(), item, token, RuntimeIdentity);
                return Task.CompletedTask;
            });

    public async Task Unsubscribe()
    {
        await subscription!.UnsubscribeAsync();
        subscription = null;
    }
}

/// <summary>Records every item of its implicitly subscribed stream, with the request context it arrived with.</summary>
[ImplicitStreamSubscription(StreamNamespaces.Context)]
public sealed class ContextConsumerGrain : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Context, this.GetPrimaryKey()))
            .SubscribeAsync((item, token) =>
            {
                DeliveryLog.RecordContext(this.GetPrimaryKey(), item, RequestContext.Get("trace") as string);
                DeliveryLog.Record(this.GetPrimaryKey(), item, token, RuntimeIdentity);
                return Task.CompletedTask;
            });

    public Task Subscribe() => Task.CompletedTask;

    public Task Deactivate()
    {
        DeactivateOnIdle();
        return Task.CompletedTask;
    }
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
