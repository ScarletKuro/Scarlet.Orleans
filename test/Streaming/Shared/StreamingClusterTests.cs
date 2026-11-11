using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Scarlet.Orleans.Streaming.Tests;

/// <summary>
/// Streams through the provider in a two-silo cluster: delivery from clients and grains, implicit and explicit
/// subscriptions, batches, redelivery through the failure handler, and silos leaving while they hold messages
/// (design section 5.6).
/// </summary>
public abstract class StreamingClusterTests(StreamingClusterFixture fixture)
{
    private static readonly TimeSpan Delivery = TimeSpan.FromSeconds(60);

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ClientProducer_DeliversEveryEvent_InOrder()
    {
        var stream = Guid.NewGuid();
        int[] items = [.. Enumerable.Range(0, 200)];
        var producer = fixture.Cluster.Client.GetStreamProvider(StreamNamespaces.Provider).GetStream<int>(StreamId.Create(StreamNamespaces.Implicit, stream));

        foreach (var item in items)
        {
            await producer.OnNextAsync(item);
        }

        await DeliveryLog.WaitForAsync(stream, items, Delivery, CancellationToken);
        Assert.Equal(items, DeliveryLog.Of(stream));
    }

    [Fact]
    public async Task GrainProducers_OnManyStreams_DeliverEachStreamsEvents()
    {
        var streams = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToArray();
        int[] items = [.. Enumerable.Range(0, 50)];

        await Task.WhenAll(streams.Select(stream => fixture.Cluster.Client.GetGrain<IProducerGrain>(Guid.NewGuid()).Produce(StreamNamespaces.Implicit, stream, items)));

        foreach (var stream in streams)
        {
            await DeliveryLog.WaitForAsync(stream, items, Delivery, CancellationToken);
            Assert.Equal(items, DeliveryLog.Of(stream));
        }
    }

    [Fact]
    public async Task ExplicitSubscription_ReceivesEventsProducedAfterIt()
    {
        var stream = Guid.NewGuid();
        int[] items = [.. Enumerable.Range(0, 20)];
        await fixture.Cluster.Client.GetGrain<IConsumerGrain>(stream, typeof(ExplicitConsumerGrain).FullName).Subscribe();

        await fixture.Cluster.Client.GetGrain<IProducerGrain>(Guid.NewGuid()).Produce(StreamNamespaces.Explicit, stream, items);

        await DeliveryLog.WaitForAsync(stream, items, Delivery, CancellationToken);
        Assert.Equal(items, DeliveryLog.Of(stream));
    }

    [Fact]
    public async Task Batch_IsDeliveredWhole()
    {
        var stream = Guid.NewGuid();
        int[] items = [.. Enumerable.Range(0, 10)];

        await fixture.Cluster.Client.GetGrain<IProducerGrain>(Guid.NewGuid()).ProduceBatch(StreamNamespaces.Implicit, stream, items);

        await DeliveryLog.WaitForAsync(stream, items, Delivery, CancellationToken);
        Assert.Equal(items, DeliveryLog.Of(stream));
    }

    [Fact]
    public async Task FailedDelivery_IsRetriedThroughTheFailureHandler()
    {
        var stream = Guid.NewGuid();
        int[] items = [1, -1, 2];

        await fixture.Cluster.Client.GetGrain<IProducerGrain>(Guid.NewGuid()).Produce(StreamNamespaces.Failing, stream, items);

        await DeliveryLog.WaitForAsync(stream, items, Delivery, CancellationToken);
        Assert.True(DeliveryLog.AttemptsOf(stream, -1) >= 2);
    }
}

/// <summary>
/// Silos leaving while they hold cached, undelivered messages. Each test deploys its own cluster on its own database.
/// </summary>
public abstract class StreamingHandOverTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    /// <summary>
    /// A stopping silo releases what it holds, so its queues' next owner delivers it within a visibility timeout
    /// that the test outlasts by far.
    /// </summary>
    [Fact]
    public Task StoppedSilo_ReleasesItsMessages_ToTheNextOwner() =>
        HandOverAsync(TimeSpan.FromMinutes(5), (cluster, silo) => cluster.StopSiloAsync(silo, CancellationToken));

    /// <summary>
    /// A silo killed in process cannot be relied on to release, so its messages return when their visibility timeout
    /// ends; none is lost.
    /// </summary>
    [Fact]
    public Task KilledSilo_LosesNoMessage() =>
        HandOverAsync(TimeSpan.FromSeconds(10), (cluster, silo) => cluster.KillSiloAsync(silo, CancellationToken));

    private async Task HandOverAsync(TimeSpan visibilityTimeout, Func<InProcessTestCluster, InProcessSiloHandle, Task> leave)
    {
        fixture.Require();
        var database = await fixture.CreateDatabaseAsync();
        await using var cluster = await StreamingCluster.StartAsync(database, silos: 2, visibilityTimeout);
        var leaving = cluster.Silos[1];
        var factory = (RelationalQueueAdapterFactory)leaving.ServiceProvider.GetRequiredKeyedService<IQueueAdapterFactory>(StreamNamespaces.Provider);
        var owned = await WaitForAsync(() => factory.OwnedQueues, queues => queues.Count > 0, "The departing silo never took a queue.");

        // Only streams the departing silo reads, so every message the test waits for was held there.
        var mapper = factory.GetStreamQueueMapper();
        var streams = Enumerable.Range(0, int.MaxValue).Select(_ => Guid.NewGuid())
            .Where(stream => owned.Contains(mapper.GetQueueForStream(StreamId.Create(StreamNamespaces.Slow, stream))))
            .Take(6)
            .ToArray();
        int[] items = [.. Enumerable.Range(0, 60)];
        var provider = cluster.Client.GetStreamProvider(StreamNamespaces.Provider);
        await Task.WhenAll(streams.Select(async stream =>
        {
            var producer = provider.GetStream<int>(StreamId.Create(StreamNamespaces.Slow, stream));
            foreach (var item in items)
            {
                await producer.OnNextAsync(item);
            }
        }));

        await WaitForAsync(() => factory.HeldMessages, held => held > 0, "The departing silo never held a message.");
        Assert.True(streams.Sum(stream => DeliveryLog.Of(stream).Length) < streams.Length * items.Length);
        await leave(cluster, leaving);

        // Within 90 seconds: well inside the stopped silo's visibility timeout, and well past the killed one's.
        foreach (var stream in streams)
        {
            await DeliveryLog.WaitForAsync(stream, items, TimeSpan.FromSeconds(90), CancellationToken);
        }
    }

    private static async Task<T> WaitForAsync<T>(Func<T> read, Func<T, bool> done, string failure)
    {
        for (var waited = TimeSpan.Zero; waited < TimeSpan.FromSeconds(30); waited += TimeSpan.FromMilliseconds(20))
        {
            if (read() is var value && done(value))
            {
                return value;
            }

            await Task.Delay(20, CancellationToken);
        }

        throw new TimeoutException(failure);
    }
}
