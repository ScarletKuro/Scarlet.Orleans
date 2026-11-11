using Orleans.Runtime;
using Orleans.TestingHost;

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
        var streams = Enumerable.Range(0, 8).Select(_ => Guid.NewGuid()).ToArray();
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
        await Task.Delay(TimeSpan.FromMilliseconds(500), CancellationToken);
        var deliveredBefore = streams.Sum(stream => DeliveryLog.Of(stream).Length);
        await leave(cluster, cluster.Silos[1]);

        Assert.True(deliveredBefore < streams.Length * items.Length, "Every message was delivered before the silo left, so nothing was handed over.");

        // Within 90 seconds: well inside the stopped silo's visibility timeout, and well past the killed one's.
        foreach (var stream in streams)
        {
            await DeliveryLog.WaitForAsync(stream, items, TimeSpan.FromSeconds(90), CancellationToken);
        }
    }
}
