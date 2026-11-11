using System.Data;
using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Scarlet.Orleans.Streaming.Tests;

/// <summary>
/// The receiver, adapter and failure handler over a real store, without a silo: cache positions, coalesced
/// confirmation, release on shutdown, receipts in the failure handler, unreadable payloads, initialization retried, and
/// eviction (design section 5.6).
/// </summary>
public abstract class StreamReceiverTests(DatabaseFixture fixture) : IAsyncLifetime
{
    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly Serializer Serializer = new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<Serializer>();

    private readonly ManualTimeProvider time = new(T0);

    private readonly ScarletAdoNetStreamOptions options = new() { MaxAttempts = 3 };

    private readonly string provider = Guid.NewGuid().ToString("N");

    private IStreamQueueStore store = null!;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        fixture.Require();
        store = FlavourStreaming.CreateStore(fixture.Database, TestSchemaMode.Apply);
        await store.InitializeAsync(CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task Positions_IncreaseAcrossReads_ForRedeliveredMessagesToo()
    {
        var (adapter, receiver) = await CreateAsync();
        await EnqueueAsync(adapter, "a", "b");
        var first = await ReadAsync(receiver);

        time.Advance(options.VisibilityTimeout);
        await EnqueueAsync(adapter, "c");
        var second = await ReadAsync(receiver);

        Assert.Equal(["a", "b"], Events(first));
        Assert.Equal(["a", "b", "c"], Events(second));
        var positions = first.Concat(second).Select(b => b.SequenceToken.SequenceNumber).ToArray();
        Assert.Equal(positions.Order(), positions);
        Assert.Equal(positions.Length, positions.Distinct().Count());
    }

    [Fact]
    public async Task Delivered_AttemptsOfOneMessage_AreConfirmedOnce_WithTheLatestReceipt()
    {
        var (adapter, receiver) = await CreateAsync();
        await EnqueueAsync(adapter, "a");
        var first = await ReadAsync(receiver);
        time.Advance(options.VisibilityTimeout);
        var second = await ReadAsync(receiver);

        await receiver.MessagesDeliveredAsync([.. first, .. second], CancellationToken);

        time.Advance(options.VisibilityTimeout);
        Assert.Empty(await ReadAsync(receiver));
    }

    [Fact]
    public async Task Delivered_AnOlderAttemptAlone_DoesNotConfirmTheMessage()
    {
        var (adapter, receiver) = await CreateAsync();
        await EnqueueAsync(adapter, "a");
        var first = await ReadAsync(receiver);
        time.Advance(options.VisibilityTimeout);
        await ReadAsync(receiver);

        await receiver.MessagesDeliveredAsync(first, CancellationToken);

        time.Advance(options.VisibilityTimeout);
        Assert.Equal(["a"], Events(await ReadAsync(receiver)));
    }

    [Fact]
    public async Task Shutdown_ReleasesHeldMessages_SoTheNextOwnerReadsThemAtOnce()
    {
        var (adapter, receiver) = await CreateAsync();
        await EnqueueAsync(adapter, "a", "b");
        var held = await ReadAsync(receiver);
        await receiver.MessagesDeliveredAsync([held[0]], CancellationToken);

        await receiver.Shutdown(TimeSpan.FromSeconds(30), CancellationToken);

        var (_, next) = await CreateAsync();
        Assert.Equal(["b"], Events(await ReadAsync(next)));
    }

    [Fact]
    public async Task FailureHandler_RetriesTheFailedAttempt_IgnoresStalePositions_AndDeadLettersTheLast()
    {
        var factory = CreateFactory();
        var adapter = await factory.CreateAdapter(CancellationToken);
        var receiver = adapter.CreateReceiver(SingleQueue(factory));
        var handler = await factory.GetDeliveryFailureHandler(SingleQueue(factory));
        await EnqueueAsync(adapter, "a");

        var first = Assert.Single(await ReadAsync(receiver));
        await FailAsync(handler, first);
        var second = Assert.Single(await ReadAsync(receiver));
        await FailAsync(handler, first);
        Assert.Empty(await ReadAsync(receiver));

        await FailAsync(handler, second);
        var third = Assert.Single(await ReadAsync(receiver));
        await FailAsync(handler, third);

        Assert.Empty(await ReadAsync(receiver));
        Assert.Equal(1, await CountDeadLettersAsync());
    }

    [Fact]
    public async Task UnreadablePayload_GoesToTheDeadLetters_AndTheRestAreRead()
    {
        var factory = CreateFactory();
        var adapter = await factory.CreateAdapter(CancellationToken);
        var receiver = adapter.CreateReceiver(SingleQueue(factory));
        await store.EnqueueAsync(factory.GetQueue(StreamId.Create("test", "x")), Guid.NewGuid(), [1, 2, 3], T0, T0 + options.ExpiryTimeout, CancellationToken);
        await EnqueueAsync(adapter, "a");

        Assert.Equal(["a"], Events(await ReadAsync(receiver)));
        Assert.Equal(1, await CountDeadLettersAsync());
    }

    [Fact]
    public async Task FailedInitialization_IsFinishedByTheNextRead()
    {
        var failing = new FailingRegistration(store);
        var factory = CreateFactory(failing);
        var adapter = await factory.CreateAdapter(CancellationToken);
        var receiver = adapter.CreateReceiver(SingleQueue(factory));
        await EnqueueAsync(adapter, "a");

        await receiver.Initialize(TimeSpan.FromSeconds(30), CancellationToken);
        await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(receiver));

        Assert.Equal(["a"], Events(await ReadAsync(receiver)));
        Assert.Equal(3, failing.Attempts);
    }

    [Fact]
    public async Task Eviction_RunsOnTheReadAfterItIsDue()
    {
        var (adapter, receiver) = await CreateAsync();
        await EnqueueAsync(adapter, "a");
        Assert.Single(await ReadAsync(receiver));

        time.Advance(options.ExpiryTimeout);
        Assert.Empty(await ReadAsync(receiver));

        Assert.Equal(1, await CountDeadLettersAsync());
    }

    private async Task<(IQueueAdapter Adapter, IQueueAdapterReceiver Receiver)> CreateAsync()
    {
        var factory = CreateFactory();
        var adapter = await factory.CreateAdapter(CancellationToken);
        return (adapter, adapter.CreateReceiver(SingleQueue(factory)));
    }

    private RelationalQueueAdapterFactory CreateFactory(IStreamQueueStore? withStore = null) =>
        new(provider, "service", withStore ?? store, Serializer, options, new HashRingStreamQueueMapperOptions { TotalQueueCount = 1 }, new SimpleQueueCacheOptions(), time, NullLoggerFactory.Instance);

    private static QueueId SingleQueue(RelationalQueueAdapterFactory factory) => Assert.Single(factory.GetStreamQueueMapper().GetAllQueues());

    private static async Task EnqueueAsync(IQueueAdapter adapter, params string[] events)
    {
        foreach (var e in events)
        {
            await adapter.QueueMessageBatchAsync(StreamId.Create("test", e), [e], null, null);
        }
    }

    private static Task<IList<IBatchContainer>> ReadAsync(IQueueAdapterReceiver receiver) => receiver.GetQueueMessagesAsync(100, CancellationToken);

    private static string[] Events(IEnumerable<IBatchContainer> batches) => [.. batches.SelectMany(b => b.GetEvents<string>().Select(e => e.Item1))];

    private static Task FailAsync(IStreamFailureHandler handler, IBatchContainer batch) =>
        handler.OnDeliveryFailure(GuidId.GetNewGuidId(), "provider", StreamId.Create("test", "x"), batch.SequenceToken);

    private async Task<long> CountDeadLettersAsync()
    {
        await using var count = fixture.Database.DataSource.CreateCommand(
            $"SELECT COUNT(*) FROM {StreamingSchema.DeadLetterTableName} d JOIN {StreamingSchema.QueueTableName} q ON q.queue_hash = d.queue_hash WHERE q.provider_name = @provider");
        count.AddParameter("provider", provider, DbType.String);
        return Convert.ToInt64(await count.ExecuteScalarAsync(CancellationToken), CultureInfo.InvariantCulture);
    }

    private sealed class ManualTimeProvider(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset now = now;

        public override DateTimeOffset GetUtcNow() => now;

        public void Advance(TimeSpan by) => now += by;
    }

    /// <summary>Fails the first two registrations, as a database unreachable at startup would.</summary>
    private sealed class FailingRegistration(IStreamQueueStore inner) : IStreamQueueStore
    {
        public int Attempts { get; private set; }

        public Task RegisterQueueAsync(StreamQueueKey queue, DateTimeOffset now, DateTimeOffset evictOn, CancellationToken cancellationToken) =>
            ++Attempts <= 2 ? throw new InvalidOperationException("Unreachable.") : inner.RegisterQueueAsync(queue, now, evictOn, cancellationToken);

        public Task InitializeAsync(CancellationToken cancellationToken) => inner.InitializeAsync(cancellationToken);

        public Task EnqueueAsync(StreamQueueKey queue, Guid messageKey, byte[] payload, DateTimeOffset now, DateTimeOffset expiresOn, CancellationToken cancellationToken) =>
            inner.EnqueueAsync(queue, messageKey, payload, now, expiresOn, cancellationToken);

        public Task<IReadOnlyList<DequeuedMessage>> DequeueAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset visibleOn, CancellationToken cancellationToken) =>
            inner.DequeueAsync(queue, maxCount, maxAttempts, now, visibleOn, cancellationToken);

        public Task<int> ConfirmAsync(StreamQueueKey queue, IReadOnlyList<MessageReceipt> receipts, CancellationToken cancellationToken) =>
            inner.ConfirmAsync(queue, receipts, cancellationToken);

        public Task<int> ReleaseAsync(StreamQueueKey queue, IReadOnlyList<MessageReceipt> receipts, DateTimeOffset now, CancellationToken cancellationToken) =>
            inner.ReleaseAsync(queue, receipts, now, cancellationToken);

        public Task<FailOutcome> FailAsync(StreamQueueKey queue, MessageReceipt receipt, int maxAttempts, DateTimeOffset now, DateTimeOffset removeOn, CancellationToken cancellationToken) =>
            inner.FailAsync(queue, receipt, maxAttempts, now, removeOn, cancellationToken);

        public Task<bool> TryClaimEvictionAsync(StreamQueueKey queue, DateTimeOffset now, DateTimeOffset next, CancellationToken cancellationToken) =>
            inner.TryClaimEvictionAsync(queue, now, next, cancellationToken);

        public Task<int> EvictAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset removeOn, CancellationToken cancellationToken) =>
            inner.EvictAsync(queue, maxCount, maxAttempts, now, removeOn, cancellationToken);

        public Task<int> EvictDeadLettersAsync(StreamQueueKey queue, int maxCount, DateTimeOffset now, CancellationToken cancellationToken) =>
            inner.EvictDeadLettersAsync(queue, maxCount, now, cancellationToken);
    }
}
