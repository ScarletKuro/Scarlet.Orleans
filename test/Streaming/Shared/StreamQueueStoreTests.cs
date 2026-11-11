using System.Collections.Concurrent;
using System.Data;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Streaming.Tests;

/// <summary>
/// The queue store contract (<see cref="IStreamQueueStore"/>) on every engine: receipts, visibility, attempts,
/// idempotent enqueue, dead letters and eviction (design section 5.6). Times are passed in, so tests move time by passing
/// a later one.
/// </summary>
public abstract class StreamQueueStoreTests(DatabaseFixture fixture) : IAsyncLifetime
{
    private const int MaxAttempts = 3;

    private static readonly DateTimeOffset T0 = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static readonly TimeSpan Visibility = TimeSpan.FromMinutes(1);

    private static readonly TimeSpan Expiry = TimeSpan.FromMinutes(10);

    private static readonly TimeSpan Retention = TimeSpan.FromDays(7);

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
    public async Task Dequeue_ReturnsMessagesInEnqueueOrder_WithTheirFirstReceipt()
    {
        var queue = NewQueue();
        var keys = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        for (var i = 0; i < keys.Length; i++)
        {
            await EnqueueAsync(queue, keys[i], payload: i);
        }

        var messages = await DequeueAsync(queue, T0);

        Assert.Equal(keys, messages.Select(m => m.MessageKey));
        Assert.All(messages, m => Assert.Equal(1, m.Receipt));
        Assert.Equal([0, 1, 2], messages.Select(m => BitConverter.ToInt32(m.Payload)));
        Assert.True(messages.Zip(messages.Skip(1)).All(pair => pair.First.MessageId < pair.Second.MessageId));
        Assert.All(messages, m => Assert.Equal(T0 + Expiry, m.ExpiresOn));
    }

    [Fact]
    public async Task Enqueue_RetriedWithTheSameKey_StoresOneMessage()
    {
        var queue = NewQueue();
        var key = Guid.NewGuid();

        await EnqueueAsync(queue, key);
        await EnqueueAsync(queue, key);

        Assert.Single(await DequeueAsync(queue, T0));
    }

    [Fact]
    public async Task Dequeue_HidesMessages_UntilTheirVisibilityEnds()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid());
        var first = Assert.Single(await DequeueAsync(queue, T0));

        Assert.Empty(await DequeueAsync(queue, T0 + Visibility - TimeSpan.FromTicks(1)));
        var second = Assert.Single(await DequeueAsync(queue, T0 + Visibility));

        Assert.Equal(first.MessageId, second.MessageId);
        Assert.Equal(2, second.Receipt);
    }

    [Fact]
    public async Task Dequeue_SkipsExpiredMessages_AndMessagesOutOfAttempts()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid());
        var now = T0;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++, now += Visibility)
        {
            Assert.Equal(attempt, Assert.Single(await DequeueAsync(queue, now)).Receipt);
        }

        Assert.Empty(await DequeueAsync(queue, now));

        var expiring = NewQueue();
        await EnqueueAsync(expiring, Guid.NewGuid());
        Assert.Empty(await DequeueAsync(expiring, T0 + Expiry));
    }

    [Fact]
    public async Task Dequeue_TakesAtMostTheRequestedCount_AndLeavesTheRest()
    {
        var queue = NewQueue();
        for (var i = 0; i < 5; i++)
        {
            await EnqueueAsync(queue, Guid.NewGuid(), payload: i);
        }

        var first = await store.DequeueAsync(queue, 2, MaxAttempts, T0, T0 + Visibility, CancellationToken);
        var rest = await DequeueAsync(queue, T0);

        Assert.Equal([0, 1], first.Select(m => BitConverter.ToInt32(m.Payload)));
        Assert.Equal([2, 3, 4], rest.Select(m => BitConverter.ToInt32(m.Payload)));
    }

    [Fact]
    public async Task Queues_DoNotShareMessages()
    {
        var a = NewQueue();
        var b = NewQueue();
        await EnqueueAsync(a, Guid.NewGuid());

        Assert.Empty(await DequeueAsync(b, T0));
        Assert.Single(await DequeueAsync(a, T0));
    }

    [Fact]
    public async Task Confirm_DeletesOnlyMessagesWhoseReceiptStillMatches()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid());
        await EnqueueAsync(queue, Guid.NewGuid());
        var first = await DequeueAsync(queue, T0);
        var redelivered = await DequeueAsync(queue, T0 + Visibility);

        Assert.Equal(0, await store.ConfirmAsync(queue, [.. first.Select(m => m.ToReceipt())], CancellationToken));
        Assert.Equal(2, await store.ConfirmAsync(queue, [.. redelivered.Select(m => m.ToReceipt())], CancellationToken));
        Assert.Empty(await DequeueAsync(queue, T0 + (3 * Visibility)));
    }

    [Fact]
    public async Task Confirm_OfMoreReceiptsThanOneStatementTakes_DeletesThemAll()
    {
        var queue = NewQueue();
        const int count = 501;
        for (var i = 0; i < count; i++)
        {
            await EnqueueAsync(queue, Guid.NewGuid());
        }

        var messages = await store.DequeueAsync(queue, count, MaxAttempts, T0, T0 + Visibility, CancellationToken);

        Assert.Equal(count, messages.Count);
        Assert.Equal(count, await store.ConfirmAsync(queue, [.. messages.Select(m => m.ToReceipt())], CancellationToken));
    }

    [Fact]
    public async Task Release_MakesOnlyMessagesWhoseReceiptStillMatches_VisibleNow()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid());
        var message = Assert.Single(await DequeueAsync(queue, T0));

        Assert.Equal(0, await store.ReleaseAsync(queue, [message.ToReceipt() with { Receipt = 2 }], T0, CancellationToken));
        Assert.Empty(await DequeueAsync(queue, T0));
        Assert.Equal(1, await store.ReleaseAsync(queue, [message.ToReceipt()], T0, CancellationToken));

        Assert.Equal(2, Assert.Single(await DequeueAsync(queue, T0)).Receipt);
    }

    [Fact]
    public async Task Fail_BelowMaxAttempts_MakesTheMessageVisibleAgain()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid());
        var message = Assert.Single(await DequeueAsync(queue, T0));

        Assert.Equal(FailOutcome.Retried, await FailAsync(queue, message.ToReceipt(), T0));

        Assert.Equal(2, Assert.Single(await DequeueAsync(queue, T0)).Receipt);
    }

    [Fact]
    public async Task Fail_WithAStaleReceipt_ChangesNothing()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid());
        var stale = Assert.Single(await DequeueAsync(queue, T0));
        var current = Assert.Single(await DequeueAsync(queue, T0 + Visibility));

        Assert.Equal(FailOutcome.Stale, await FailAsync(queue, stale.ToReceipt(), T0 + Visibility));
        Assert.Empty(await DequeueAsync(queue, T0 + Visibility));

        Assert.Equal(1, await store.ConfirmAsync(queue, [current.ToReceipt()], CancellationToken));
        Assert.Equal(FailOutcome.Stale, await FailAsync(queue, current.ToReceipt() with { Receipt = MaxAttempts }, T0 + Visibility));
        Assert.Equal(0, await CountDeadLettersAsync(queue));
    }

    [Fact]
    public async Task Fail_OnTheLastAttempt_MovesTheMessageToTheDeadLetters()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid());
        DequeuedMessage message = null!;
        var now = T0;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++, now += Visibility)
        {
            message = Assert.Single(await DequeueAsync(queue, now));
        }

        Assert.Equal(FailOutcome.DeadLettered, await FailAsync(queue, message.ToReceipt(), now));

        Assert.Equal(1, await CountDeadLettersAsync(queue));
        Assert.Equal(0, await store.ConfirmAsync(queue, [message.ToReceipt()], CancellationToken));
        Assert.Equal(0, await store.EvictDeadLettersAsync(queue, 10, now + Retention - TimeSpan.FromTicks(1), CancellationToken));
        Assert.Equal(1, await store.EvictDeadLettersAsync(queue, 10, now + Retention, CancellationToken));
        Assert.Equal(0, await CountDeadLettersAsync(queue));
    }

    [Fact]
    public async Task Evict_MovesExpiredMessages_AndOnesOutOfAttemptsOnceInvisible()
    {
        var queue = NewQueue();
        await EnqueueAsync(queue, Guid.NewGuid(), payload: 1);
        var now = T0;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++, now += Visibility)
        {
            await DequeueAsync(queue, now);
        }

        var exhaustedHidden = now - Visibility;
        Assert.Equal(0, await store.EvictAsync(queue, 10, MaxAttempts, exhaustedHidden, exhaustedHidden + Retention, CancellationToken));
        Assert.Equal(1, await store.EvictAsync(queue, 10, MaxAttempts, now, now + Retention, CancellationToken));

        var expiring = NewQueue();
        await EnqueueAsync(expiring, Guid.NewGuid());
        Assert.Equal(0, await store.EvictAsync(expiring, 10, MaxAttempts, T0 + Expiry - TimeSpan.FromTicks(1), T0 + Retention, CancellationToken));
        Assert.Equal(1, await store.EvictAsync(expiring, 10, MaxAttempts, T0 + Expiry, T0 + Retention, CancellationToken));

        Assert.Equal(1, await CountDeadLettersAsync(queue));
        Assert.Equal(1, await CountDeadLettersAsync(expiring));
    }

    [Fact]
    public async Task Evict_AndEvictDeadLetters_MoveAtMostTheRequestedCount()
    {
        var queue = NewQueue();
        for (var i = 0; i < 5; i++)
        {
            await EnqueueAsync(queue, Guid.NewGuid());
        }

        Assert.Equal(3, await store.EvictAsync(queue, 3, MaxAttempts, T0 + Expiry, T0 + Retention, CancellationToken));
        Assert.Equal(2, await store.EvictAsync(queue, 3, MaxAttempts, T0 + Expiry, T0 + Retention, CancellationToken));
        Assert.Equal(4, await store.EvictDeadLettersAsync(queue, 4, T0 + Retention, CancellationToken));
        Assert.Equal(1, await store.EvictDeadLettersAsync(queue, 4, T0 + Retention, CancellationToken));
    }

    [Fact]
    public async Task TryClaimEviction_LetsOneCallerClaimEachDueSlot()
    {
        var queue = NewQueue();
        await store.RegisterQueueAsync(queue, T0, T0 + TimeSpan.FromSeconds(10), CancellationToken);

        Assert.False(await store.TryClaimEvictionAsync(queue, T0, T0 + TimeSpan.FromSeconds(20), CancellationToken));
        var due = T0 + TimeSpan.FromSeconds(10);
        var claims = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ =>
            Task.Run(() => store.TryClaimEvictionAsync(queue, due, due + TimeSpan.FromSeconds(10), CancellationToken))));

        Assert.Equal(1, claims.Count(claimed => claimed));
        Assert.False(await store.TryClaimEvictionAsync(queue, due, due + TimeSpan.FromSeconds(10), CancellationToken));
    }

    [Fact]
    public async Task RegisterQueue_IsIdempotent_AndRejectsAnotherQueueUnderTheSameHash()
    {
        var queue = NewQueue();
        await store.RegisterQueueAsync(queue, T0, T0, CancellationToken);
        await store.RegisterQueueAsync(queue, T0, T0, CancellationToken);

        var squatter = NewQueue();
        await using (var insert = fixture.Database.DataSource.CreateCommand(
            $"INSERT INTO {StreamingSchema.QueueTableName} (queue_hash, queue_identity, service_id, provider_name, queue_id, evict_on, created_on) " +
            "VALUES (@queue_hash, @queue_identity, 's', 'p', 'q', 0, @created_on)"))
        {
            insert.AddParameter("queue_hash", squatter.Hash, DbType.Binary);
            insert.AddParameter("queue_identity", new byte[] { 1, 2, 3 }, DbType.Binary);
            insert.AddParameter("created_on", T0, DbType.DateTimeOffset);
            await insert.ExecuteNonQueryAsync(CancellationToken);
        }

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RegisterQueueAsync(squatter, T0, T0, CancellationToken));
    }

    [Fact]
    public async Task ConcurrentDequeuers_NeverHoldTheSameMessage()
    {
        var queue = NewQueue();
        const int count = 200;
        for (var i = 0; i < count; i++)
        {
            await EnqueueAsync(queue, Guid.NewGuid());
        }

        var taken = new ConcurrentBag<long>();
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            while (true)
            {
                var batch = await store.DequeueAsync(queue, 7, MaxAttempts, T0, T0 + Visibility, CancellationToken);
                if (batch.Count == 0)
                {
                    return;
                }

                foreach (var message in batch)
                {
                    taken.Add(message.MessageId);
                }
            }
        })));

        Assert.Equal(count, taken.Count);
        Assert.Equal(count, taken.Distinct().Count());
    }

    private Task EnqueueAsync(StreamQueueKey queue, Guid key, int payload = 0) =>
        store.EnqueueAsync(queue, key, BitConverter.GetBytes(payload), T0, T0 + Expiry, CancellationToken);

    private Task<IReadOnlyList<DequeuedMessage>> DequeueAsync(StreamQueueKey queue, DateTimeOffset now) =>
        store.DequeueAsync(queue, 100, MaxAttempts, now, now + Visibility, CancellationToken);

    private Task<FailOutcome> FailAsync(StreamQueueKey queue, MessageReceipt receipt, DateTimeOffset now) =>
        store.FailAsync(queue, receipt, MaxAttempts, now, now + Retention, CancellationToken);

    private async Task<long> CountDeadLettersAsync(StreamQueueKey queue)
    {
        await using var count = fixture.Database.DataSource.CreateCommand($"SELECT COUNT(*) FROM {StreamingSchema.DeadLetterTableName} WHERE queue_hash = @queue_hash");
        count.AddParameter("queue_hash", queue.Hash, DbType.Binary);
        return Convert.ToInt64(await count.ExecuteScalarAsync(CancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static StreamQueueKey NewQueue() => StreamQueueKey.Create("service", "provider", Guid.NewGuid().ToString("N"));
}
