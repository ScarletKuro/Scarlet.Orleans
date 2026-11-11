using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// Reads one queue for the pulling agent (design section 5.6).
/// </summary>
/// <remarks>
/// <para>
/// Every dequeued message gets a cache position from <see cref="StreamPositions"/>, never its ID, because a redelivered
/// or late-committed message has a lower ID than messages already cached. The receiver keeps each pending message's
/// latest receipt, confirms each message once with the highest receipt among the attempts that leave the cache
/// together, and releases what it still holds when it shuts down, since Orleans confirms nothing at shutdown or when a
/// rebalance moves the queue.
/// </para>
/// <para>
/// Orleans does not await <see cref="Initialize(TimeSpan, CancellationToken)"/>, so a read first finishes an
/// initialization that failed. A read also runs the queue's eviction when it is due and this receiver claims it.
/// </para>
/// </remarks>
internal sealed partial class RelationalQueueAdapterReceiver(
    StreamQueueKey queue,
    IStreamQueueStore store,
    StreamPositions positions,
    Serializer serializer,
    ScarletAdoNetStreamOptions options,
    StreamingObserver observer,
    TimeProvider timeProvider,
    ILogger<RelationalQueueAdapterReceiver> logger,
    Action<RelationalQueueAdapterReceiver>? onShutdown = null) : IQueueAdapterReceiver
{
    private readonly AdmissionGate gate = new();
    private readonly Lock pendingLock = new();
    private readonly Dictionary<long, Pending> pending = [];
    private volatile bool registered;
    private DateTimeOffset nextEvictionCheck;

    /// <summary>Messages this receiver holds, dequeued and not yet confirmed or released.</summary>
    public int HeldMessages
    {
        get
        {
            lock (pendingLock)
            {
                return pending.Count;
            }
        }
    }

    [Obsolete("Orleans calls the overload with a cancellation token.")]
    public Task Initialize(TimeSpan timeout) => Initialize(timeout, CancellationToken.None);

    public async Task Initialize(TimeSpan timeout, CancellationToken cancellationToken)
    {
        try
        {
            await RegisterAsync(cancellationToken).WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            LogInitializationFailed(exception, queue);
        }
    }

    [Obsolete("Orleans calls the overload with a cancellation token.")]
    public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount) => GetQueueMessagesAsync(maxCount, CancellationToken.None);

    public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount, CancellationToken cancellationToken)
    {
        if (maxCount <= 0 || !gate.TryEnter())
        {
            return [];
        }

        try
        {
            await RegisterAsync(cancellationToken).ConfigureAwait(false);
            var now = timeProvider.GetUtcNow();
            await EvictIfDueAsync(now, cancellationToken).ConfigureAwait(false);

            var messages = await observer.ObserveAsync("dequeue", queue, () =>
                store.DequeueAsync(queue, maxCount, options.MaxAttempts, now, now + options.VisibilityTimeout, cancellationToken)).ConfigureAwait(false);
            observer.Count("dequeued", messages.Count);
            observer.Count("redelivered", messages.Count(static m => m.Receipt > 1));
            var batches = new List<IBatchContainer>(messages.Count);
            foreach (var message in messages)
            {
                RelationalBatchContainer batch;
                try
                {
                    batch = serializer.Deserialize<RelationalBatchContainer>(message.Payload)
                        ?? throw new InvalidOperationException("The payload holds no batch.");
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    // A payload that does not deserialize never will, so it goes to the dead letters at once.
                    LogUnreadableMessage(exception, message.MessageId, queue);
                    var outcome = await observer.ObserveAsync("fail", queue, () =>
                        store.FailAsync(queue, message.ToReceipt(), message.Receipt, now, now + options.DeadLetterRetention, cancellationToken)).ConfigureAwait(false);
                    observer.Count("dead_lettered", outcome == FailOutcome.DeadLettered ? 1 : 0);
                    continue;
                }

                lock (pendingLock)
                {
                    if (pending.TryGetValue(message.MessageId, out var previous))
                    {
                        positions.Remove(previous.Position);
                    }

                    var position = positions.Add(queue, message.ToReceipt());
                    pending[message.MessageId] = new(message.Receipt, message.ExpiresOn, position);
                    batch.Position = new EventSequenceTokenV2(position);
                }

                batches.Add(batch);
            }

            RemoveExpired(now);
            return batches;
        }
        finally
        {
            gate.Exit();
        }
    }

    [Obsolete("Orleans calls the overload with a cancellation token.")]
    public Task MessagesDeliveredAsync(IList<IBatchContainer> messages) => MessagesDeliveredAsync(messages, CancellationToken.None);

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages, CancellationToken cancellationToken)
    {
        if (messages.Count == 0 || !gate.TryEnter())
        {
            return;
        }

        try
        {
            // Several attempts of one message can leave the cache together; only the latest receipt can match.
            var receipts = new Dictionary<long, int>(messages.Count);
            foreach (var message in messages)
            {
                if (message.SequenceToken is EventSequenceTokenV2 position && positions.TryRemove(position.SequenceNumber, out var attempt))
                {
                    var (messageId, receipt) = attempt.Receipt;
                    receipts[messageId] = receipts.TryGetValue(messageId, out var other) ? Math.Max(other, receipt) : receipt;
                }
            }

            if (receipts.Count == 0)
            {
                return;
            }

            var confirmed = await observer.ObserveAsync("confirm", queue, () =>
                store.ConfirmAsync(queue, [.. receipts.Select(static r => new MessageReceipt(r.Key, r.Value))], cancellationToken)).ConfigureAwait(false);
            observer.Count("confirmed", confirmed);
            lock (pendingLock)
            {
                foreach (var (messageId, receipt) in receipts)
                {
                    if (pending.TryGetValue(messageId, out var held) && held.Receipt == receipt)
                    {
                        pending.Remove(messageId);
                    }
                }
            }
        }
        finally
        {
            gate.Exit();
        }
    }

    [Obsolete("Orleans calls the overload with a cancellation token.")]
    public Task Shutdown(TimeSpan timeout) => Shutdown(timeout, CancellationToken.None);

    /// <summary>
    /// Waits for reads and confirmations in progress, then releases every message still held, so the queue's next owner
    /// receives them at once instead of after their visibility timeout.
    /// </summary>
    public async Task Shutdown(TimeSpan timeout, CancellationToken cancellationToken)
    {
        try
        {
            await ReleaseHeldAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            onShutdown?.Invoke(this);
        }
    }

    private async Task ReleaseHeldAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        try
        {
            await gate.CloseAsync().WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            LogShutdownTimedOut(queue);
            return;
        }

        MessageReceipt[] held;
        lock (pendingLock)
        {
            held = [.. pending.Select(static p => new MessageReceipt(p.Key, p.Value.Receipt))];
            foreach (var message in pending.Values)
            {
                positions.Remove(message.Position);
            }

            pending.Clear();
        }

        if (held.Length == 0)
        {
            return;
        }

        try
        {
            var released = await observer.ObserveAsync("release", queue, () =>
                store.ReleaseAsync(queue, held, timeProvider.GetUtcNow(), cancellationToken)).WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            observer.Count("released", released);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // The messages become visible again when their visibility timeout ends.
            LogReleaseFailed(exception, held.Length, queue);
        }
    }

    private async Task RegisterAsync(CancellationToken cancellationToken)
    {
        if (registered)
        {
            return;
        }

        var now = timeProvider.GetUtcNow();
        await store.RegisterQueueAsync(queue, now, now + options.EvictionInterval, cancellationToken).ConfigureAwait(false);
        nextEvictionCheck = now + options.EvictionInterval;
        registered = true;
    }

    /// <summary>
    /// Runs the queue's eviction if it is due and this receiver claims it. Eviction is upkeep, so its failure is logged
    /// and does not stop the read; the next due slot tries again.
    /// </summary>
    private async Task EvictIfDueAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (now < nextEvictionCheck)
        {
            return;
        }

        nextEvictionCheck = now + options.EvictionInterval;
        try
        {
            if (await store.TryClaimEvictionAsync(queue, now, now + options.EvictionInterval, cancellationToken).ConfigureAwait(false))
            {
                var (moved, deleted) = await observer.ObserveAsync("evict", queue, async () => (
                    await store.EvictAsync(queue, options.EvictionBatchSize, options.MaxAttempts, now, now + options.DeadLetterRetention, cancellationToken).ConfigureAwait(false),
                    await store.EvictDeadLettersAsync(queue, options.EvictionBatchSize, now, cancellationToken).ConfigureAwait(false))).ConfigureAwait(false);
                observer.Count("dead_lettered", moved);
                observer.Count("removed", deleted);
                if (moved > 0 || deleted > 0)
                {
                    LogEvicted(moved, deleted, queue);
                }
            }
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            LogEvictionFailed(exception, queue);
        }
    }

    /// <summary>Forgets held messages past their expiry, which eviction moves to the dead letters.</summary>
    private void RemoveExpired(DateTimeOffset now)
    {
        lock (pendingLock)
        {
            foreach (var (messageId, message) in pending)
            {
                if (message.ExpiresOn <= now)
                {
                    positions.Remove(message.Position);
                    pending.Remove(messageId);
                }
            }
        }
    }

    private readonly record struct Pending(int Receipt, DateTimeOffset ExpiresOn, long Position);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not register stream queue {Queue}; the next read tries again.")]
    private partial void LogInitializationFailed(Exception exception, StreamQueueKey queue);

    [LoggerMessage(Level = LogLevel.Error, Message = "Message {MessageId} of stream queue {Queue} could not be deserialized and was moved to the dead letters.")]
    private partial void LogUnreadableMessage(Exception exception, long messageId, StreamQueueKey queue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Stream queue {Queue} did not finish its reads before the shutdown timeout; its held messages reappear after their visibility timeout.")]
    private partial void LogShutdownTimedOut(StreamQueueKey queue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not release {Count} messages of stream queue {Queue}; they reappear after their visibility timeout.")]
    private partial void LogReleaseFailed(Exception exception, int count, StreamQueueKey queue);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Moved {Moved} messages to the dead letters and deleted {Deleted} dead letters of stream queue {Queue}.")]
    private partial void LogEvicted(int moved, int deleted, StreamQueueKey queue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Eviction of stream queue {Queue} failed; the next due slot tries again.")]
    private partial void LogEvictionFailed(Exception exception, StreamQueueKey queue);
}
