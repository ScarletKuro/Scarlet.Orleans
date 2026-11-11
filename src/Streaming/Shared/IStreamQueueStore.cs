namespace Scarlet.Orleans.Streaming;

/// <summary>
/// The atomic queue operations of one backend (design section 5.6). The receiver holds the queue semantics; the store
/// only applies each operation atomically.
/// </summary>
/// <remarks>
/// <para>
/// A dequeued message carries a receipt, its dequeue count after that dequeue. Confirm, release and fail act only on a
/// message whose dequeue count still equals the receipt, so an operation for an older attempt never touches a newer one.
/// Message IDs are never reused, which keeps a receipt from matching a later message.
/// </para>
/// <para>
/// Times are passed in, from the caller's <see cref="TimeProvider"/> (design section 4.5), and compared as UTC ticks.
/// </para>
/// </remarks>
internal interface IStreamQueueStore
{
    /// <summary>Checks or applies the schema, as configured.</summary>
    Task InitializeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Records the queue, if it is not recorded yet, with its first eviction due at <paramref name="evictOn"/>. Throws if
    /// another queue's identity is recorded under the same hash.
    /// </summary>
    Task RegisterQueueAsync(StreamQueueKey queue, DateTimeOffset now, DateTimeOffset evictOn, CancellationToken cancellationToken);

    /// <summary>
    /// Adds a message, visible from <paramref name="now"/> until <paramref name="expiresOn"/>. A message already stored
    /// under <paramref name="messageKey"/> means an earlier attempt of this call committed, and is success.
    /// </summary>
    Task EnqueueAsync(StreamQueueKey queue, Guid messageKey, byte[] payload, DateTimeOffset now, DateTimeOffset expiresOn, CancellationToken cancellationToken);

    /// <summary>
    /// Takes up to <paramref name="maxCount"/> visible, unexpired messages dequeued fewer than
    /// <paramref name="maxAttempts"/> times, skipping messages another dequeue holds, and hides them until
    /// <paramref name="visibleOn"/>. Returns them in message ID order, each with its new receipt.
    /// </summary>
    Task<IReadOnlyList<DequeuedMessage>> DequeueAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset visibleOn, CancellationToken cancellationToken);

    /// <summary>Deletes the messages whose receipts still match. Returns how many it deleted.</summary>
    Task<int> ConfirmAsync(StreamQueueKey queue, IReadOnlyList<MessageReceipt> receipts, CancellationToken cancellationToken);

    /// <summary>Makes the messages whose receipts still match visible at <paramref name="now"/>. Returns how many.</summary>
    Task<int> ReleaseAsync(StreamQueueKey queue, IReadOnlyList<MessageReceipt> receipts, DateTimeOffset now, CancellationToken cancellationToken);

    /// <summary>
    /// If the message's receipt still matches, makes it visible at <paramref name="now"/>, or, once it has been dequeued
    /// <paramref name="maxAttempts"/> times, moves it to the dead letters until <paramref name="removeOn"/>.
    /// </summary>
    Task<FailOutcome> FailAsync(StreamQueueKey queue, MessageReceipt receipt, int maxAttempts, DateTimeOffset now, DateTimeOffset removeOn, CancellationToken cancellationToken);

    /// <summary>
    /// Claims the queue's eviction if it is due at <paramref name="now"/>, moving the next one to <paramref name="next"/>.
    /// Of concurrent callers, only one claims a due eviction.
    /// </summary>
    Task<bool> TryClaimEvictionAsync(StreamQueueKey queue, DateTimeOffset now, DateTimeOffset next, CancellationToken cancellationToken);

    /// <summary>
    /// Moves up to <paramref name="maxCount"/> messages to the dead letters, until <paramref name="removeOn"/>: those
    /// expired at <paramref name="now"/>, and those dequeued <paramref name="maxAttempts"/> times whose last attempt's
    /// visibility has ended. Returns how many it moved.
    /// </summary>
    Task<int> EvictAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset removeOn, CancellationToken cancellationToken);

    /// <summary>Deletes up to <paramref name="maxCount"/> dead letters due for removal at <paramref name="now"/>.</summary>
    Task<int> EvictDeadLettersAsync(StreamQueueKey queue, int maxCount, DateTimeOffset now, CancellationToken cancellationToken);
}

/// <param name="MessageId">The message's ID, unique and never reused.</param>
/// <param name="MessageKey">The key the enqueue generated, the same across its retries.</param>
/// <param name="Receipt">The message's dequeue count after this dequeue.</param>
/// <param name="ExpiresOn">When the message expires.</param>
/// <param name="Payload">The serialized batch.</param>
internal sealed record DequeuedMessage(long MessageId, Guid MessageKey, int Receipt, DateTimeOffset ExpiresOn, byte[] Payload)
{
    public MessageReceipt ToReceipt() => new(MessageId, Receipt);
}

/// <param name="MessageId">The message's ID.</param>
/// <param name="Receipt">The dequeue count the operation expects the message to still have.</param>
internal readonly record struct MessageReceipt(long MessageId, int Receipt);

internal enum FailOutcome
{
    /// <summary>The receipt no longer matched, so nothing changed.</summary>
    Stale,

    /// <summary>The message is visible again, for another attempt.</summary>
    Retried,

    /// <summary>The message was moved to the dead letters.</summary>
    DeadLettered,
}
