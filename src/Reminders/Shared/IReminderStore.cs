namespace Scarlet.Orleans.Reminders;

/// <summary>
/// The atomic database operations behind the reminder table. Each method runs one statement and reports that statement's result.
/// </summary>
/// <remarks>
/// <para>
/// Stores hold no Orleans logic: upsert, identity verification and decoding live in <see cref="RelationalReminderTable"/>,
/// which both flavours share. Rows returned by a hash are returned whatever their identity.
/// </para>
/// <para>
/// <b>Retries.</b> A store may repeat a statement only when the server reported that the statement was not applied
/// (<c>TransientRetry</c>). It never repeats a statement after a connection failure, whose outcome is unknown; that
/// failure surfaces to Orleans. A store also prevents layers it controls from repeating writes, such as a retrying EF
/// Core execution strategy.
/// </para>
/// <para>
/// <b>Transactions.</b> Every statement autocommits. Callers run outside any ambient transaction.
/// </para>
/// </remarks>
internal interface IReminderStore
{
    /// <summary>
    /// Resolves the engine and checks or applies the schema. Called before any other method; may be called again.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Reads the row stored under <see cref="ReminderKey.Hash"/>, or <see langword="null"/>.
    /// </summary>
    Task<StoredReminder?> ReadAsync(ReminderKey key, CancellationToken cancellationToken);

    /// <summary>
    /// Reads the rows whose grain key hash is <paramref name="grainKeyHash"/>.
    /// </summary>
    Task<IReadOnlyList<StoredReminder>> ReadGrainAsync(byte[] grainKeyHash, CancellationToken cancellationToken);

    /// <summary>
    /// Reads the rows of a service whose grain hash lies on the ring range <c>(begin, end]</c>. When
    /// <paramref name="begin"/> is not less than <paramref name="end"/>, the range wraps around: hashes above
    /// <paramref name="begin"/> or at most <paramref name="end"/>, so <c>(x, x]</c> is the whole ring.
    /// </summary>
    Task<IReadOnlyList<StoredReminder>> ReadRangeAsync(byte[] serviceHash, uint begin, uint end, CancellationToken cancellationToken);

    /// <summary>
    /// Inserts a row. Returns <see langword="false"/> when the statement wrote nothing because a row with the same hash exists.
    /// </summary>
    Task<bool> TryInsertAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken);

    /// <summary>
    /// Replaces the schedule and ETag of the row whose hash and identity match. Returns whether the statement matched a row.
    /// </summary>
    Task<bool> TryUpdateAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the row whose hash, identity and ETag all match. Returns whether the statement matched a row.
    /// </summary>
    Task<bool> TryDeleteAsync(ReminderKey key, byte[] expectedETag, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes every row of a service.
    /// </summary>
    Task ClearAsync(byte[] serviceHash, CancellationToken cancellationToken);
}

/// <param name="Identity">The identity bytes stored with the row (<see cref="ReminderKey"/>).</param>
/// <param name="StartAt">The first due time, in UTC ticks.</param>
/// <param name="Period">The period, in ticks.</param>
/// <param name="ETag">The current ETag of the row.</param>
internal sealed record StoredReminder(byte[] Identity, long StartAt, long Period, byte[] ETag);
