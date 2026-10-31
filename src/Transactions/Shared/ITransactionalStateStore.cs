namespace Scarlet.Orleans.Transactions;

/// <summary>
/// The atomic database operations behind transactional state storage.
/// </summary>
/// <remarks>
/// <para>
/// Stores hold no Orleans logic: serialization, identity verification and the mapping to Orleans types live in
/// <see cref="RelationalTransactionalStateStorage{TState}"/>, which both flavours share. A row found by its hash is
/// returned whatever its identity.
/// </para>
/// <para>
/// <b>Lock order.</b> A store writes the state row first, in the same transaction as its entries, so writers of one state
/// are serialized on that row, the entries need no concurrency control of their own, and no two transactions take locks
/// in opposite orders (design section 5.5).
/// </para>
/// <para>
/// <b>Isolation.</b> Stores run at <c>READ COMMITTED</c>, set explicitly, because MySQL and MariaDB otherwise take
/// next-key locks that reach into the neighbouring state's entries. Loads run at <c>REPEATABLE READ</c>, state row
/// first: one snapshot on PostgreSQL, MySQL, MariaDB and SQLite, and on SQL Server a shared lock on the state row held to
/// the end, so no store can commit between the state row and its entries.
/// </para>
/// <para>
/// <b>Retries.</b> A store repeats an operation only when the server reported that it was not applied
/// (<c>TransientRetry</c>), after its transaction rolled back. It never repeats one after a connection failure, whose
/// outcome is unknown; Orleans loads the state again after any failed store.
/// </para>
/// </remarks>
internal interface ITransactionalStateStore
{
    /// <summary>
    /// Resolves the engine and checks or applies the schema. Called before any other method.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Reads the state row stored under <see cref="TransactionalStateKey.Hash"/>, whatever its identity, and its entries
    /// from the committed sequence number on, in sequence order, as one snapshot. Returns <see langword="null"/> if there
    /// is no state row.
    /// </summary>
    Task<StoredTransactionalState?> LoadAsync(TransactionalStateKey key, CancellationToken cancellationToken);

    /// <summary>
    /// Applies <paramref name="request"/> in one transaction: replaces the state row if its identity and ETag match, or
    /// inserts it if <see cref="StoreRequest.ExpectedETag"/> is <see langword="null"/> and there is none; then deletes the
    /// entries above <see cref="StoreRequest.AbortAfter"/>, writes the prepared entries over any with the same sequence
    /// numbers, advances the committed sequence number to <see cref="StoreRequest.CommitUpTo"/> if that is later, and
    /// deletes the entries the commit made obsolete. Changes nothing unless it returns <see cref="StoreStatus.Stored"/>.
    /// </summary>
    Task<StoreResult> StoreAsync(TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken);
}

/// <param name="SequenceId">The local sequence number of the transaction that prepared this version.</param>
/// <param name="TransactionId">The transaction's globally unique ID.</param>
/// <param name="TimestampTicks">The transaction's logical timestamp, in UTC ticks.</param>
/// <param name="TransactionManager">The serialized transaction manager; a default one when this state is the manager.</param>
/// <param name="Payload">The serialized state after the transaction.</param>
internal sealed record StoredEntry(long SequenceId, string TransactionId, long TimestampTicks, byte[] TransactionManager, byte[] Payload);

/// <param name="Identity">The identity bytes stored with the state row, compared with the requested key.</param>
/// <param name="ETag">The state row's current ETag.</param>
/// <param name="CommittedSequenceId">The sequence number of the committed version, or 0 if none is committed.</param>
/// <param name="Metadata">The serialized transaction metadata.</param>
/// <param name="Entries">The committed entry, if any, then the pending entries, in sequence order.</param>
internal sealed record StoredTransactionalState(byte[] Identity, byte[] ETag, long CommittedSequenceId, byte[] Metadata, IReadOnlyList<StoredEntry> Entries);

/// <param name="ExpectedETag">The ETag the state row must hold, or <see langword="null"/> to insert it.</param>
/// <param name="ETag">The state row's new ETag.</param>
/// <param name="Metadata">The serialized metadata replacing the stored metadata.</param>
/// <param name="Prepared">The entries to add, each replacing any with the same sequence number.</param>
/// <param name="CommitUpTo">The sequence number to commit through, or <see langword="null"/>.</param>
/// <param name="AbortAfter">The sequence number above which pending entries are removed, or <see langword="null"/>.</param>
/// <param name="ModifiedOn">The time recorded on the state row.</param>
internal sealed record StoreRequest(
    byte[]? ExpectedETag,
    byte[] ETag,
    byte[] Metadata,
    IReadOnlyList<StoredEntry> Prepared,
    long? CommitUpTo,
    long? AbortAfter,
    DateTimeOffset ModifiedOn);

internal enum StoreStatus
{
    /// <summary>The request was applied.</summary>
    Stored,

    /// <summary>No state row with the expected identity and ETag, or one exists where none was expected.</summary>
    Conflict,

    /// <summary>The request commits through a sequence number that has no entry.</summary>
    MissingCommitRecord,
}

/// <param name="Status">The outcome.</param>
/// <param name="StoredIdentity">On a conflict, the identity of the state row under the key's hash, if there is one.</param>
/// <param name="DeletedEntries">The number of entries deleted, when stored.</param>
internal readonly record struct StoreResult(StoreStatus Status, byte[]? StoredIdentity = null, int DeletedEntries = 0);
