using System.Transactions;
using Orleans;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.Transactions;
using Orleans.Transactions.Abstractions;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions;

/// <summary>
/// Implements the <see cref="ITransactionalStateStorage{TState}"/> contract for one transactional state over an
/// <see cref="ITransactionalStateStore"/>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Every <see cref="Store"/> is one database transaction, so <see cref="Load"/> never sees a partial store, and a
/// failed store changed nothing unless its commit's acknowledgement was lost, which Orleans settles by loading.</item>
/// <item>The committed state is the entry at the committed sequence number: a commit moves that number and deletes the
/// entries it made obsolete, without copying a payload.</item>
/// <item>An ETag that does not match fails with <see cref="InconsistentStateException"/>, which Orleans treats as a
/// storage conflict; a hash collision fails with <see cref="TransactionalStateKeyCollisionException"/>.</item>
/// <item>The storage keeps no state between calls, so a stale cache cannot make it write over a version it did not
/// load; the ETag alone decides.</item>
/// </list>
/// <para>
/// <typeparamref name="TState"/> is serialized with the configured <see cref="IGrainStorageSerializer"/>, as grain state
/// is. The metadata and transaction managers hold grain references, so they use the Orleans serializer.
/// </para>
/// </remarks>
internal sealed class RelationalTransactionalStateStorage<TState>(
    TransactionalStateKey key,
    ITransactionalStateStore store,
    IGrainStorageSerializer stateSerializer,
    Serializer serializer,
    TransactionalStateObserver observer,
    TimeProvider timeProvider) : ITransactionalStateStorage<TState>
    where TState : class, new()
{
    public Task<TransactionalStorageLoadResponse<TState>> Load() => observer.ObserveAsync("load", key, LoadCoreAsync);

    private async Task<TransactionalStorageLoadResponse<TState>> LoadCoreAsync()
    {
        using var scope = SuppressAmbientTransaction();
        var stored = await store.LoadAsync(key, CancellationToken.None).ConfigureAwait(false);
        if (stored is null)
        {
            return new TransactionalStorageLoadResponse<TState>();
        }

        VerifyIdentity(stored.Identity);
        var committed = stored.CommittedSequenceId == 0 ? new TState() : null;
        var pending = new List<PendingTransactionState<TState>>(stored.Entries.Count);
        foreach (var entry in stored.Entries)
        {
            if (entry.SequenceId == stored.CommittedSequenceId)
            {
                committed = DeserializeState(entry.Payload);
            }
            else if (entry.SequenceId > stored.CommittedSequenceId)
            {
                pending.Add(ToPending(entry));
            }
        }

        if (committed is null)
        {
            throw new InvalidOperationException(
                $"Transactional state corrupted. Missing record of the committed transaction (SequenceId={stored.CommittedSequenceId}) for {key}.");
        }

        return new TransactionalStorageLoadResponse<TState>(
            ETags.Format(stored.ETag),
            committed,
            stored.CommittedSequenceId,
            serializer.Deserialize<TransactionalStateMetaData>(stored.Metadata)
                ?? throw new InvalidOperationException($"Transactional state corrupted. The metadata of {key} is null."),
            pending);
    }

    public Task<string> Store(
        string? expectedETag,
        TransactionalStateMetaData metadata,
        List<PendingTransactionState<TState>>? statesToPrepare,
        long? commitUpTo,
        long? abortAfter)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return observer.ObserveAsync("store", key, () => StoreCoreAsync(expectedETag, metadata, statesToPrepare, commitUpTo, abortAfter));
    }

    private async Task<string> StoreCoreAsync(
        string? expectedETag,
        TransactionalStateMetaData metadata,
        List<PendingTransactionState<TState>>? statesToPrepare,
        long? commitUpTo,
        long? abortAfter)
    {
        using var scope = SuppressAmbientTransaction();
        byte[]? expected = null;
        if (expectedETag is not null && !ETags.TryParse(expectedETag, out expected))
        {
            // An ETag this provider never issued cannot be current.
            throw Conflict(expectedETag);
        }

        var prepared = statesToPrepare is null ? [] : statesToPrepare.ConvertAll(ToEntry);
        var etag = ETags.New(timeProvider);
        var request = new StoreRequest(expected, etag, serializer.SerializeToArray(metadata), prepared, commitUpTo, abortAfter, timeProvider.GetUtcNow());
        var result = await store.StoreAsync(key, request, CancellationToken.None).ConfigureAwait(false);
        switch (result.Status)
        {
            case StoreStatus.Stored:
                observer.RecordStore(prepared.Count, result.DeletedEntries, request.Metadata.Length + prepared.Sum(e => (long)e.Payload.Length + e.TransactionManager.Length));
                return ETags.Format(etag);

            case StoreStatus.MissingCommitRecord:
                // The message Orleans' grain storage wrapper uses for the same corruption.
                throw new InvalidOperationException($"Transactional state corrupted. Missing prepare record (SequenceId={commitUpTo}) for committed transaction.");

            default:
                if (result.StoredIdentity is { } identity)
                {
                    VerifyIdentity(identity);
                }

                throw Conflict(expectedETag);
        }
    }

    private StoredEntry ToEntry(PendingTransactionState<TState> state) =>
        new(
            state.SequenceId,
            state.TransactionId,
            UtcTicks(state.TimeStamp),
            serializer.SerializeToArray(state.TransactionManager),
            stateSerializer.Serialize(state.State).ToArray());

    private PendingTransactionState<TState> ToPending(StoredEntry entry) =>
        new()
        {
            SequenceId = entry.SequenceId,
            TransactionId = entry.TransactionId,
            TimeStamp = new DateTime(entry.TimestampTicks, DateTimeKind.Utc),
            TransactionManager = serializer.Deserialize<ParticipantId>(entry.TransactionManager),
            State = DeserializeState(entry.Payload),
        };

    private TState DeserializeState(byte[] payload) => stateSerializer.Deserialize<TState>(new BinaryData(payload)) ?? new TState();

    /// <summary>
    /// The transaction clock is UTC; a local time is converted, so the stored ticks always mean the same instant.
    /// </summary>
    private static long UtcTicks(DateTime time) => time.Kind == DateTimeKind.Local ? time.ToUniversalTime().Ticks : time.Ticks;

    private void VerifyIdentity(byte[] identity)
    {
        if (!key.Identity.AsSpan().SequenceEqual(identity))
        {
            throw new TransactionalStateKeyCollisionException(
                $"The storage key of {key} is already used by a different transactional state. No data was read or written. This is a 128-bit hash collision; please report it.");
        }
    }

    private InconsistentStateException Conflict(string? expectedETag) =>
        new($"Version conflict (Store): ProviderName={observer.Name} {key} ETag={expectedETag ?? "null"}.", storedEtag: null, currentEtag: expectedETag);

    private static TransactionScope SuppressAmbientTransaction() =>
        new(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
}

/// <summary>
/// Two distinct transactional states hash to the same storage key.
/// </summary>
internal sealed class TransactionalStateKeyCollisionException(string message) : InvalidOperationException(message);
