using System.Transactions;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization.Serializers;
using Orleans.Storage;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence;

/// <summary>
/// Implements the <see cref="IGrainStorage"/> contract over an <see cref="IGrainStateStore"/>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>A write with a <see langword="null"/> ETag inserts, and fails if any row exists.</item>
/// <item>A write with an ETag replaces the row only if that ETag is still current.</item>
/// <item>A clear checks the ETag too: it deletes the row, or keeps it with no payload and a new ETag.</item>
/// <item>Every operation that does not match throws <see cref="InconsistentStateException"/>, except a hash collision,
/// which throws <see cref="GrainStateKeyCollisionException"/>.</item>
/// </list>
/// <para>
/// Every operation runs outside any ambient <see cref="Transaction"/>. Grain state is committed when the call returns,
/// as Orleans assumes: a caller's <see cref="TransactionScope"/> rolling back must not undo a write whose new ETag the
/// grain already holds, and the stores' retries rely on each statement autocommitting.
/// </para>
/// </remarks>
internal sealed partial class RelationalGrainStorage(
    string name,
    string serviceId,
    IGrainStateStore store,
    IGrainStorageSerializer serializer,
    IActivatorProvider activatorProvider,
    bool deleteStateOnClear,
    int initStage,
    TimeProvider timeProvider,
    ILogger<RelationalGrainStorage> logger) : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    public void Participate(ISiloLifecycle lifecycle) =>
        lifecycle.Subscribe($"{nameof(RelationalGrainStorage)}-{name}", initStage, InitializeAsync);

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        using var scope = SuppressAmbientTransaction();
        await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        LogInitialized(name, serviceId);
    }

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        ReadStateAsync(stateName, grainId, grainState, CancellationToken.None);

    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var scope = SuppressAmbientTransaction();
        var key = GrainStateKey.Create(serviceId, stateName, grainId);
        var stored = await store.ReadAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is null)
        {
            Reset(grainState, etag: null);
            return;
        }

        VerifyIdentity(key, stored);
        if (stored.Payload is null)
        {
            Reset(grainState, ETags.Format(stored.ETag));
            return;
        }

        grainState.State = serializer.Deserialize<T>(new BinaryData(stored.Payload)) ?? CreateInstance<T>();
        grainState.ETag = ETags.Format(stored.ETag);
        grainState.RecordExists = true;
    }

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        WriteStateAsync(stateName, grainId, grainState, CancellationToken.None);

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        // A canceled call does no work and leaves the grain state untouched. Once a write commits, the result is reported
        // whatever happens to the token, because the grain must learn its new ETag.
        cancellationToken.ThrowIfCancellationRequested();
        using var scope = SuppressAmbientTransaction();
        var key = GrainStateKey.Create(serviceId, stateName, grainId);
        var payload = serializer.Serialize(grainState.State).ToArray();
        var etag = ETags.New(timeProvider);
        var now = timeProvider.GetUtcNow();

        bool written;
        if (grainState.ETag is null)
        {
            written = await store.TryInsertAsync(key, payload, etag, now, cancellationToken).ConfigureAwait(false);
        }
        else if (ETags.TryParse(grainState.ETag, out var expected))
        {
            written = await store.TryUpdateAsync(key, expected, payload, etag, now, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // An ETag this provider never issued cannot be current.
            written = false;
        }

        if (!written && await CreateConflictAsync("Write", key, grainState.ETag, etag, cancellationToken).ConfigureAwait(false) is { } conflict)
        {
            throw conflict;
        }

        grainState.ETag = ETags.Format(etag);
        grainState.RecordExists = true;
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        ClearStateAsync(stateName, grainId, grainState, CancellationToken.None);

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var scope = SuppressAmbientTransaction();
        var key = GrainStateKey.Create(serviceId, stateName, grainId);
        if (grainState.ETag is null)
        {
            // Nothing was ever written, as far as this grain knows: that holds only if there is no row.
            var stored = await store.ReadAsync(key, cancellationToken).ConfigureAwait(false);
            if (stored is not null)
            {
                VerifyIdentity(key, stored);
                throw CreateConflict("Clear", key, ETags.Format(stored.ETag), grainState.ETag);
            }

            Reset(grainState, etag: null);
            return;
        }

        if (!ETags.TryParse(grainState.ETag, out var expected))
        {
            throw (await CreateConflictAsync("Clear", key, grainState.ETag, proposedETag: null, cancellationToken).ConfigureAwait(false))!;
        }

        if (deleteStateOnClear)
        {
            if (!await store.TryDeleteAsync(key, expected, cancellationToken).ConfigureAwait(false))
            {
                throw (await CreateConflictAsync("Clear", key, grainState.ETag, proposedETag: null, cancellationToken).ConfigureAwait(false))!;
            }

            Reset(grainState, etag: null);
            return;
        }

        var etag = ETags.New(timeProvider);
        if (!await store.TryUpdateAsync(key, expected, payload: null, etag, timeProvider.GetUtcNow(), cancellationToken).ConfigureAwait(false)
            && await CreateConflictAsync("Clear", key, grainState.ETag, etag, cancellationToken).ConfigureAwait(false) is { } conflict)
        {
            throw conflict;
        }

        Reset(grainState, ETags.Format(etag));
    }

    /// <summary>
    /// Explains a write that matched no row: a hash collision if another identity holds the key, otherwise a stale ETag.
    /// Returns <see langword="null"/> when the row holds <paramref name="proposedETag"/>, the ETag this call generated.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A row holding the proposed ETag means this write committed and a later attempt of the same statement found it
    /// already applied, for example a driver or EF Core retry after the acknowledgement of a committed write was lost.
    /// No other writer can produce that ETag, so the write succeeded and is not a conflict (design section 4.7).
    /// </para>
    /// <para>Only failed writes pay for this read.</para>
    /// </remarks>
    private async Task<Exception?> CreateConflictAsync(string operation, GrainStateKey key, string? expectedETag, byte[]? proposedETag, CancellationToken cancellationToken)
    {
        var stored = await store.ReadAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is not null)
        {
            VerifyIdentity(key, stored);
            if (proposedETag is not null && stored.ETag.AsSpan().SequenceEqual(proposedETag))
            {
                return null;
            }
        }

        return CreateConflict(operation, key, stored is null ? null : ETags.Format(stored.ETag), expectedETag);
    }

    private InconsistentStateException CreateConflict(string operation, GrainStateKey key, string? storedETag, string? expectedETag) =>
        new($"Version conflict ({operation}): ProviderName={name} {key} ETag={expectedETag ?? "null"} StoredETag={storedETag ?? "none"}.", storedETag, expectedETag);

    private static void VerifyIdentity(GrainStateKey key, StoredGrainState stored)
    {
        if (!key.Identity.AsSpan().SequenceEqual(stored.Identity))
        {
            throw new GrainStateKeyCollisionException(
                $"The storage key of {key} is already used by a different grain state. No data was read or written. This is a 128-bit hash collision; please report it.");
        }
    }

    private static TransactionScope SuppressAmbientTransaction() =>
        new(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

    private void Reset<T>(IGrainState<T> grainState, string? etag)
    {
        grainState.State = CreateInstance<T>();
        grainState.ETag = etag;
        grainState.RecordExists = false;
    }

    private T CreateInstance<T>() => activatorProvider.GetActivator<T>().Create();

    [LoggerMessage(Level = LogLevel.Information, Message = "Initialized grain storage {Name} for service {ServiceId}.")]
    private partial void LogInitialized(string name, string serviceId);
}

/// <summary>
/// Two distinct grain states hash to the same storage key.
/// </summary>
internal sealed class GrainStateKeyCollisionException(string message) : InvalidOperationException(message);
