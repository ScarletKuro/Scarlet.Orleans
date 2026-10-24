using System.Diagnostics;
using System.Diagnostics.Metrics;
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
/// <para>
/// Every operation is traced and measured under <see cref="Diagnostics.Name"/>, tagged with the storage name, the
/// operation and its outcome. Conflicts are logged at debug level, because concurrent activations make them part of
/// normal operation; collisions at error level, because they should never happen. Other failures propagate to Orleans,
/// which logs them.
/// </para>
/// <para>
/// A write whose serialized state exceeds <c>maxPayloadSize</c> fails with <see cref="GrainStatePayloadTooLargeException"/>
/// before anything is sent to the database.
/// </para>
/// </remarks>
internal sealed partial class RelationalGrainStorage(
    string name,
    string serviceId,
    IGrainStateStore store,
    IGrainStorageSerializer serializer,
    IActivatorProvider activatorProvider,
    bool deleteStateOnClear,
    int? maxPayloadSize,
    int initStage,
    TimeProvider timeProvider,
    ILogger<RelationalGrainStorage> logger) : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
{
    private static readonly Histogram<double> Duration = Diagnostics.Meter.CreateHistogram<double>(
        "scarlet.orleans.persistence.duration",
        unit: "s",
        description: "The duration of grain storage operations, by storage, operation and outcome.");

    private static readonly Histogram<long> PayloadSize = Diagnostics.Meter.CreateHistogram<long>(
        "scarlet.orleans.persistence.payload.size",
        unit: "By",
        description: "The serialized size of written grain states, by storage.");

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

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken) =>
        ObserveAsync("read", stateName, grainId, () => ReadCoreAsync(stateName, grainId, grainState, cancellationToken));

    private async Task ReadCoreAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var scope = SuppressAmbientTransaction();
        var key = GrainStateKey.Create(serviceId, stateName, grainId);
        var stored = await ReadVerifiedAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is null)
        {
            Reset(grainState, etag: null);
            return;
        }

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

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken) =>
        ObserveAsync("write", stateName, grainId, () => WriteCoreAsync(stateName, grainId, grainState, cancellationToken));

    private async Task WriteCoreAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        // A canceled call does no work and leaves the grain state untouched. Once a write commits, the result is reported
        // whatever happens to the token, because the grain must learn its new ETag.
        cancellationToken.ThrowIfCancellationRequested();
        using var scope = SuppressAmbientTransaction();
        var key = GrainStateKey.Create(serviceId, stateName, grainId);
        var payload = serializer.Serialize(grainState.State).ToArray();
        PayloadSize.Record(payload.Length, new KeyValuePair<string, object?>("orleans.storage.name", name));
        if (payload.Length > maxPayloadSize)
        {
            throw new GrainStatePayloadTooLargeException(
                $"The serialized state of {key} is {payload.Length} bytes, larger than the {maxPayloadSize} bytes that grain storage {name} accepts. Nothing was written.");
        }

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
            throw await CreateConflictAsync("Write", key, grainState.ETag, cancellationToken).ConfigureAwait(false);
        }

        if (!written)
        {
            await ResolveUnmatchedWriteAsync("Write", key, grainState.ETag, etag, cancellationToken).ConfigureAwait(false);
        }

        grainState.ETag = ETags.Format(etag);
        grainState.RecordExists = true;
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        ClearStateAsync(stateName, grainId, grainState, CancellationToken.None);

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken) =>
        ObserveAsync("clear", stateName, grainId, () => ClearCoreAsync(stateName, grainId, grainState, cancellationToken));

    private async Task ClearCoreAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var scope = SuppressAmbientTransaction();
        var key = GrainStateKey.Create(serviceId, stateName, grainId);
        if (grainState.ETag is null)
        {
            // Nothing was ever written, as far as this grain knows: that holds only if there is no row.
            if (await ReadVerifiedAsync(key, cancellationToken).ConfigureAwait(false) is { } stored)
            {
                throw CreateConflict("Clear", key, stored, grainState.ETag);
            }

            Reset(grainState, etag: null);
            return;
        }

        if (!ETags.TryParse(grainState.ETag, out var expected))
        {
            throw await CreateConflictAsync("Clear", key, grainState.ETag, cancellationToken).ConfigureAwait(false);
        }

        if (deleteStateOnClear)
        {
            if (!await store.TryDeleteAsync(key, expected, cancellationToken).ConfigureAwait(false))
            {
                throw await CreateConflictAsync("Clear", key, grainState.ETag, cancellationToken).ConfigureAwait(false);
            }

            Reset(grainState, etag: null);
            return;
        }

        var etag = ETags.New(timeProvider);
        if (!await store.TryUpdateAsync(key, expected, payload: null, etag, timeProvider.GetUtcNow(), cancellationToken).ConfigureAwait(false))
        {
            await ResolveUnmatchedWriteAsync("Clear", key, grainState.ETag, etag, cancellationToken).ConfigureAwait(false);
        }

        Reset(grainState, ETags.Format(etag));
    }

    /// <summary>
    /// Runs one storage operation in a trace span, and records its duration and outcome.
    /// </summary>
    private async Task ObserveAsync(string operation, string stateName, GrainId grainId, Func<Task> body)
    {
        using var activity = Diagnostics.ActivitySource.StartActivity($"persistence {operation}");
        activity?.SetTag("orleans.storage.name", name);
        activity?.SetTag("orleans.storage.operation", operation);
        activity?.SetTag("orleans.grain.id", grainId.ToString());
        activity?.SetTag("orleans.storage.state_name", stateName);

        var started = timeProvider.GetTimestamp();
        var outcome = "error";
        try
        {
            await body().ConfigureAwait(false);
            outcome = "success";
        }
        catch (InconsistentStateException exception)
        {
            outcome = "conflict";
            LogConflict(exception, name, operation, grainId, stateName);
            throw;
        }
        catch (GrainStateKeyCollisionException exception)
        {
            outcome = "collision";
            LogCollision(exception, name, grainId, stateName);
            throw;
        }
        catch (GrainStatePayloadTooLargeException)
        {
            outcome = "payload_too_large";
            throw;
        }
        catch (OperationCanceledException)
        {
            outcome = "canceled";
            throw;
        }
        finally
        {
            Duration.Record(
                timeProvider.GetElapsedTime(started).TotalSeconds,
                new KeyValuePair<string, object?>("orleans.storage.name", name),
                new KeyValuePair<string, object?>("orleans.storage.operation", operation),
                new KeyValuePair<string, object?>("outcome", outcome));
            activity?.SetTag("outcome", outcome);
            if (outcome is not ("success" or "conflict" or "canceled"))
            {
                activity?.SetStatus(ActivityStatusCode.Error, outcome);
            }
        }
    }

    /// <summary>
    /// Settles a write that matched no row. Returns if the row holds <paramref name="proposedETag"/>, the ETag this call
    /// generated; otherwise throws <see cref="InconsistentStateException"/>, or <see cref="GrainStateKeyCollisionException"/>
    /// if another identity holds the key.
    /// </summary>
    /// <remarks>
    /// A store reports the result of the statement it ran (<see cref="IGrainStateStore"/>). When a driver or EF Core repeats
    /// a statement whose first attempt committed but whose acknowledgement was lost, the repeat matches no row, yet the
    /// write succeeded. Only this call can have produced the proposed ETag, so finding it proves the write committed
    /// (design section 4.7). Only writes that matched no row pay for this read.
    /// </remarks>
    private async Task ResolveUnmatchedWriteAsync(string operation, GrainStateKey key, string? expectedETag, byte[] proposedETag, CancellationToken cancellationToken)
    {
        var stored = await ReadVerifiedAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is not null && stored.ETag.AsSpan().SequenceEqual(proposedETag))
        {
            return;
        }

        throw CreateConflict(operation, key, stored, expectedETag);
    }

    /// <summary>
    /// Creates the exception for a request that cannot have been applied: a delete that matched no row, or an ETag this
    /// provider never issued. Throws <see cref="GrainStateKeyCollisionException"/> instead if another identity holds the key.
    /// </summary>
    /// <remarks>
    /// A delete leaves no ETag behind to prove that it committed, so a delete that matched no row is always a conflict.
    /// </remarks>
    private async Task<InconsistentStateException> CreateConflictAsync(string operation, GrainStateKey key, string? expectedETag, CancellationToken cancellationToken) =>
        CreateConflict(operation, key, await ReadVerifiedAsync(key, cancellationToken).ConfigureAwait(false), expectedETag);

    private InconsistentStateException CreateConflict(string operation, GrainStateKey key, StoredGrainState? stored, string? expectedETag)
    {
        var storedETag = stored is null ? null : ETags.Format(stored.ETag);
        return new($"Version conflict ({operation}): ProviderName={name} {key} ETag={expectedETag ?? "null"} StoredETag={storedETag ?? "none"}.", storedETag, expectedETag);
    }

    /// <summary>
    /// Reads the row under the key's hash, throwing <see cref="GrainStateKeyCollisionException"/> if it belongs to another identity.
    /// </summary>
    private async Task<StoredGrainState?> ReadVerifiedAsync(GrainStateKey key, CancellationToken cancellationToken)
    {
        var stored = await store.ReadAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is not null)
        {
            VerifyIdentity(key, stored);
        }

        return stored;
    }

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

    [LoggerMessage(Level = LogLevel.Debug, Message = "Grain storage {Name}: {Operation} of {GrainId} state {StateName} conflicted with a newer version.")]
    private partial void LogConflict(Exception exception, string name, string operation, GrainId grainId, string stateName);

    [LoggerMessage(Level = LogLevel.Error, Message = "Grain storage {Name}: the storage key of {GrainId} state {StateName} is held by another grain state.")]
    private partial void LogCollision(Exception exception, string name, GrainId grainId, string stateName);
}

/// <summary>
/// Two distinct grain states hash to the same storage key.
/// </summary>
internal sealed class GrainStateKeyCollisionException(string message) : InvalidOperationException(message);

/// <summary>
/// A grain state serialized to more bytes than the storage's <c>MaxPayloadSize</c> allows.
/// </summary>
internal sealed class GrainStatePayloadTooLargeException(string message) : InvalidOperationException(message);
