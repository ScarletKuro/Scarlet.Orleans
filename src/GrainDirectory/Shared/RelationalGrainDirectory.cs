using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.GrainDirectory;
using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.GrainDirectory;

/// <summary>
/// Implements the <see cref="IGrainDirectory"/> contract over an <see cref="IGrainDirectoryStore"/>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Registration is first-writer-wins: it returns the existing entry, and never replaces another activation's
/// entry, unless the caller names that entry as the previous address, in which case it is replaced in one statement.</item>
/// <item>Unregistering removes an entry only if its activation ID matches.</item>
/// <item>A row whose hash matches but whose identity does not fails the operation with
/// <see cref="GrainDirectoryKeyCollisionException"/>: no other grain's entry is returned, replaced or removed.</item>
/// </list>
/// <para>
/// Every operation runs outside any ambient <see cref="Transaction"/>, and is traced and measured under
/// <see cref="Diagnostics.Name"/>, tagged with the directory name, the operation and its outcome.
/// </para>
/// </remarks>
internal sealed partial class RelationalGrainDirectory(
    string name,
    string clusterId,
    IGrainDirectoryStore store,
    int initStage,
    TimeProvider timeProvider,
    ILogger<RelationalGrainDirectory> logger) : IGrainDirectory, ILifecycleParticipant<ISiloLifecycle>
{
    /// <summary>
    /// How many times a registration tries again when the entry it found is removed before it can be read.
    /// </summary>
    /// <remarks>
    /// Every retry means another activation of the grain registered and unregistered in between, so the directory as a
    /// whole makes progress, and a hot grain under heavy churn can need several. The bound only stops a fault, such as a
    /// duplicate-key rule that matches other errors, from looping forever.
    /// </remarks>
    private const int RegisterAttempts = 100;

    /// <summary>
    /// The most rows one statement deletes when a silo's entries are removed.
    /// </summary>
    internal const int SiloBatchSize = 500;

    private static readonly Histogram<double> Duration = Diagnostics.Meter.CreateHistogram<double>(
        "scarlet.orleans.grain_directory.duration",
        unit: "s",
        description: "The duration of grain directory operations, by directory, operation and outcome.");

    private readonly DirectoryScope scope = new(clusterId, name);

    public void Participate(ISiloLifecycle lifecycle) =>
        lifecycle.Subscribe($"{nameof(RelationalGrainDirectory)}-{name}", initStage, InitializeAsync);

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        using var transaction = SuppressAmbientTransaction();
        await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        LogInitialized(name, clusterId);
    }

    public Task<GrainAddress?> Register(GrainAddress address) => Register(address, previousAddress: null, CancellationToken.None);

    public Task<GrainAddress?> Register(GrainAddress address, CancellationToken cancellationToken) => Register(address, previousAddress: null, cancellationToken);

    public Task<GrainAddress?> Register(GrainAddress address, GrainAddress? previousAddress) => Register(address, previousAddress, CancellationToken.None);

    public Task<GrainAddress?> Register(GrainAddress address, GrainAddress? previousAddress, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentNullException.ThrowIfNull(address.SiloAddress);
        return ObserveAsync<GrainAddress?>("register", address.GrainId, async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var transaction = SuppressAmbientTransaction();
            var key = DirectoryKey.Create(scope, address.GrainId);
            var entry = DirectoryEntry.From(address);

            // Replacing the previous activation's entry is one conditional update, so no other activation can register
            // in between, as it could if the previous entry were removed first.
            if (previousAddress is not null
                && previousAddress.GrainId.Equals(address.GrainId)
                && await SettleCancellationAsync(store.TryReplaceAsync(key, DirectoryEntry.ActivationBytes(previousAddress.ActivationId), entry, cancellationToken), key, entry).ConfigureAwait(false))
            {
                return address;
            }

            for (var attempt = 0; attempt < RegisterAttempts; attempt++)
            {
                if (await SettleCancellationAsync(store.TryInsertAsync(key, entry, cancellationToken), key, entry).ConfigureAwait(false))
                {
                    return address;
                }

                // Another activation registered first, which wins. It may be unregistered before it is read, and then
                // this registration tries again.
                if (await ReadVerifiedAsync(key, cancellationToken).ConfigureAwait(false) is { } existing)
                {
                    return existing.Entry.ToAddress(address.GrainId);
                }
            }

            throw new InvalidOperationException(
                $"The registration of {key} raced with concurrent unregistrations {RegisterAttempts} times. Nothing was registered by this call.");
        });
    }

    public Task<GrainAddress?> Lookup(GrainId grainId) => Lookup(grainId, CancellationToken.None);

    public Task<GrainAddress?> Lookup(GrainId grainId, CancellationToken cancellationToken) =>
        ObserveAsync("lookup", grainId, async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            var stored = await ReadVerifiedAsync(DirectoryKey.Create(scope, grainId), cancellationToken).ConfigureAwait(false);
            return stored?.Entry.ToAddress(grainId);
        });

    public Task Unregister(GrainAddress address) => Unregister(address, CancellationToken.None);

    public Task Unregister(GrainAddress address, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(address);
        return ObserveAsync("unregister", address.GrainId, async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var transaction = SuppressAmbientTransaction();
            return await store.TryDeleteAsync(DirectoryKey.Create(scope, address.GrainId), DirectoryEntry.ActivationBytes(address.ActivationId), cancellationToken).ConfigureAwait(false);
        });
    }

    public Task UnregisterSilos(List<SiloAddress> siloAddresses) => UnregisterSilos(siloAddresses, CancellationToken.None);

    public Task UnregisterSilos(List<SiloAddress> siloAddresses, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(siloAddresses);
        return ObserveAsync("unregister_silos", grainId: null, async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            var removed = 0;
            foreach (var silo in siloAddresses)
            {
                var address = silo.ToParsableString();
                var siloHash = DirectoryEntry.SiloHashOf(address);
                // A batch can delete fewer rows than it selected, or none, when entries move to a live silo in between, so
                // only a fresh check after a batch that deleted nothing shows that the silo has none left.
                int deleted;
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    deleted = await store.DeleteSiloBatchAsync(scope.Hash, siloHash, address, SiloBatchSize, cancellationToken).ConfigureAwait(false);
                    removed += deleted;
                }
                while (deleted > 0 || await store.HasSiloEntriesAsync(scope.Hash, siloHash, address, cancellationToken).ConfigureAwait(false));
            }

            if (removed > 0)
            {
                LogUnregisteredSilos(name, removed, siloAddresses.Count);
            }

            return removed;
        });
    }

    /// <summary>
    /// Awaits a write and, if it is canceled, reports whether it committed anyway.
    /// </summary>
    /// <remarks>
    /// A token that fires while the statement is in flight makes the driver cancel it, but the server may already have
    /// committed. A read-back, which must not be canceled itself, settles it: if the row holds this activation, the
    /// registration committed and is reported, so the activation is not mistaken for a duplicate; otherwise the
    /// cancellation stands.
    /// </remarks>
    private async Task<bool> SettleCancellationAsync(Task<bool> write, DirectoryKey key, DirectoryEntry entry)
    {
        try
        {
            return await write.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            var stored = await ReadVerifiedAsync(key, CancellationToken.None).ConfigureAwait(false);
            if (stored is not null && stored.Entry.ActivationId.AsSpan().SequenceEqual(entry.ActivationId) && stored.Entry.SiloAddress == entry.SiloAddress)
            {
                return true;
            }

            throw;
        }
    }

    /// <summary>
    /// Reads the row under the key's hash, throwing <see cref="GrainDirectoryKeyCollisionException"/> if it belongs to another grain.
    /// </summary>
    private async Task<StoredDirectoryEntry?> ReadVerifiedAsync(DirectoryKey key, CancellationToken cancellationToken)
    {
        var stored = await store.ReadAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is not null && !key.Identity.AsSpan().SequenceEqual(stored.Identity))
        {
            throw new GrainDirectoryKeyCollisionException(
                $"The storage key of {key} is already used by a different grain. No entry was read or written. This is a 128-bit hash collision; please report it.");
        }

        return stored;
    }

    /// <summary>
    /// Runs one directory operation in a trace span, and records its duration and outcome.
    /// </summary>
    private async Task<T> ObserveAsync<T>(string operation, GrainId? grainId, Func<Task<T>> body)
    {
        using var activity = Diagnostics.ActivitySource.StartActivity($"grain_directory {operation}");
        activity?.SetTag("orleans.grain_directory.name", name);
        activity?.SetTag("orleans.grain_directory.operation", operation);
        if (grainId is { } id)
        {
            activity?.SetTag("orleans.grain.id", id.ToString());
        }

        var started = timeProvider.GetTimestamp();
        var outcome = "error";
        try
        {
            var result = await body().ConfigureAwait(false);
            outcome = "success";
            return result;
        }
        catch (GrainDirectoryKeyCollisionException exception)
        {
            outcome = "collision";
            LogCollision(exception, name, operation);
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
                new KeyValuePair<string, object?>("orleans.grain_directory.name", name),
                new KeyValuePair<string, object?>("orleans.grain_directory.operation", operation),
                new KeyValuePair<string, object?>("outcome", outcome));
            activity?.SetTag("outcome", outcome);
            if (outcome is not ("success" or "canceled"))
            {
                activity?.SetStatus(ActivityStatusCode.Error, outcome);
            }
        }
    }

    private static TransactionScope SuppressAmbientTransaction() =>
        new(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

    [LoggerMessage(Level = LogLevel.Information, Message = "Initialized grain directory {Name} for cluster {ClusterId}.")]
    private partial void LogInitialized(string name, string clusterId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Grain directory {Name}: removed {Count} entries of {SiloCount} silos.")]
    private partial void LogUnregisteredSilos(string name, int count, int siloCount);

    [LoggerMessage(Level = LogLevel.Error, Message = "Grain directory {Name}: {Operation} found an entry held by another grain.")]
    private partial void LogCollision(Exception exception, string name, string operation);
}

/// <summary>
/// Two distinct grains hash to the same storage key.
/// </summary>
internal sealed class GrainDirectoryKeyCollisionException(string message) : InvalidOperationException(message);
