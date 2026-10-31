using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Clustering;

/// <summary>
/// Implements the <see cref="IMembershipTable"/> contract over an <see cref="IMembershipStore"/>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Every insert and update is a compare-and-swap of the cluster's version row, made in the same transaction as the
/// member row change, so it commits exactly one version or nothing.</item>
/// <item>Reads return the version and the members from one snapshot, as the membership protocol requires.</item>
/// <item>A row whose hash matches but whose identity does not fails the operation with
/// <see cref="MembershipKeyCollisionException"/>: no other silo's or cluster's row is returned or replaced.</item>
/// </list>
/// <para>
/// Every operation runs outside any ambient <see cref="Transaction"/>, and is traced and measured under
/// <see cref="Diagnostics.Name"/>, tagged with the operation and its outcome.
/// </para>
/// </remarks>
internal sealed partial class RelationalMembershipTable(
    string clusterId,
    IMembershipStore store,
    TimeProvider timeProvider,
    ILogger<RelationalMembershipTable> logger) : IMembershipTable
{
    private static readonly Histogram<double> Duration = Diagnostics.Meter.CreateHistogram<double>(
        "scarlet.orleans.clustering.duration",
        unit: "s",
        description: "The duration of membership table operations, by operation and outcome.");

    private readonly ClusterScope scope = new(clusterId);

    public Task InitializeMembershipTableAsync(bool tryInitTableVersion, CancellationToken cancellationToken = default) =>
        ObserveAsync("initialize", async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
            if (tryInitTableVersion
                && await store.TryCreateVersionAsync(scope, ETags.New(timeProvider), cancellationToken).ConfigureAwait(false) is { } existing)
            {
                Verify(existing);
            }

            LogInitialized(clusterId);
            return true;
        });

    public Task DeleteMembershipTableEntriesAsync(string clusterId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        return ObserveAsync("delete_cluster", async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            await store.DeleteClusterAsync(new ClusterScope(clusterId), cancellationToken).ConfigureAwait(false);
            return true;
        });
    }

    public Task CleanupDefunctSiloEntriesAsync(DateTimeOffset beforeDate, CancellationToken cancellationToken = default) =>
        ObserveAsync("cleanup", async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            var removed = await store.DeleteDefunctAsync(scope, beforeDate.UtcTicks, cancellationToken).ConfigureAwait(false);
            if (removed > 0)
            {
                LogCleanedUp(clusterId, removed, beforeDate);
            }

            return removed;
        });

    public Task<MembershipTableData> ReadRowAsync(SiloAddress key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return ObserveAsync("read_row", async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            var identity = scope.MemberIdentity(key.ToParsableString());
            var snapshot = await store.ReadAsync(scope, StorageIdentity.Hash(identity), cancellationToken).ConfigureAwait(false);
            var version = VersionOf(snapshot);
            if (snapshot.Members is [var member])
            {
                if (!member.Row.Identity.AsSpan().SequenceEqual(identity))
                {
                    throw Collision($"silo {key}");
                }

                return new MembershipTableData(Tuple.Create(member.Row.ToEntry(), ETags.Format(member.ETag)), version);
            }

            return new MembershipTableData(version);
        });
    }

    public Task<MembershipTableData> ReadAllAsync(CancellationToken cancellationToken = default) =>
        ObserveAsync("read_all", async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            var snapshot = await store.ReadAsync(scope, memberKeyHash: null, cancellationToken).ConfigureAwait(false);
            var version = VersionOf(snapshot);
            var members = new List<Tuple<MembershipEntry, string>>(snapshot.Members.Count);
            foreach (var member in snapshot.Members)
            {
                if (!scope.OwnsMember(member.Row.Identity))
                {
                    throw Collision("a member row");
                }

                members.Add(Tuple.Create(member.Row.ToEntry(), ETags.Format(member.ETag)));
            }

            return new MembershipTableData(members, version);
        });

    public Task<bool> InsertRowAsync(MembershipEntry entry, TableVersion tableVersion, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(tableVersion);
        return ObserveAsync("insert", async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            if (Change(tableVersion) is not { } version)
            {
                return false;
            }

            var row = MemberRow.From(scope, entry);
            var result = await store.TryInsertAsync(scope, version, row, ETags.New(timeProvider), cancellationToken).ConfigureAwait(false);
            if (result.ExistingIdentity is { } existing && !existing.AsSpan().SequenceEqual(row.Identity))
            {
                throw Collision($"silo {entry.SiloAddress}");
            }

            return result.Inserted;
        });
    }

    public Task<bool> UpdateRowAsync(MembershipEntry entry, string etag, TableVersion tableVersion, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(tableVersion);
        return ObserveAsync("update", async () =>
        {
            using var transaction = SuppressAmbientTransaction();

            // An ETag this provider never issued, or a version it cannot have written, matches no row.
            if (Change(tableVersion) is not { } version || !ETags.TryParse(etag, out var expectedETag))
            {
                return false;
            }

            return await store.TryUpdateAsync(scope, version, MemberRow.From(scope, entry), expectedETag, ETags.New(timeProvider), cancellationToken).ConfigureAwait(false);
        });
    }

    public Task UpdateIAmAliveAsync(MembershipEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return ObserveAsync("heartbeat", async () =>
        {
            using var transaction = SuppressAmbientTransaction();
            var identity = scope.MemberIdentity(entry.SiloAddress.ToParsableString());
            await store.UpdateIAmAliveAsync(scope, StorageIdentity.Hash(identity), identity, MemberRow.UtcTicks(entry.IAmAliveTime), cancellationToken).ConfigureAwait(false);
            return true;
        });
    }

    Task IMembershipTable.InitializeMembershipTable(bool tryInitTableVersion) => InitializeMembershipTableAsync(tryInitTableVersion);

    Task IMembershipTable.DeleteMembershipTableEntries(string clusterId) => DeleteMembershipTableEntriesAsync(clusterId);

    Task IMembershipTable.CleanupDefunctSiloEntries(DateTimeOffset beforeDate) => CleanupDefunctSiloEntriesAsync(beforeDate);

    Task<MembershipTableData> IMembershipTable.ReadRow(SiloAddress key) => ReadRowAsync(key);

    Task<MembershipTableData> IMembershipTable.ReadAll() => ReadAllAsync();

    Task<bool> IMembershipTable.InsertRow(MembershipEntry entry, TableVersion tableVersion) => InsertRowAsync(entry, tableVersion);

    Task<bool> IMembershipTable.UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion) => UpdateRowAsync(entry, etag, tableVersion);

    Task IMembershipTable.UpdateIAmAlive(MembershipEntry entry) => UpdateIAmAliveAsync(entry);

    /// <summary>
    /// The version swap for a write that Orleans makes with <c>TableVersion.Next()</c> of the version it read, or
    /// <see langword="null"/> if the caller's version cannot match any row.
    /// </summary>
    private VersionChange? Change(TableVersion tableVersion) =>
        tableVersion.Version > 0 && ETags.TryParse(tableVersion.VersionEtag, out var expectedETag)
            ? new VersionChange(tableVersion.Version - 1, expectedETag, tableVersion.Version, ETags.New(timeProvider))
            : null;

    /// <summary>
    /// The version of a snapshot, or version 0 with an ETag no row holds if the cluster has no version row, which
    /// <see cref="InitializeMembershipTableAsync"/> creates.
    /// </summary>
    private TableVersion VersionOf(MembershipSnapshot snapshot)
    {
        if (snapshot.Version is not { } version)
        {
            return new TableVersion(0, string.Empty);
        }

        Verify(version.Identity);
        return version.ToTableVersion();
    }

    private void Verify(byte[] versionIdentity)
    {
        if (!versionIdentity.AsSpan().SequenceEqual(scope.Identity))
        {
            throw Collision("the version row");
        }
    }

    private MembershipKeyCollisionException Collision(string what) =>
        new($"The storage key of {what} in {scope} is already used by a different cluster or silo. Nothing was read or written. This is a 128-bit hash collision; please report it.");

    /// <summary>
    /// Runs one membership operation in a trace span, and records its duration and outcome.
    /// </summary>
    private async Task<T> ObserveAsync<T>(string operation, Func<Task<T>> body)
    {
        using var activity = Diagnostics.ActivitySource.StartActivity($"membership {operation}");
        activity?.SetTag("orleans.cluster.id", clusterId);
        activity?.SetTag("orleans.membership.operation", operation);

        var started = timeProvider.GetTimestamp();
        var outcome = "error";
        try
        {
            var result = await body().ConfigureAwait(false);
            outcome = result is false ? "conflict" : "success";
            return result;
        }
        catch (MembershipKeyCollisionException exception)
        {
            outcome = "collision";
            LogCollision(exception, clusterId, operation);
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
                new KeyValuePair<string, object?>("orleans.membership.operation", operation),
                new KeyValuePair<string, object?>("outcome", outcome));
            activity?.SetTag("outcome", outcome);
            if (outcome is not ("success" or "conflict" or "canceled"))
            {
                activity?.SetStatus(ActivityStatusCode.Error, outcome);
            }
        }
    }

    private static TransactionScope SuppressAmbientTransaction() =>
        new(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

    [LoggerMessage(Level = LogLevel.Information, Message = "Initialized the membership table of cluster {ClusterId}.")]
    private partial void LogInitialized(string clusterId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Membership of cluster {ClusterId}: removed {Count} dead silos older than {BeforeDate}.")]
    private partial void LogCleanedUp(string clusterId, int count, DateTimeOffset beforeDate);

    [LoggerMessage(Level = LogLevel.Error, Message = "Membership of cluster {ClusterId}: {Operation} found a row held by another cluster or silo.")]
    private partial void LogCollision(Exception exception, string clusterId, string operation);
}

/// <summary>
/// Two distinct clusters or silos hash to the same storage key.
/// </summary>
internal sealed class MembershipKeyCollisionException(string message) : InvalidOperationException(message);
