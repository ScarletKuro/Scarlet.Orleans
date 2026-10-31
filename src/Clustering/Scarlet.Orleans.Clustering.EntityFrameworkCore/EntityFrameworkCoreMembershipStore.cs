using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.EntityFrameworkCore;

namespace Scarlet.Orleans.Clustering.EntityFrameworkCore;

/// <summary>
/// Membership operations through EF Core APIs, with no provider special-cased, each in its own transaction.
/// </summary>
/// <remarks>
/// <para>
/// Every operation uses its own short-lived context, because Orleans calls the membership table concurrently and a
/// <see cref="DbContext"/> is not thread-safe. Conditional updates and deletes are <c>ExecuteUpdate</c> and
/// <c>ExecuteDelete</c>, so the change tracker never turns them into a read followed by a write.
/// </para>
/// <para>
/// Writes run in a <see cref="SingleAttemptExecutionStrategy"/>, so a retrying execution strategy configured on the
/// context never repeats them, and are repeated only when the server reported that the attempt was not applied
/// (<see cref="TransientRetry"/>), as the <see cref="IMembershipStore"/> contract requires. Reads keep the context's
/// strategy, since repeating them is safe.
/// </para>
/// </remarks>
/// <param name="contextFactory">Creates the short-lived context of each operation.</param>
/// <param name="schemaMode">What to check about the schema at startup.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
/// <param name="isDuplicateKeyError">The application's rule for recognising duplicate keys (<see cref="ScarletEntityFrameworkCoreClusteringOptions.IsDuplicateKeyError"/>).</param>
internal sealed class EntityFrameworkCoreMembershipStore<TContext>(
    IDbContextFactory<TContext> contextFactory,
    SchemaMode schemaMode,
    TimeProvider timeProvider,
    ILogger logger,
    Func<DbException, bool>? isDuplicateKeyError = null)
    : IMembershipStore
    where TContext : DbContext
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        if (context.Model.FindEntityType(typeof(ClusterMemberRecord)) is null)
        {
            throw new InvalidOperationException(
                $"{typeof(TContext).Name} does not map the Orleans clustering tables. Call modelBuilder.{nameof(ScarletClusteringModelBuilderExtensions.ApplyOrleansClusteringModel)}() in OnModelCreating.");
        }

        if (schemaMode == SchemaMode.Validate)
        {
            await SchemaValidation.ValidateAsync(
                context,
                ClusteringSchema.Component,
                ClusteringSchema.MinimumVersion,
                ClusteringSchema.LatestVersion,
                nameof(ScarletClusteringModelBuilderExtensions.ApplyOrleansClusteringSchema),
                cancellationToken).ConfigureAwait(false);

            // Only a warning depends on the provider; the schema is the application's to change.
            if (context.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
            {
                await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await SqliteJournal.EnsureWriteAheadLogAsync(context.Database.GetDbConnection(), enable: false, logger, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public Task<byte[]?> TryCreateVersionAsync(ClusterScope scope, byte[] etag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            DbUpdateException failure;
            await using (var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false))
            {
                context.Set<ClusterVersionRecord>().Add(new ClusterVersionRecord
                {
                    ClusterHash = scope.Hash,
                    ClusterIdentity = scope.Identity,
                    ClusterId = scope.ClusterIdText,
                    Version = 0,
                    ETag = etag,
                });

                try
                {
                    await new SingleAttemptExecutionStrategy(context).ExecuteAsync(context.SaveChangesAsync, ct).ConfigureAwait(false);
                    return null;
                }
                catch (DbUpdateException exception)
                {
                    failure = exception;
                }
            }

            // The row exists, or the failure is not recognised; either way a row under the key settles it. Version rows
            // are only ever deleted with their cluster.
            var existing = await ReadVersionIdentityAsync(scope, ct).ConfigureAwait(false);
            if (existing is null || InsertErrors.IsDuplicateKey(failure.InnerException ?? failure, isDuplicateKeyError) == false)
            {
                ExceptionDispatchInfo.Throw(failure);
            }

            return existing;
        }, cancellationToken);

    public Task<MembershipSnapshot> ReadAsync(ClusterScope scope, byte[]? memberKeyHash, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.RepeatableRead, write: false, async (context, ct) =>
        {
            var clusterHash = scope.Hash;
            var version = await context.Set<ClusterVersionRecord>()
                .AsNoTracking()
                .Where(v => v.ClusterHash == clusterHash)
                .Select(v => new StoredVersion(v.ClusterIdentity, v.Version, v.ETag))
                .SingleOrDefaultAsync(ct)
                .ConfigureAwait(false);

            // Members exist only with a version row. An absent row is not locked on SQL Server, so members read now could
            // belong to a version created after it.
            if (version is null)
            {
                return (new MembershipSnapshot(null, []), true);
            }

            var members = context.Set<ClusterMemberRecord>().AsNoTracking().Where(m => m.ClusterHash == clusterHash);
            if (memberKeyHash is not null)
            {
                members = members.Where(m => m.KeyHash == memberKeyHash);
            }

            var rows = await members.Select(m => new StoredMember(ToRow(m), m.ETag)).ToListAsync(ct).ConfigureAwait(false);
            return (new MembershipSnapshot(version, rows), true);
        }, cancellationToken);

    public Task<MemberInsertResult> TryInsertAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] etag, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, write: true, async (context, ct) =>
        {
            if (!await SwapVersionAsync(context, scope, version, ct).ConfigureAwait(false))
            {
                return (default(MemberInsertResult), false);
            }

            // The version row is locked, and every writer of this cluster's members locks it first, so no row can
            // appear under the key between this check and the insert.
            var clusterHash = scope.Hash;
            var keyHash = row.KeyHash;
            var existing = await context.Set<ClusterMemberRecord>()
                .AsNoTracking()
                .Where(m => m.ClusterHash == clusterHash && m.KeyHash == keyHash)
                .Select(m => m.MemberIdentity)
                .FirstOrDefaultAsync(ct)
                .ConfigureAwait(false);
            if (existing is not null)
            {
                return (new MemberInsertResult(false, existing), false);
            }

            context.Set<ClusterMemberRecord>().Add(new ClusterMemberRecord
            {
                ClusterHash = clusterHash,
                KeyHash = keyHash,
                MemberIdentity = row.Identity,
                SiloAddress = row.SiloAddress,
                Status = row.Status,
                ProxyPort = row.ProxyPort,
                HostName = row.HostName,
                SiloName = row.SiloName,
                RoleName = row.RoleName,
                UpdateZone = row.UpdateZone,
                FaultZone = row.FaultZone,
                SuspectTimes = row.SuspectTimes,
                LatestSuspectTicks = row.LatestSuspectTicks,
                StartTicks = row.StartTicks,
                IAmAliveTicks = row.IAmAliveTicks,
                ETag = etag,
            });
            await context.SaveChangesAsync(ct).ConfigureAwait(false);
            return (new MemberInsertResult(true, null), true);
        }, cancellationToken);

    /// <remarks>
    /// The heartbeat time only moves forward, as with <see cref="UpdateIAmAliveAsync"/>, so an update made with an entry
    /// read before the silo's latest heartbeat does not make it look older than it is.
    /// </remarks>
    public Task<bool> TryUpdateAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] expectedETag, byte[] etag, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, write: true, async (context, ct) =>
        {
            if (!await SwapVersionAsync(context, scope, version, ct).ConfigureAwait(false))
            {
                return (false, false);
            }

            var iAmAlive = row.IAmAliveTicks;
            var updated = await Member(context, scope, row.KeyHash, row.Identity)
                .Where(m => m.ETag == expectedETag)
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(m => m.Status, row.Status)
                        .SetProperty(m => m.ProxyPort, row.ProxyPort)
                        .SetProperty(m => m.HostName, row.HostName)
                        .SetProperty(m => m.SiloName, row.SiloName)
                        .SetProperty(m => m.RoleName, row.RoleName)
                        .SetProperty(m => m.UpdateZone, row.UpdateZone)
                        .SetProperty(m => m.FaultZone, row.FaultZone)
                        .SetProperty(m => m.SuspectTimes, row.SuspectTimes)
                        .SetProperty(m => m.LatestSuspectTicks, row.LatestSuspectTicks)
                        .SetProperty(m => m.StartTicks, row.StartTicks)
                        .SetProperty(m => m.IAmAliveTicks, m => m.IAmAliveTicks < iAmAlive ? iAmAlive : m.IAmAliveTicks)
                        .SetProperty(m => m.ETag, etag),
                    ct)
                .ConfigureAwait(false) == 1;
            return (updated, updated);
        }, cancellationToken);

    public Task UpdateIAmAliveAsync(ClusterScope scope, byte[] memberKeyHash, byte[] memberIdentity, long iAmAliveTicks, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(
                    c => Member(context, scope, memberKeyHash, memberIdentity)
                        .Where(m => m.IAmAliveTicks < iAmAliveTicks)
                        .ExecuteUpdateAsync(setters => setters.SetProperty(m => m.IAmAliveTicks, iAmAliveTicks), c),
                    ct)
                .ConfigureAwait(false);
        }, cancellationToken);

    public Task<(int Deleted, byte[]? VersionIdentity)> DeleteDefunctAsync(ClusterScope scope, long beforeTicks, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, write: true, async (context, ct) =>
        {
            // A row created after the lock attempt is not locked, so it is not read either.
            var identity = await LockVersionAsync(context, scope, ct).ConfigureAwait(false) == 0
                ? null
                : await VersionIdentity(context, scope).FirstOrDefaultAsync(ct).ConfigureAwait(false);
            if (identity is null || !identity.AsSpan().SequenceEqual(scope.Identity))
            {
                return ((0, identity), false);
            }

            var clusterHash = scope.Hash;
            var deleted = await context.Set<ClusterMemberRecord>()
                .Where(m => m.ClusterHash == clusterHash
                    && m.Status == DeadStatus
                    && m.StartTicks < beforeTicks
                    && m.IAmAliveTicks < beforeTicks
                    && (m.LatestSuspectTicks == null || m.LatestSuspectTicks < beforeTicks))
                .ExecuteDeleteAsync(ct)
                .ConfigureAwait(false);
            return ((deleted, identity), true);
        }, cancellationToken);

    /// <remarks>
    /// The version row is deleted first, which locks it as every writer of the cluster does, and its count is reliable on
    /// every provider, unlike that of an update that changes nothing.
    /// </remarks>
    public Task<byte[]?> DeleteClusterAsync(ClusterScope scope, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, write: true, async (context, ct) =>
        {
            var clusterHash = scope.Hash;
            var clusterIdentity = scope.Identity;
            if (await context.Set<ClusterVersionRecord>()
                .Where(v => v.ClusterHash == clusterHash && v.ClusterIdentity == clusterIdentity)
                .ExecuteDeleteAsync(ct)
                .ConfigureAwait(false) == 0)
            {
                return (await VersionIdentity(context, scope).FirstOrDefaultAsync(ct).ConfigureAwait(false), false);
            }

            await context.Set<ClusterMemberRecord>().Where(m => m.ClusterHash == clusterHash).ExecuteDeleteAsync(ct).ConfigureAwait(false);
            return (default(byte[]), true);
        }, cancellationToken);

    public async Task<IReadOnlyList<StoredGateway>> ReadGatewaysAsync(ClusterScope scope, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        var clusterHash = scope.Hash;
        return await context.Set<ClusterMemberRecord>()
            .AsNoTracking()
            .Where(m => m.ClusterHash == clusterHash && m.Status == ActiveStatus && m.ProxyPort > 0)
            .Select(m => new StoredGateway(m.MemberIdentity, m.SiloAddress, m.ProxyPort))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>The stored value of <c>SiloStatus.Active</c>.</summary>
    private const int ActiveStatus = 3;

    /// <summary>The stored value of <c>SiloStatus.Dead</c>.</summary>
    private const int DeadStatus = 6;

    private async Task<byte[]?> ReadVersionIdentityAsync(ClusterScope scope, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        return await VersionIdentity(context, scope).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
    }

    private static IQueryable<byte[]> VersionIdentity(TContext context, ClusterScope scope)
    {
        var clusterHash = scope.Hash;
        return context.Set<ClusterVersionRecord>().AsNoTracking().Where(v => v.ClusterHash == clusterHash).Select(v => v.ClusterIdentity);
    }

    private static async Task<bool> SwapVersionAsync(TContext context, ClusterScope scope, VersionChange version, CancellationToken cancellationToken)
    {
        var clusterHash = scope.Hash;
        var clusterIdentity = scope.Identity;
        return await context.Set<ClusterVersionRecord>()
            .Where(v => v.ClusterHash == clusterHash
                && v.ClusterIdentity == clusterIdentity
                && v.Version == version.ExpectedVersion
                && v.ETag == version.ExpectedETag)
            .ExecuteUpdateAsync(setters => setters.SetProperty(v => v.Version, version.NewVersion).SetProperty(v => v.ETag, version.NewETag), cancellationToken)
            .ConfigureAwait(false) == 1;
    }

    /// <summary>
    /// Writes the version row without changing it, which takes its row lock on every engine, so operations that change
    /// members without a new version keep the lock order of the others. Returns the number of rows locked: the providers
    /// report matched rows, as EF's own concurrency checks require, even on MySQL.
    /// </summary>
    private static Task<int> LockVersionAsync(TContext context, ClusterScope scope, CancellationToken cancellationToken)
    {
        var clusterHash = scope.Hash;
        return context.Set<ClusterVersionRecord>()
            .Where(v => v.ClusterHash == clusterHash)
            .ExecuteUpdateAsync(setters => setters.SetProperty(v => v.Version, v => v.Version), cancellationToken);
    }

    private static IQueryable<ClusterMemberRecord> Member(TContext context, ClusterScope scope, byte[] keyHash, byte[] identity)
    {
        var clusterHash = scope.Hash;
        return context.Set<ClusterMemberRecord>().Where(m => m.ClusterHash == clusterHash && m.KeyHash == keyHash && m.MemberIdentity == identity);
    }

    private static MemberRow ToRow(ClusterMemberRecord m) =>
        new(m.KeyHash, m.MemberIdentity, m.SiloAddress, m.Status, m.ProxyPort, m.HostName, m.SiloName, m.RoleName, m.UpdateZone, m.FaultZone,
            m.SuspectTimes, m.LatestSuspectTicks, m.StartTicks, m.IAmAliveTicks);

    /// <summary>
    /// Runs <paramref name="body"/> in a transaction on its own context, and commits it if the body says so, or rolls it
    /// back. A transient error rolls the transaction back, as the transaction is disposed, before the whole operation is
    /// repeated (design section 4.7).
    /// </summary>
    private Task<T> InTransactionAsync<T>(
        IsolationLevel isolationLevel,
        bool write,
        Func<TContext, CancellationToken, Task<(T Result, bool Commit)>> body,
        CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var strategy = write ? new SingleAttemptExecutionStrategy(context) : context.Database.CreateExecutionStrategy();
            return await strategy.ExecuteAsync(
                async c =>
                {
                    await using var transaction = await context.Database.BeginTransactionAsync(isolationLevel, c).ConfigureAwait(false);
                    var (result, commit) = await body(context, c).ConfigureAwait(false);
                    if (commit)
                    {
                        await transaction.CommitAsync(c).ConfigureAwait(false);
                    }
                    else
                    {
                        await transaction.RollbackAsync(c).ConfigureAwait(false);
                    }

                    return result;
                },
                ct).ConfigureAwait(false);
        }, cancellationToken);
}
