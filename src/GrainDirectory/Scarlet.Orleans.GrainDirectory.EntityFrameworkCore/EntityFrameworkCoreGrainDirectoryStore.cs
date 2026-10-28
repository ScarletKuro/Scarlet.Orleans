using System.Data.Common;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.EntityFrameworkCore;

namespace Scarlet.Orleans.GrainDirectory.EntityFrameworkCore;

/// <summary>
/// Grain directory operations through EF Core APIs, with no provider special-cased.
/// </summary>
/// <remarks>
/// <para>
/// Every operation uses its own short-lived context, because Orleans calls the directory concurrently and a
/// <see cref="DbContext"/> is not thread-safe. Each write is one statement: an insert through <c>SaveChanges</c>, and
/// conditional updates and deletes through <c>ExecuteUpdate</c> and <c>ExecuteDelete</c>.
/// </para>
/// <para>
/// Writes run in a <see cref="SingleAttemptExecutionStrategy"/>, so a retrying execution strategy configured on the
/// context never repeats them, and are repeated only when the server reported that the attempt was not applied
/// (<see cref="TransientRetry"/>), as the <see cref="IGrainDirectoryStore"/> contract requires. Reads keep the context's
/// strategy, since repeating them is safe.
/// </para>
/// </remarks>
/// <param name="contextFactory">Creates the short-lived context of each operation.</param>
/// <param name="schemaMode">What to check about the schema at startup.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
/// <param name="isDuplicateKeyError">The application's rule for recognising duplicate keys (<see cref="ScarletEntityFrameworkCoreGrainDirectoryOptions.IsDuplicateKeyError"/>).</param>
internal sealed class EntityFrameworkCoreGrainDirectoryStore<TContext>(
    IDbContextFactory<TContext> contextFactory,
    SchemaMode schemaMode,
    TimeProvider timeProvider,
    ILogger logger,
    Func<DbException, bool>? isDuplicateKeyError = null)
    : IGrainDirectoryStore
    where TContext : DbContext
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        if (context.Model.FindEntityType(typeof(GrainDirectoryRecord)) is null)
        {
            throw new InvalidOperationException(
                $"{typeof(TContext).Name} does not map the Orleans grain directory table. Call modelBuilder.{nameof(ScarletGrainDirectoryModelBuilderExtensions.ApplyOrleansGrainDirectoryModel)}() in OnModelCreating.");
        }

        if (schemaMode == SchemaMode.Validate)
        {
            await SchemaValidation.ValidateAsync(
                context,
                GrainDirectorySchema.Component,
                GrainDirectorySchema.MinimumVersion,
                GrainDirectorySchema.LatestVersion,
                nameof(ScarletGrainDirectoryModelBuilderExtensions.ApplyOrleansGrainDirectorySchema),
                cancellationToken).ConfigureAwait(false);

            // Only a warning depends on the provider; the schema is the application's to change.
            if (context.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
            {
                await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await SqliteJournal.EnsureWriteAheadLogAsync(context.Database.GetDbConnection(), enable: false, logger, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<StoredDirectoryEntry?> ReadAsync(DirectoryKey key, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        var hash = key.Hash;
        return await context.Set<GrainDirectoryRecord>()
            .AsNoTracking()
            .Where(r => r.KeyHash == hash)
            .Select(r => new StoredDirectoryEntry(r.GrainIdentity, new DirectoryEntry(r.SiloAddress, r.SiloHash, r.ActivationId, r.MembershipVersion)))
            .SingleOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public Task<bool> TryInsertAsync(DirectoryKey key, DirectoryEntry entry, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, ct => InsertAsync(key, entry, ct), cancellationToken);

    public Task<bool> TryReplaceAsync(DirectoryKey key, byte[] expectedActivationId, DirectoryEntry entry, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(
                    c => Match(context, key, expectedActivationId).ExecuteUpdateAsync(
                        setters => setters
                            .SetProperty(r => r.SiloAddress, entry.SiloAddress)
                            .SetProperty(r => r.SiloHash, entry.SiloHash)
                            .SetProperty(r => r.ActivationId, entry.ActivationId)
                            .SetProperty(r => r.MembershipVersion, entry.MembershipVersion),
                        c),
                    ct)
                .ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task<bool> TryDeleteAsync(DirectoryKey key, byte[] expectedActivationId, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(c => Match(context, key, expectedActivationId).ExecuteDeleteAsync(c), ct)
                .ConfigureAwait(false) == 1;
        }, cancellationToken);

    /// <summary>
    /// Selects a batch of a silo's keys, then deletes those rows that still belong to the silo.
    /// </summary>
    /// <remarks>
    /// The delete tests the silo itself. A row selected while it belonged to the dead silo can be moved to a live one by
    /// a concurrent replacement before the delete locks it; a delete of the selected keys alone would then remove the live
    /// registration, which is what EF Core's translation of an ordered <c>Take</c> into a delete does on PostgreSQL. The
    /// keys are compared one by one, an equality every provider translates.
    /// </remarks>
    public Task<int> DeleteSiloBatchAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, int limit, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var keys = await context.Set<GrainDirectoryRecord>()
                .AsNoTracking()
                .Where(r => r.ScopeHash == scopeHash && r.SiloHash == siloHash && r.SiloAddress == siloAddress)
                .OrderBy(r => r.KeyHash)
                .Take(limit)
                .Select(r => r.KeyHash)
                .ToListAsync(ct)
                .ConfigureAwait(false);
            if (keys.Count == 0)
            {
                return 0;
            }

            var batch = context.Set<GrainDirectoryRecord>()
                .Where(r => r.ScopeHash == scopeHash && r.SiloHash == siloHash && r.SiloAddress == siloAddress)
                .Where(AnyKey(keys));
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(batch.ExecuteDeleteAsync, ct)
                .ConfigureAwait(false);
        }, cancellationToken);

    public async Task<bool> HasSiloEntriesAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        return await context.Set<GrainDirectoryRecord>()
            .AnyAsync(r => r.ScopeHash == scopeHash && r.SiloHash == siloHash && r.SiloAddress == siloAddress, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// <c>r =&gt; r.KeyHash == keys[0] || r.KeyHash == keys[1] || …</c>, with each key a parameter.
    /// </summary>
    /// <remarks>
    /// The comparisons are combined as a balanced tree, so the SQL nests about log2(n) levels deep rather than n, which
    /// engines limit.
    /// </remarks>
    private static Expression<Func<GrainDirectoryRecord, bool>> AnyKey(List<byte[]> keys)
    {
        var record = Expression.Parameter(typeof(GrainDirectoryRecord), "r");
        var keyHash = Expression.Property(record, nameof(GrainDirectoryRecord.KeyHash));
        return Expression.Lambda<Func<GrainDirectoryRecord, bool>>(Any(0, keys.Count), record);

        Expression Any(int start, int count)
        {
            if (count == 1)
            {
                // A captured value, rather than a constant, so EF Core sends each key as a parameter.
                var value = Expression.Field(Expression.Constant(new StrongBox<byte[]>(keys[start])), nameof(StrongBox<byte[]>.Value));
                return Expression.Equal(keyHash, value);
            }

            var half = count / 2;
            return Expression.OrElse(Any(start, half), Any(start + half, count - half));
        }
    }

    private async Task<bool> InsertAsync(DirectoryKey key, DirectoryEntry entry, CancellationToken cancellationToken)
    {
        DbUpdateException failure;
        await using (var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false))
        {
            context.Set<GrainDirectoryRecord>().Add(new GrainDirectoryRecord
            {
                KeyHash = key.Hash,
                GrainIdentity = key.Identity,
                ScopeHash = key.ScopeHash,
                SiloHash = entry.SiloHash,
                ClusterId = key.ClusterIdText,
                DirectoryName = key.DirectoryNameText,
                GrainId = key.GrainIdText,
                SiloAddress = entry.SiloAddress,
                ActivationId = entry.ActivationId,
                MembershipVersion = entry.MembershipVersion,
            });

            try
            {
                await new SingleAttemptExecutionStrategy(context).ExecuteAsync(context.SaveChangesAsync, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (DbUpdateException exception)
            {
                failure = exception;
            }
        }

        var cause = failure.InnerException ?? failure;
        switch (InsertErrors.IsDuplicateKey(cause, isDuplicateKeyError))
        {
            case true:
                return false;
            case false:
                ExceptionDispatchInfo.Throw(failure);
                break;
        }

        // A cause that cannot be recognised. An insert expects no row, so a row under the key now means it could not
        // have succeeded, and the registration reads that row. With no row, the original failure surfaces.
        if (await ReadAsync(key, cancellationToken).ConfigureAwait(false) is not null)
        {
            return false;
        }

        ExceptionDispatchInfo.Throw(failure);
        return false;
    }

    private static IQueryable<GrainDirectoryRecord> Match(TContext context, DirectoryKey key, byte[] expectedActivationId)
    {
        var hash = key.Hash;
        var identity = key.Identity;
        return context.Set<GrainDirectoryRecord>().Where(r => r.KeyHash == hash && r.GrainIdentity == identity && r.ActivationId == expectedActivationId);
    }
}
