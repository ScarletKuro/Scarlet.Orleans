using System.Data.Common;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.EntityFrameworkCore;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// Grain state operations through EF Core APIs, with no provider special-cased.
/// </summary>
/// <remarks>
/// <para>
/// Every operation uses its own short-lived context, because Orleans calls storage concurrently and a
/// <see cref="DbContext"/> is not thread-safe. Each write is one statement: an insert through <c>SaveChanges</c>, and
/// conditional updates and deletes through <c>ExecuteUpdate</c> and <c>ExecuteDelete</c>, so the change tracker never
/// turns a conditional write into a read followed by a write.
/// </para>
/// <para>
/// Writes run in a <see cref="SingleAttemptExecutionStrategy"/>, so a retrying execution strategy configured on the
/// context never repeats them. A repeat after a connection failure would run after an attempt of unknown outcome, and if
/// another writer moved the row on in between, nothing could tell whether this write had committed. Writes are repeated
/// only when the server reported that the attempt was not applied (<see cref="TransientRetry"/>), as the
/// <see cref="IGrainStateStore"/> contract requires. Reads keep the context's strategy, since repeating them is safe.
/// </para>
/// </remarks>
/// <param name="contextFactory">Creates the short-lived context of each operation.</param>
/// <param name="schemaMode">What to check about the schema at startup.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
/// <param name="isDuplicateKeyError">The application's rule for recognising duplicate keys (<see cref="ScarletEntityFrameworkCoreGrainStorageOptions.IsDuplicateKeyError"/>).</param>
internal sealed class EntityFrameworkCoreGrainStateStore<TContext>(
    IDbContextFactory<TContext> contextFactory,
    SchemaMode schemaMode,
    TimeProvider timeProvider,
    ILogger logger,
    Func<DbException, bool>? isDuplicateKeyError = null)
    : IGrainStateStore
    where TContext : DbContext
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        if (context.Model.FindEntityType(typeof(GrainStateRecord)) is null)
        {
            throw new InvalidOperationException(
                $"{typeof(TContext).Name} does not map the Orleans grain state table. Call modelBuilder.{nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceModel)}() in OnModelCreating.");
        }

        if (schemaMode == SchemaMode.Validate)
        {
            await SchemaValidation.ValidateAsync(
                context,
                PersistenceSchema.Component,
                PersistenceSchema.MinimumVersion,
                PersistenceSchema.LatestVersion,
                nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceSchema),
                cancellationToken).ConfigureAwait(false);

            // Only a warning depends on the provider; the schema is the application's to change.
            if (context.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
            {
                await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await SqliteJournal.EnsureWriteAheadLogAsync(context.Database.GetDbConnection(), enable: false, logger, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<StoredGrainState?> ReadAsync(GrainStateKey key, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        var hash = key.Hash;
        return await context.Set<GrainStateRecord>()
            .AsNoTracking()
            .Where(r => r.KeyHash == hash)
            .Select(r => new StoredGrainState(r.GrainIdentity, r.Payload, r.ETag))
            .SingleOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public Task<bool> TryInsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, ct => InsertAsync(key, payload, etag, modifiedOn, ct), cancellationToken);

    public Task<bool> TryUpdateAsync(GrainStateKey key, byte[] expectedETag, byte[]? payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var modifiedOnUtc = modifiedOn.UtcDateTime;
            var updated = await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(
                    c => Match(context, key, expectedETag).ExecuteUpdateAsync(
                        setters => setters
                            .SetProperty(r => r.Payload, payload)
                            .SetProperty(r => r.ETag, etag)
                            .SetProperty(r => r.ModifiedOn, modifiedOnUtc),
                        c),
                    ct)
                .ConfigureAwait(false);
            return updated == 1;
        }, cancellationToken);

    public Task<bool> TryDeleteAsync(GrainStateKey key, byte[] expectedETag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(c => Match(context, key, expectedETag).ExecuteDeleteAsync(c), ct)
                .ConfigureAwait(false) == 1;
        }, cancellationToken);

    private async Task<bool> InsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken)
    {
        DbUpdateException failure;
        await using (var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false))
        {
            context.Set<GrainStateRecord>().Add(new GrainStateRecord
            {
                KeyHash = key.Hash,
                GrainIdentity = key.Identity,
                ServiceId = key.ServiceIdText,
                StateName = key.StateNameText,
                GrainId = key.GrainIdText,
                Payload = payload,
                ETag = etag,
                ModifiedOn = modifiedOn.UtcDateTime,
            });

            try
            {
                await new SingleAttemptExecutionStrategy(context).ExecuteAsync(c => context.SaveChangesAsync(c), cancellationToken).ConfigureAwait(false);
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
        // have succeeded: a conflict, or this insert committed, which the storage settles from the ETag. With no row,
        // the original failure surfaces, as it would without this check; that includes a conflicting row deleted in
        // between, so the check only ever turns errors into correct answers.
        if (await ExistsAsync(key, cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        ExceptionDispatchInfo.Throw(failure);
        return false;
    }

    private async Task<bool> ExistsAsync(GrainStateKey key, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        var hash = key.Hash;
        return await context.Set<GrainStateRecord>().AnyAsync(r => r.KeyHash == hash, cancellationToken).ConfigureAwait(false);
    }

    private static IQueryable<GrainStateRecord> Match(TContext context, GrainStateKey key, byte[] expectedETag)
    {
        var hash = key.Hash;
        var identity = key.Identity;
        return context.Set<GrainStateRecord>().Where(r => r.KeyHash == hash && r.GrainIdentity == identity && r.ETag == expectedETag);
    }
}
