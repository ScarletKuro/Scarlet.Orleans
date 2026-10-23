using System.Data.Common;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

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
/// <param name="isDuplicateKeyError">The application's rule for recognising duplicate keys (<see cref="ScarletEntityFrameworkCoreGrainStorageOptions.IsDuplicateKeyError"/>).</param>
internal sealed class EntityFrameworkCoreGrainStateStore<TContext>(
    IDbContextFactory<TContext> contextFactory,
    SchemaMode schemaMode,
    TimeProvider timeProvider,
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
            await ValidateSchemaAsync(context, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Checks the versions recorded in <c>orleans_schema_history</c>, whichever flavour recorded them.
    /// </summary>
    private static async Task ValidateSchemaAsync(TContext context, CancellationToken cancellationToken)
    {
        List<int> versions;
        try
        {
            versions = await SchemaHistoryModel.Set(context)
                .Where(row => EF.Property<string>(row, SchemaHistoryModel.Component) == PersistenceSchema.Component)
                .Select(row => EF.Property<int>(row, SchemaHistoryModel.Version))
                .OrderBy(version => version)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            throw new SchemaVersionException(
                $"Cannot read the {PersistenceSchema.Component} schema history. Create the Orleans tables with an EF Core migration that calls migrationBuilder.{nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceSchema)}({PersistenceSchema.LatestVersion}).",
                exception);
        }

        if (versions.Count == 0)
        {
            throw new SchemaVersionException(
                $"No {PersistenceSchema.Component} schema version is recorded. Call migrationBuilder.{nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceSchema)}({PersistenceSchema.LatestVersion}) in the migration that creates the Orleans tables.");
        }

        for (var i = 0; i < versions.Count; i++)
        {
            if (versions[i] != i + 1)
            {
                throw new SchemaVersionException($"The {PersistenceSchema.Component} schema history is missing version {i + 1}.");
            }
        }

        var current = versions[^1];
        if (current > PersistenceSchema.LatestVersion)
        {
            throw new SchemaVersionException(
                $"The {PersistenceSchema.Component} schema is at version {current}, newer than the latest version {PersistenceSchema.LatestVersion} this code supports. Upgrade the package.");
        }

        if (current < PersistenceSchema.MinimumVersion)
        {
            throw new SchemaVersionException(
                $"The {PersistenceSchema.Component} schema is at version {current}, and this code needs at least version {PersistenceSchema.MinimumVersion}. Add an EF Core migration that calls migrationBuilder.{nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceSchema)}({PersistenceSchema.MinimumVersion}).");
        }
    }

    /// <summary>
    /// Runs an operation exactly once. EF Core does not retry operations nested in a running execution strategy, so the
    /// strategy configured on the context is suspended for the operation.
    /// </summary>
    private sealed class SingleAttemptExecutionStrategy(DbContext context) : ExecutionStrategy(context, maxRetryCount: 0, maxRetryDelay: TimeSpan.Zero)
    {
        protected override bool ShouldRetryOn(Exception exception) => false;
    }
}
