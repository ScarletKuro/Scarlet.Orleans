using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// Grain state operations through EF Core, each one set-based statement.
/// </summary>
/// <remarks>
/// <para>
/// The insert runs the shared SQL of both flavours (<see cref="GrainStateSql.Insert"/>), because EF Core cannot
/// express an insert that tolerates an existing key without an exception the engine may not let us classify.
/// </para>
/// <para>
/// Every operation uses its own short-lived context, because Orleans calls storage concurrently and a
/// <see cref="DbContext"/> is not thread-safe. Updates and deletes use <c>ExecuteUpdate</c> and <c>ExecuteDelete</c>,
/// so the change tracker never turns a conditional write into a read followed by a write.
/// </para>
/// </remarks>
internal sealed class EntityFrameworkCoreGrainStateStore<TContext>(IDbContextFactory<TContext> contextFactory, SchemaMode schemaMode, TimeProvider timeProvider)
    : IGrainStateStore
    where TContext : DbContext
{
    private RelationalEngine engine;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        if (context.Model.FindEntityType(typeof(GrainStateRecord)) is null)
        {
            throw new InvalidOperationException(
                $"{typeof(TContext).Name} does not map the Orleans grain state table. Call modelBuilder.{nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceModel)}() in OnModelCreating.");
        }

        engine = EntityFrameworkEngines.FromProviderName(context.Database.ProviderName);
        if (schemaMode == SchemaMode.None)
        {
            return;
        }

        await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await PersistenceSchema.CreateMigrator(engine)
                .EnsureAsync(context.Database.GetDbConnection(), apply: schemaMode == SchemaMode.Apply, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            await context.Database.CloseConnectionAsync().ConfigureAwait(false);
        }
    }

    public Task<StoredGrainState?> ReadAsync(GrainStateKey key, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var hash = key.Hash;
            return await context.Set<GrainStateRecord>()
                .AsNoTracking()
                .Where(r => r.KeyHash == hash)
                .Select(r => new StoredGrainState(r.GrainIdentity, r.Payload, r.ETag))
                .SingleOrDefaultAsync(ct)
                .ConfigureAwait(false);
        }, cancellationToken);

    public Task<bool> TryInsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);

            // The parameters are built on a scratch command of the provider's connection, then handed to EF, so the
            // statement still goes through EF's logging and interceptors.
            DbParameter[] parameters;
            await using (var scratch = context.Database.GetDbConnection().CreateCommand())
            {
                GrainStateSql.AddInsertParameters(scratch, key, payload, etag, modifiedOn);
                parameters = scratch.Parameters.Cast<DbParameter>().ToArray();
                scratch.Parameters.Clear();
            }

            try
            {
                return await context.Database.ExecuteSqlRawAsync(GrainStateSql.Insert(engine), parameters, ct).ConfigureAwait(false) == 1;
            }
            catch (DbException exception) when (DbErrors.IsDuplicateKey(engine, exception))
            {
                return false;
            }
        }, cancellationToken);

    public Task<bool> TryUpdateAsync(GrainStateKey key, byte[] expectedETag, byte[]? payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var modifiedOnUtc = modifiedOn.UtcDateTime;
            var updated = await Match(context, key, expectedETag)
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(r => r.Payload, payload)
                        .SetProperty(r => r.ETag, etag)
                        .SetProperty(r => r.ModifiedOn, modifiedOnUtc),
                    ct)
                .ConfigureAwait(false);
            return updated == 1;
        }, cancellationToken);

    public Task<bool> TryDeleteAsync(GrainStateKey key, byte[] expectedETag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            return await Match(context, key, expectedETag).ExecuteDeleteAsync(ct).ConfigureAwait(false) == 1;
        }, cancellationToken);

    private static IQueryable<GrainStateRecord> Match(TContext context, GrainStateKey key, byte[] expectedETag)
    {
        var hash = key.Hash;
        var identity = key.Identity;
        return context.Set<GrainStateRecord>().Where(r => r.KeyHash == hash && r.GrainIdentity == identity && r.ETag == expectedETag);
    }
}
