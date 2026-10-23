using System.Data;
using System.Data.Common;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence.AdoNet;

/// <summary>
/// Grain state operations as plain SQL (<see cref="GrainStateSql"/>).
/// </summary>
/// <remarks>
/// Every statement autocommits on its own, so a transient server error means nothing was applied and the statement
/// is retried as a whole.
/// </remarks>
internal sealed class AdoNetGrainStateStore(DbDataSource dataSource, string? engineName, SchemaMode schemaMode, TimeProvider timeProvider) : IGrainStateStore
{
    private RelationalEngine engine;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        engine = engineName is null ? RelationalEngines.FromConnection(connection) : RelationalEngines.Parse(engineName);
        if (schemaMode != SchemaMode.None)
        {
            await PersistenceSchema.CreateMigrator(engine).EnsureAsync(connection, apply: schemaMode == SchemaMode.Apply, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task<StoredGrainState?> ReadAsync(GrainStateKey key, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainStateSql.Select);
            command.AddParameter("key_hash", key.Hash, DbType.Binary);
            await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return null;
            }

            return new StoredGrainState(
                reader.GetFieldValue<byte[]>(0),
                reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1),
                reader.GetFieldValue<byte[]>(2));
        }, cancellationToken);

    public Task<bool> TryInsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainStateSql.Insert(engine));
            GrainStateSql.AddInsertParameters(command, key, payload, etag, modifiedOn);
            try
            {
                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
            }
            catch (DbException exception) when (DbErrors.IsDuplicateKey(engine, exception))
            {
                return false;
            }
        }, cancellationToken);

    public Task<bool> TryUpdateAsync(GrainStateKey key, byte[] expectedETag, byte[]? payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainStateSql.Update);
            command.AddParameter("payload", payload, DbType.Binary);
            command.AddParameter("etag", etag, DbType.Binary);
            command.AddParameter("modified_on", modifiedOn, DbType.DateTimeOffset);
            AddMatch(command, key, expectedETag);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task<bool> TryDeleteAsync(GrainStateKey key, byte[] expectedETag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(engine, timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainStateSql.Delete);
            AddMatch(command, key, expectedETag);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }, cancellationToken);

    private static void AddMatch(DbCommand command, GrainStateKey key, byte[] expectedETag)
    {
        command.AddParameter("key_hash", key.Hash, DbType.Binary);
        command.AddParameter("grain_identity", key.Identity, DbType.Binary);
        command.AddParameter("expected_etag", expectedETag, DbType.Binary);
    }
}
