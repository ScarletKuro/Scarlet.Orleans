using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.GrainDirectory.AdoNet;

/// <summary>
/// Grain directory operations as plain SQL (<see cref="GrainDirectorySql"/>).
/// </summary>
/// <remarks>
/// Every statement autocommits on its own, so a transient server error, such as a deadlock, means nothing was applied
/// and the statement is retried as a whole.
/// </remarks>
internal sealed class AdoNetGrainDirectoryStore(DbDataSource dataSource, string? engineName, SchemaMode schemaMode, TimeProvider timeProvider, ILogger logger) : IGrainDirectoryStore
{
    private RelationalEngine engine;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        engine = engineName is null ? RelationalEngines.FromConnection(connection) : RelationalEngines.Parse(engineName);
        if (engine == RelationalEngine.Sqlite && schemaMode != SchemaMode.None)
        {
            await SqliteJournal.EnsureWriteAheadLogAsync(connection, enable: schemaMode == SchemaMode.Apply, logger, cancellationToken).ConfigureAwait(false);
        }

        if (schemaMode != SchemaMode.None)
        {
            await CreateMigrator(engine).EnsureAsync(connection, apply: schemaMode == SchemaMode.Apply, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task<StoredDirectoryEntry?> ReadAsync(DirectoryKey key, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainDirectorySql.Select);
            command.AddParameter("key_hash", key.Hash, DbType.Binary);
            await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleRow, ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return null;
            }

            return new StoredDirectoryEntry(
                reader.GetFieldValue<byte[]>(0),
                new DirectoryEntry(reader.GetString(1), reader.GetFieldValue<byte[]>(2), reader.GetFieldValue<byte[]>(3), reader.GetInt64(4)));
        }, cancellationToken);

    public Task<bool> TryInsertAsync(DirectoryKey key, DirectoryEntry entry, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainDirectorySql.Insert(engine));
            GrainDirectorySql.AddInsertParameters(command, key, entry);
            try
            {
                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
            }
            catch (DbException exception) when (DbErrors.IsDuplicateKey(exception))
            {
                return false;
            }
        }, cancellationToken);

    public Task<bool> TryReplaceAsync(DirectoryKey key, byte[] expectedActivationId, DirectoryEntry entry, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainDirectorySql.Replace);
            GrainDirectorySql.AddEntryParameters(command, entry);
            AddMatch(command, key, expectedActivationId);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task<bool> TryDeleteAsync(DirectoryKey key, byte[] expectedActivationId, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainDirectorySql.Delete);
            AddMatch(command, key, expectedActivationId);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task<int> DeleteSiloBatchAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, int limit, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainDirectorySql.DeleteSiloBatch(engine, limit));
            command.AddParameter("scope_hash", scopeHash, DbType.Binary);
            command.AddParameter("silo_hash", siloHash, DbType.Binary);
            command.AddParameter("silo_address", siloAddress, DbType.String);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);

    public Task<bool> HasSiloEntriesAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(GrainDirectorySql.SiloHasEntries);
            command.AddParameter("scope_hash", scopeHash, DbType.Binary);
            command.AddParameter("silo_hash", siloHash, DbType.Binary);
            command.AddParameter("silo_address", siloAddress, DbType.String);
            return Convert.ToInt32(await command.ExecuteScalarAsync(ct).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture) == 1;
        }, cancellationToken);

    private static void AddMatch(DbCommand command, DirectoryKey key, byte[] expectedActivationId)
    {
        command.AddParameter("key_hash", key.Hash, DbType.Binary);
        command.AddParameter("grain_identity", key.Identity, DbType.Binary);
        command.AddParameter("expected_activation_id", expectedActivationId, DbType.Binary);
    }

    private static SchemaMigrator CreateMigrator(RelationalEngine engine) =>
        new(GrainDirectorySchema.Component, engine, SchemaScript.Load(typeof(AdoNetGrainDirectoryStore).Assembly, engine), GrainDirectorySchema.MinimumVersion);
}
