using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Reminders.AdoNet;

/// <summary>
/// Reminder operations as plain SQL (<see cref="ReminderSql"/>).
/// </summary>
/// <remarks>
/// Every statement autocommits on its own, so a transient server error means nothing was applied and the statement
/// is retried as a whole.
/// </remarks>
internal sealed class AdoNetReminderStore(DbDataSource dataSource, string? engineName, SchemaMode schemaMode, TimeProvider timeProvider, ILogger logger) : IReminderStore
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

    public async Task<StoredReminder?> ReadAsync(ReminderKey key, CancellationToken cancellationToken)
    {
        var rows = await ReadRowsAsync(ReminderSql.Select, command => command.AddParameter("key_hash", key.Hash, DbType.Binary), cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public Task<IReadOnlyList<StoredReminder>> ReadGrainAsync(byte[] grainKeyHash, CancellationToken cancellationToken) =>
        ReadRowsAsync(ReminderSql.SelectGrain, command => command.AddParameter("grain_key_hash", grainKeyHash, DbType.Binary), cancellationToken);

    public Task<IReadOnlyList<StoredReminder>> ReadRangeAsync(byte[] serviceHash, uint begin, uint end, CancellationToken cancellationToken) =>
        ReadRowsAsync(
            begin < end ? ReminderSql.SelectRange : ReminderSql.SelectWrappedRange,
            command =>
            {
                command.AddParameter("service_hash", serviceHash, DbType.Binary);
                command.AddParameter("begin", (long)begin, DbType.Int64);
                command.AddParameter("end", (long)end, DbType.Int64);
            },
            cancellationToken);

    public Task<bool> TryInsertAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(ReminderSql.Insert(engine));
            ReminderSql.AddInsertParameters(command, key, startAt, period, etag);
            try
            {
                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
            }
            catch (DbException exception) when (DbErrors.IsDuplicateKey(exception))
            {
                return false;
            }
        }, cancellationToken);

    public Task<bool> TryUpdateAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(ReminderSql.Update);
            command.AddParameter("start_at_ticks", startAt, DbType.Int64);
            command.AddParameter("period_ticks", period, DbType.Int64);
            command.AddParameter("etag", etag, DbType.Binary);
            command.AddParameter("key_hash", key.Hash, DbType.Binary);
            command.AddParameter("reminder_identity", key.Identity, DbType.Binary);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task<bool> TryDeleteAsync(ReminderKey key, byte[] expectedETag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(ReminderSql.Delete);
            command.AddParameter("key_hash", key.Hash, DbType.Binary);
            command.AddParameter("reminder_identity", key.Identity, DbType.Binary);
            command.AddParameter("expected_etag", expectedETag, DbType.Binary);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task ClearAsync(byte[] serviceHash, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(ReminderSql.Clear);
            command.AddParameter("service_hash", serviceHash, DbType.Binary);
            return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);

    private Task<IReadOnlyList<StoredReminder>> ReadRowsAsync(string sql, Action<DbCommand> bind, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync<IReadOnlyList<StoredReminder>>(timeProvider, async ct =>
        {
            await using var command = dataSource.CreateCommand(sql);
            bind(command);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            var rows = new List<StoredReminder>();
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add(new StoredReminder(reader.GetFieldValue<byte[]>(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetFieldValue<byte[]>(3)));
            }

            return rows;
        }, cancellationToken);

    private static SchemaMigrator CreateMigrator(RelationalEngine engine) =>
        new(RemindersSchema.Component, engine, SchemaScript.Load(typeof(AdoNetReminderStore).Assembly, engine), RemindersSchema.MinimumVersion);
}
