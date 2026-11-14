using System.Data;
using System.Data.Common;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Transactions.AdoNet;

/// <summary>
/// Transactional state operations as plain SQL (<see cref="TransactionsSql"/>), each in its own transaction.
/// </summary>
/// <param name="dataSource">Opens the connection of each operation.</param>
/// <param name="engineName">The configured engine, or <see langword="null"/> to infer it from the driver.</param>
/// <param name="schemaMode">What to do with the schema in <see cref="InitializeAsync"/>.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
internal sealed class AdoNetTransactionalStateStore(DbDataSource dataSource, string? engineName, SchemaMode schemaMode, TimeProvider timeProvider, ILogger logger) : ITransactionalStateStore
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
            var migrator = new SchemaMigrator(
                TransactionsSchema.Component,
                engine,
                SchemaScript.Load(typeof(AdoNetTransactionalStateStore).Assembly, engine),
                TransactionsSchema.MinimumVersion);
            await migrator.EnsureAsync(connection, apply: schemaMode == SchemaMode.Apply, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A load is one statement on every engine but SQL Server, so it reads from one snapshot and takes no write lock:
    /// on SQLite a transaction of two reads would begin immediately, queueing every load behind the single writer.
    /// </summary>
    public Task<StoredTransactionalState?> LoadAsync(TransactionalStateKey key, CancellationToken cancellationToken) =>
        engine == RelationalEngine.SqlServer ? LoadSqlServerAsync(key, cancellationToken) : LoadInOneStatementAsync(key, cancellationToken);

    public async Task<StoreResult> StoreAsync(TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken)
    {
        var result = engine switch
        {
            RelationalEngine.SqlServer when request.Prepared.Count <= SqlServerTransactionsBatch.MaxPrepared => await StoreSqlServerAsync(key, request, cancellationToken).ConfigureAwait(false),
            RelationalEngine.PostgreSql => await StorePostgreSqlAsync(key, request, cancellationToken).ConfigureAwait(false),
            RelationalEngine.MySql or RelationalEngine.MariaDb when request.Prepared.Count <= MySqlTransactionsBatch.MaxPrepared => await StoreMySqlAsync(key, request, cancellationToken).ConfigureAwait(false),
            _ => await InTransactionAsync(IsolationLevel.ReadCommitted, (connection, transaction, ct) => StoreCoreAsync(connection, transaction, key, request, ct), cancellationToken).ConfigureAwait(false),
        };
        if (result.Status != StoreStatus.Conflict)
        {
            return result;
        }

        // Rolled back. The identity of a row under the key tells a conflict from a collision.
        return result with { StoredIdentity = await ReadIdentityAsync(key, cancellationToken).ConfigureAwait(false) };
    }

    /// <summary>
    /// The steps of a store, in the order of design section 5.5: the state row, which every writer takes first; aborted
    /// entries; prepared entries; the commit, checked against its entry; and the entries the commit made obsolete.
    /// </summary>
    private async Task<(StoreResult Result, bool Commit)> StoreCoreAsync(DbConnection connection, DbTransaction transaction, TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken)
    {
        // The committed sequence number before this store settles every step, as in Orleans' grain storage wrapper. It is
        // read before the row is locked: if another store commits in between, the update misses the ETag and nothing is
        // applied.
        long previous = 0;
        if (request.ExpectedETag is not null)
        {
            var value = await ScalarAsync(connection, transaction, TransactionsSql.SelectCommitted, key, sequenceId: null, cancellationToken).ConfigureAwait(false);
            if (value is null or DBNull)
            {
                return (new StoreResult(StoreStatus.Conflict), false);
            }

            previous = Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        var plan = StorePlan.Create(request, previous);
        if (!await WriteStateAsync(connection, transaction, key, request, plan.Committed, cancellationToken).ConfigureAwait(false))
        {
            return (new StoreResult(StoreStatus.Conflict), false);
        }

        var deleted = 0;
        if (plan.AbortAbove is { } abortAbove)
        {
            deleted += await DeleteAsync(connection, transaction, TransactionsSql.DeleteAbove, key, abortAbove, cancellationToken).ConfigureAwait(false);
        }

        if (plan.Prepared.Count > 0)
        {
            await DeleteAsync(connection, transaction, TransactionsSql.DeleteSequences(plan.Prepared.Select(e => e.SequenceId)), key, sequenceId: null, cancellationToken).ConfigureAwait(false);
            foreach (var chunk in plan.Prepared.Chunk(TransactionsSql.EntriesPerInsert))
            {
                await using var insert = Command(connection, transaction, TransactionsSql.InsertEntries(chunk.Length));
                TransactionsSql.AddKey(insert, key);
                for (var i = 0; i < chunk.Length; i++)
                {
                    TransactionsSql.AddEntry(insert, i, chunk[i]);
                }

                await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        if (plan.Advances)
        {
            if (Convert.ToInt64(await ScalarAsync(connection, transaction, TransactionsSql.CountEntry, key, plan.Committed, cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture) == 0)
            {
                return (new StoreResult(StoreStatus.MissingCommitRecord), false);
            }

            deleted += await DeleteAsync(connection, transaction, TransactionsSql.DeleteBelow, key, plan.Committed, cancellationToken).ConfigureAwait(false);
        }

        return (new StoreResult(StoreStatus.Stored, DeletedEntries: deleted), true);
    }

    /// <summary>
    /// Replaces or inserts the state row, with <paramref name="committed"/> as its committed sequence number. Returns
    /// <see langword="false"/> if no row with the expected identity and ETag exists, or a row exists where none was expected.
    /// </summary>
    private async Task<bool> WriteStateAsync(DbConnection connection, DbTransaction transaction, TransactionalStateKey key, StoreRequest request, long committed, CancellationToken cancellationToken)
    {
        var insert = request.ExpectedETag is null;
        await using var command = Command(connection, transaction, insert ? TransactionsSql.InsertState(engine) : TransactionsSql.UpdateState);
        TransactionsSql.AddKey(command, key);
        command.AddParameter("state_identity", key.Identity, DbType.Binary);
        command.AddParameter("etag", request.ETag, DbType.Binary);
        command.AddParameter("metadata", request.Metadata, DbType.Binary);
        command.AddParameter("committed", committed, DbType.Int64);
        command.AddParameter("modified_on", request.ModifiedOn, DbType.DateTimeOffset);
        if (insert)
        {
            command.AddParameter("service_id", key.ServiceIdText, DbType.String);
            command.AddParameter("state_name", key.StateNameText, DbType.String);
            command.AddParameter("grain_id", key.GrainIdText, DbType.String);
        }
        else
        {
            command.AddParameter("expected_etag", request.ExpectedETag, DbType.Binary);
        }

        try
        {
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
        }
        catch (DbException exception) when (insert && TransactionsSql.InsertStateReportsDuplicateAsError(engine) && DbErrors.IsDuplicateKey(exception))
        {
            return false;
        }
    }

    private async Task<byte[]?> ReadIdentityAsync(TransactionalStateKey key, CancellationToken cancellationToken) =>
        await TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var select = dataSource.CreateCommand(TransactionsSql.SelectIdentity);
            TransactionsSql.AddKey(select, key);
            return await select.ExecuteScalarAsync(ct).ConfigureAwait(false) as byte[];
        }, cancellationToken).ConfigureAwait(false);

    private static async Task<int> DeleteAsync(DbConnection connection, DbTransaction transaction, string sql, TransactionalStateKey key, long? sequenceId, CancellationToken cancellationToken)
    {
        await using var delete = Command(connection, transaction, sql);
        TransactionsSql.AddKey(delete, key);
        if (sequenceId is { } id)
        {
            delete.AddParameter("sequence_id", id, DbType.Int64);
        }

        return await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<object?> ScalarAsync(DbConnection connection, DbTransaction transaction, string sql, TransactionalStateKey key, long? sequenceId, CancellationToken cancellationToken)
    {
        await using var select = Command(connection, transaction, sql);
        TransactionsSql.AddKey(select, key);
        if (sequenceId is { } id)
        {
            select.AddParameter("sequence_id", id, DbType.Int64);
        }

        return await select.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs <paramref name="body"/> in a transaction on its own connection, and commits it if the body says so, or rolls
    /// it back. A transient error rolls the transaction back, as the transaction is disposed, before the whole operation
    /// is repeated (design section 4.7).
    /// </summary>
    private Task<T> InTransactionAsync<T>(
        IsolationLevel isolationLevel,
        Func<DbConnection, DbTransaction, CancellationToken, Task<(T Result, bool Commit)>> body,
        CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var connection = await dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(isolationLevel, ct).ConfigureAwait(false);
            var (result, commit) = await body(connection, transaction, ct).ConfigureAwait(false);
            if (commit)
            {
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            else
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
            }

            return result;
        }, cancellationToken);

    private static DbCommand Command(DbConnection connection, DbTransaction transaction, string text)
    {
        var command = connection.CreateCommand();
        command.CommandText = text;
        command.Transaction = transaction;
        return command;
    }

    /// <summary>
    /// A load in one statement, which reads from one snapshot (<see cref="TransactionsSql.SelectStateWithEntries"/>).
    /// </summary>
    private Task<StoredTransactionalState?> LoadInOneStatementAsync(TransactionalStateKey key, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var select = dataSource.CreateCommand(TransactionsSql.SelectStateWithEntries);
            TransactionsSql.AddKey(select, key);
            await using var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return null;
            }

            var identity = reader.GetFieldValue<byte[]>(0);
            var etag = reader.GetFieldValue<byte[]>(1);
            var committed = reader.GetInt64(2);
            var metadata = reader.GetFieldValue<byte[]>(3);
            var entries = new List<StoredEntry>();
            do
            {
                if (!reader.IsDBNull(4))
                {
                    entries.Add(new StoredEntry(reader.GetInt64(4), reader.GetString(5), reader.GetInt64(6), reader.GetFieldValue<byte[]>(7), reader.GetFieldValue<byte[]>(8)));
                }
            }
            while (await reader.ReadAsync(ct).ConfigureAwait(false));

            return (StoredTransactionalState?)new StoredTransactionalState(identity, etag, committed, metadata, entries);
        }, cancellationToken);

    /// <summary>
    /// A store in one batch, which PostgreSQL runs as one implicit transaction (<see cref="PostgreSqlTransactionsBatch"/>).
    /// </summary>
    private Task<StoreResult> StorePostgreSqlAsync(TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var batch = PostgreSqlTransactionsBatch.CreateStore(dataSource, key, request);
            try
            {
                await batch.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (DbException exception) when (exception.SqlState == PostgreSqlTransactionsBatch.DivisionByZero)
            {
                return new StoreResult(StoreStatus.MissingCommitRecord);
            }

            return PostgreSqlTransactionsBatch.Result(batch, request);
        }, cancellationToken);

    /// <summary>
    /// A load in one command, whose T-SQL transaction holds the state row's shared lock until it has read the entries
    /// (<see cref="SqlServerTransactionsBatch"/>).
    /// </summary>
    private Task<StoredTransactionalState?> LoadSqlServerAsync(TransactionalStateKey key, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var select = dataSource.CreateCommand(SqlServerTransactionsBatch.Load);
            TransactionsSql.AddKey(select, key);
            await using var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return null;
            }

            var identity = reader.GetFieldValue<byte[]>(0);
            var etag = reader.GetFieldValue<byte[]>(1);
            var committed = reader.GetInt64(2);
            var metadata = reader.GetFieldValue<byte[]>(3);
            var entries = new List<StoredEntry>();
            await reader.NextResultAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                entries.Add(TransactionsSql.ReadEntry(reader));
            }

            return (StoredTransactionalState?)new StoredTransactionalState(identity, etag, committed, metadata, entries);
        }, cancellationToken);

    /// <summary>
    /// A store in one command, whose T-SQL runs one transaction and returns its status (<see cref="SqlServerTransactionsBatch"/>).
    /// </summary>
    private Task<StoreResult> StoreSqlServerAsync(TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var command = SqlServerTransactionsBatch.CreateStore(dataSource, key, request);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            await reader.ReadAsync(ct).ConfigureAwait(false);
            return reader.GetInt32(0) switch
            {
                SqlServerTransactionsBatch.Stored => new StoreResult(StoreStatus.Stored, DeletedEntries: reader.GetInt32(1)),
                SqlServerTransactionsBatch.MissingCommitRecord => new StoreResult(StoreStatus.MissingCommitRecord),
                _ => new StoreResult(StoreStatus.Conflict),
            };
        }, cancellationToken);

    /// <summary>
    /// A store in two commands, which lock the state row, then write and commit (<see cref="MySqlTransactionsBatch"/>).
    /// </summary>
    private Task<StoreResult> StoreMySqlAsync(TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var connection = await dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            var committed = false;
            try
            {
                object[]? locked;
                try
                {
                    await using var command = MySqlTransactionsBatch.CreateLock(connection, key, request);
                    await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
                    locked = await MySqlTransactionsBatch.ReadRowAsync(reader, ct).ConfigureAwait(false);
                }
                catch (DbException exception) when (request.ExpectedETag is null && DbErrors.IsDuplicateKey(exception))
                {
                    return new StoreResult(StoreStatus.Conflict);
                }

                if (locked is null)
                {
                    return new StoreResult(StoreStatus.Conflict);
                }

                var plan = StorePlan.Create(request, Convert.ToInt64(locked[0], CultureInfo.InvariantCulture));
                if (plan.Advances && !MySqlTransactionsBatch.CommitEntryRemains(plan, Convert.ToInt64(locked[1], CultureInfo.InvariantCulture) > 0))
                {
                    return new StoreResult(StoreStatus.MissingCommitRecord);
                }

                var deleted = 0;
                await using (var command = MySqlTransactionsBatch.CreateWrite(connection, key, request, plan))
                await using (var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false))
                {
                    for (var counts = (plan.AbortAbove is null ? 0 : 1) + (plan.Advances ? 1 : 0); counts > 0; counts--)
                    {
                        deleted += Convert.ToInt32((await MySqlTransactionsBatch.ReadRowAsync(reader, ct).ConfigureAwait(false))![0], CultureInfo.InvariantCulture);
                    }

                    while (await reader.NextResultAsync(ct).ConfigureAwait(false))
                    {
                    }
                }

                committed = true;
                return new StoreResult(StoreStatus.Stored, DeletedEntries: deleted);
            }
            finally
            {
                if (!committed)
                {
                    await SqlTransactions.RollbackAsync(connection).ConfigureAwait(false);
                }
            }
        }, cancellationToken);
}
