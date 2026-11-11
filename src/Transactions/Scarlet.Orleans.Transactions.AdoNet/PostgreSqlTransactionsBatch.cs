using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions.AdoNet;

/// <summary>
/// PostgreSQL loads and stores in one round trip each: a load is one statement
/// (<see cref="TransactionsSql.SelectStateWithEntries"/>), which PostgreSQL reads from one snapshot, and a store is one batch, which Npgsql sends with one sync, so PostgreSQL runs it as one implicit
/// transaction that an error rolls back whole.
/// </summary>
/// <remarks>
/// <para>
/// Every statement of a store is guarded by the state row holding the expected ETag, the guard: after a conflict, each
/// matches nothing, and the final update reports zero rows. The guard is the new ETag when the store inserts the row.
/// The first statement locks the state row, so the lock order is the same as every other store's: state row, then
/// entries. The steps follow <see cref="StorePlan"/>, decided in SQL from the committed sequence number before the store.
/// </para>
/// <para>
/// A commit whose entry is missing must change nothing, so its check fails the batch: it divides by a column-dependent
/// zero, which PostgreSQL cannot fold away at planning, and the store reports the resulting
/// <see cref="DivisionByZero"/> as <see cref="StoreStatus.MissingCommitRecord"/>.
/// </para>
/// </remarks>
internal static class PostgreSqlTransactionsBatch
{
    /// <summary>The SQLSTATE of the commit check, <c>division_by_zero</c>.</summary>
    public const string DivisionByZero = "22012";

    private const string States = TransactionsSchema.StateTableName;

    private const string Entries = TransactionsSchema.EntryTableName;

    private const string Guard = "s.key_hash = @key_hash AND s.state_identity = @state_identity AND s.etag = @guard";

    public static DbBatch CreateStore(DbDataSource dataSource, TransactionalStateKey key, StoreRequest request)
    {
        var batch = dataSource.CreateBatch();
        var guard = request.ExpectedETag ?? request.ETag;
        if (request.ExpectedETag is null)
        {
            Add(batch, key, guard, $"INSERT INTO {States} (key_hash, state_identity, service_id, state_name, grain_id, etag, committed_sequence_id, metadata, modified_on) " +
                "VALUES (@key_hash, @state_identity, @service_id, @state_name, @grain_id, @guard, 0, @metadata, @modified_on) ON CONFLICT (key_hash) DO NOTHING",
                ("service_id", key.ServiceIdText, DbType.String),
                ("state_name", key.StateNameText, DbType.String),
                ("grain_id", key.GrainIdText, DbType.String),
                ("metadata", request.Metadata, DbType.Binary),
                ("modified_on", request.ModifiedOn, DbType.DateTimeOffset));
        }

        Add(batch, key, guard, $"SELECT s.committed_sequence_id FROM {States} s WHERE {Guard} FOR UPDATE");

        if (request.AbortAfter is { } abortAfter)
        {
            Add(batch, key, guard, $"DELETE FROM {Entries} e USING {States} s WHERE e.key_hash = @key_hash AND {Guard} AND e.sequence_id > GREATEST(@abort_after, s.committed_sequence_id)",
                ("abort_after", abortAfter, DbType.Int64));
        }

        if (request.Prepared.Count > 0)
        {
            var sequences = string.Join(", ", request.Prepared.Select(e => e.SequenceId.ToString(CultureInfo.InvariantCulture)));
            Add(batch, key, guard, $"DELETE FROM {Entries} e USING {States} s WHERE e.key_hash = @key_hash AND {Guard} AND e.sequence_id IN ({sequences}) AND e.sequence_id > s.committed_sequence_id");

            // A statement takes at most 65,535 parameters, so the entries are inserted in chunks, each its own command in the
            // same batch, which keeps the store one round trip and one transaction.
            foreach (var chunk in request.Prepared.Chunk(TransactionsSql.EntriesPerInsert))
            {
                var values = new StringBuilder();
                var parameters = new List<(string, object?, DbType)>();
                for (var i = 0; i < chunk.Length; i++)
                {
                    var entry = chunk[i];
                    values.Append(CultureInfo.InvariantCulture, $"{(i == 0 ? "" : ", ")}(@s{i}::bigint, @t{i}, @c{i}::bigint, @m{i}::bytea, @p{i}::bytea)");
                    parameters.Add(($"s{i}", entry.SequenceId, DbType.Int64));
                    parameters.Add(($"t{i}", entry.TransactionId, DbType.String));
                    parameters.Add(($"c{i}", entry.TimestampTicks, DbType.Int64));
                    parameters.Add(($"m{i}", entry.TransactionManager, DbType.Binary));
                    parameters.Add(($"p{i}", entry.Payload, DbType.Binary));
                }

                Add(batch, key, guard,
                    $"INSERT INTO {Entries} (key_hash, sequence_id, transaction_id, timestamp_ticks, transaction_manager, payload) " +
                    $"SELECT @key_hash, v.sequence_id, v.transaction_id, v.timestamp_ticks, v.transaction_manager, v.payload " +
                    $"FROM (VALUES {values}) AS v(sequence_id, transaction_id, timestamp_ticks, transaction_manager, payload) " +
                    $"JOIN {States} s ON {Guard} WHERE v.sequence_id > s.committed_sequence_id",
                    [.. parameters]);
            }
        }

        if (request.CommitUpTo is { } commitUpTo)
        {
            Add(batch, key, guard,
                $"SELECT CASE WHEN @commit_up_to > s.committed_sequence_id AND NOT EXISTS (SELECT 1 FROM {Entries} WHERE key_hash = @key_hash AND sequence_id = @commit_up_to) " +
                $"THEN 1 / (s.committed_sequence_id * 0) END FROM {States} s WHERE {Guard}",
                ("commit_up_to", commitUpTo, DbType.Int64));
        }

        Add(batch, key, guard,
            $"UPDATE {States} SET etag = @new_etag, metadata = @metadata, " +
            "committed_sequence_id = CASE WHEN @commit_up_to > committed_sequence_id THEN @commit_up_to ELSE committed_sequence_id END, modified_on = @modified_on " +
            "WHERE key_hash = @key_hash AND state_identity = @state_identity AND etag = @guard",
            ("new_etag", request.ETag, DbType.Binary),
            ("metadata", request.Metadata, DbType.Binary),
            ("commit_up_to", request.CommitUpTo ?? 0, DbType.Int64),
            ("modified_on", request.ModifiedOn, DbType.DateTimeOffset));

        if (request.CommitUpTo is not null)
        {
            Add(batch, key, guard,
                $"DELETE FROM {Entries} e USING {States} s WHERE e.key_hash = @key_hash AND s.key_hash = @key_hash AND s.etag = @new_etag AND e.sequence_id < s.committed_sequence_id",
                ("new_etag", request.ETag, DbType.Binary));
        }

        return batch;
    }

    /// <summary>
    /// The outcome of an executed store batch: stored if the final state update matched its row, with the entries the
    /// abort and the commit deleted.
    /// </summary>
    public static StoreResult Result(DbBatch batch, StoreRequest request)
    {
        var commands = batch.BatchCommands;
        var update = commands.Count - (request.CommitUpTo is null ? 1 : 2);
        if (commands[update].RecordsAffected != 1)
        {
            return new StoreResult(StoreStatus.Conflict);
        }

        var deleted = request.CommitUpTo is null ? 0 : commands[^1].RecordsAffected;
        if (request.AbortAfter is not null)
        {
            deleted += commands[request.ExpectedETag is null ? 2 : 1].RecordsAffected;
        }

        return new StoreResult(StoreStatus.Stored, DeletedEntries: deleted);
    }

    private static void Add(DbBatch batch, TransactionalStateKey key, byte[] guard, string sql, params (string Name, object? Value, DbType Type)[] parameters)
    {
        var command = batch.CreateBatchCommand();
        command.CommandText = sql;
        AddParameter(command, "key_hash", key.Hash, DbType.Binary);
        AddParameter(command, "state_identity", key.Identity, DbType.Binary);
        AddParameter(command, "guard", guard, DbType.Binary);
        foreach (var (name, value, type) in parameters)
        {
            AddParameter(command, name, value, type);
        }

        batch.BatchCommands.Add(command);
    }

    private static void AddParameter(DbBatchCommand command, string name, object? value, DbType type)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@" + name;
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
