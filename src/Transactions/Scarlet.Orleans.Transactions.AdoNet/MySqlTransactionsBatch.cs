using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions.AdoNet;

/// <summary>
/// MySQL and MariaDB store in two round trips, each one command of several statements: the first starts the transaction
/// and locks the state row, and the second writes and commits.
/// </summary>
/// <remarks>
/// <para>
/// MySQL runs no conditional logic outside stored programs, and MySqlConnector rejects session variables unless the
/// connection string allows them, so a command cannot carry the committed sequence number from one statement to the
/// next. The first command returns it, with whether the commit's entry exists, and the store decides
/// <see cref="StorePlan"/> from it as the general path does; the row stays locked until the second command commits.
/// </para>
/// <para>
/// The commands start and commit the transaction in SQL, so the driver does not track it: a store that does not commit
/// rolls back explicitly, since MySql.Data does not reset a pooled connection by default.
/// </para>
/// </remarks>
internal static class MySqlTransactionsBatch
{
    /// <summary>The most prepares one store sends, in one insert statement.</summary>
    public const int MaxPrepared = TransactionsSql.EntriesPerInsert;

    private const string States = TransactionsSchema.StateTableName;

    private const string Entries = TransactionsSchema.EntryTableName;

    /// <summary>
    /// Read committed, as the general path's stores, so that reads take no gap locks, which would block the neighbouring
    /// keys of other grains.
    /// </summary>
    private const string Begin = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION; ";

    /// <summary>Whether the commit's entry exists; a locking read leaves a subquery's rows unlocked.</summary>
    private const string CountCommitEntry = $"(SELECT COUNT(*) FROM {Entries} e WHERE e.key_hash = @key_hash AND e.sequence_id = @commit_up_to)";

    /// <summary>
    /// Starts the transaction and locks the state row with the expected identity and ETag, or inserts it, returning no
    /// row on a conflict, or the committed sequence number and the number of entries at the commit's sequence number.
    /// A duplicate-key error reports an existing row where none was expected.
    /// </summary>
    public static DbCommand CreateLock(DbConnection connection, TransactionalStateKey key, StoreRequest request)
    {
        var command = connection.CreateCommand();
        TransactionsSql.AddKey(command, key);
        command.AddParameter("state_identity", key.Identity, DbType.Binary);
        command.AddParameter("commit_up_to", request.CommitUpTo ?? 0, DbType.Int64);
        if (request.ExpectedETag is { } expected)
        {
            command.AddParameter("expected_etag", expected, DbType.Binary);
            command.CommandText = Begin +
                $"SELECT s.committed_sequence_id, {CountCommitEntry} FROM {States} s " +
                "WHERE s.key_hash = @key_hash AND s.state_identity = @state_identity AND s.etag = @expected_etag FOR UPDATE";
        }
        else
        {
            command.AddParameter("service_id", key.ServiceIdText, DbType.String);
            command.AddParameter("state_name", key.StateNameText, DbType.String);
            command.AddParameter("grain_id", key.GrainIdText, DbType.String);
            command.AddParameter("etag", request.ETag, DbType.Binary);
            command.AddParameter("metadata", request.Metadata, DbType.Binary);
            command.AddParameter("committed", 0L, DbType.Int64);
            command.AddParameter("modified_on", request.ModifiedOn, DbType.DateTimeOffset);
            command.CommandText = Begin + TransactionsSql.InsertState(RelationalEngine.MySql) + $"; SELECT 0, {CountCommitEntry}";
        }

        return command;
    }

    /// <summary>
    /// Whether the commit's entry exists after the store's abort and prepares, given whether it existed before.
    /// </summary>
    public static bool CommitEntryRemains(StorePlan plan, bool existed) =>
        plan.Prepared.Any(e => e.SequenceId == plan.Committed) || (existed && !(plan.AbortAbove < plan.Committed));

    /// <summary>
    /// Writes <paramref name="plan"/> on the row the lock command locked, and commits. Each delete that counts toward
    /// <see cref="StoreResult.DeletedEntries"/> is followed by its row count.
    /// </summary>
    public static DbCommand CreateWrite(DbConnection connection, TransactionalStateKey key, StoreRequest request, StorePlan plan)
    {
        var command = connection.CreateCommand();
        var sql = new StringBuilder();
        TransactionsSql.AddKey(command, key);
        if (plan.AbortAbove is { } abortAbove)
        {
            command.AddParameter("abort_above", abortAbove, DbType.Int64);
            sql.Append(CultureInfo.InvariantCulture, $"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id > @abort_above; SELECT ROW_COUNT(); ");
        }

        if (plan.Prepared.Count > 0)
        {
            var sequences = new StringBuilder();
            for (var i = 0; i < plan.Prepared.Count; i++)
            {
                sequences.Append(CultureInfo.InvariantCulture, $"{(i == 0 ? "" : ", ")}@s{i}");
                TransactionsSql.AddEntry(command, i, plan.Prepared[i]);
            }

            sql
                .Append(CultureInfo.InvariantCulture, $"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id IN ({sequences}); ")
                .Append(TransactionsSql.InsertEntries(plan.Prepared.Count))
                .Append("; ");
        }

        command.AddParameter("etag", request.ETag, DbType.Binary);
        command.AddParameter("metadata", request.Metadata, DbType.Binary);
        command.AddParameter("committed", plan.Committed, DbType.Int64);
        command.AddParameter("modified_on", request.ModifiedOn, DbType.DateTimeOffset);
        sql.Append(CultureInfo.InvariantCulture, $"UPDATE {States} SET etag = @etag, metadata = @metadata, committed_sequence_id = @committed, modified_on = @modified_on WHERE key_hash = @key_hash; ");
        if (plan.Advances)
        {
            sql.Append(CultureInfo.InvariantCulture, $"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id < @committed; SELECT ROW_COUNT(); ");
        }

        command.CommandText = sql.Append("COMMIT").ToString();
        return command;
    }

    /// <summary>
    /// The first row of the next result set with columns, or <see langword="null"/> if it has none; statements that
    /// return no result set are skipped.
    /// </summary>
    public static async Task<object[]?> ReadRowAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        while (reader.FieldCount == 0)
        {
            if (!await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException("The command returned fewer result sets than it selects.");
            }
        }

        object[]? row = null;
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            row = new object[reader.FieldCount];
            reader.GetValues(row);
        }

        await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
        return row;
    }
}
