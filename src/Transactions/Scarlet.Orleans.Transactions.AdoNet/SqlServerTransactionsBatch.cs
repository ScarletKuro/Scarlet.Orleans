using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions.AdoNet;

/// <summary>
/// SQL Server loads and stores in one round trip each: each is one command whose T-SQL runs its own transaction.
/// </summary>
/// <remarks>
/// <para>
/// A store locks the state row with <c>XLOCK</c> first, which blocks a load's shared lock as well as other stores, so the
/// lock order is the same as every other store's, state row then entries, and a load cannot hold the row while a store
/// waits for it. It then follows <see cref="StorePlan"/> from the committed number it read, and returns a status row:
/// <see cref="Conflict"/>, <see cref="Stored"/> or <see cref="MissingCommitRecord"/>, rolling back on anything but stored.
/// <c>XACT_ABORT</c> rolls back on any error.
/// </para>
/// <para>
/// A command takes at most 2,100 parameters, five per prepared entry, so a store with more than
/// <see cref="MaxPrepared"/> prepares runs the general transaction of separate statements instead.
/// </para>
/// </remarks>
internal static class SqlServerTransactionsBatch
{
    public const int Conflict = 0;

    public const int Stored = 1;

    public const int MissingCommitRecord = 2;

    /// <summary>The most prepares one store command carries, five parameters each, under SQL Server's 2,100.</summary>
    public const int MaxPrepared = 400;

    private const string States = TransactionsSchema.StateTableName;

    private const string Entries = TransactionsSchema.EntryTableName;

    /// <summary>
    /// The state row, then its entries from the committed one on, as two result sets of one transaction, holding the
    /// state row's shared lock to the end, so no store commits between them.
    /// </summary>
    public const string Load =
        "SET XACT_ABORT ON; BEGIN TRANSACTION; " +
        $"SELECT state_identity, etag, committed_sequence_id, metadata FROM {States} WITH (REPEATABLEREAD) WHERE key_hash = @key_hash; " +
        $"SELECT e.sequence_id, e.transaction_id, e.timestamp_ticks, e.transaction_manager, e.payload FROM {Entries} e " +
        $"JOIN {States} s ON s.key_hash = e.key_hash WHERE e.key_hash = @key_hash AND e.sequence_id >= s.committed_sequence_id ORDER BY e.sequence_id; " +
        "COMMIT;";

    public static DbCommand CreateStore(DbDataSource dataSource, TransactionalStateKey key, StoreRequest request)
    {
        var command = dataSource.CreateCommand();
        var sql = new StringBuilder("SET XACT_ABORT ON; BEGIN TRANSACTION; DECLARE @previous bigint = 0, @committed bigint, @deleted int = 0; ");
        command.AddParameter("key_hash", key.Hash, DbType.Binary);
        command.AddParameter("state_identity", key.Identity, DbType.Binary);
        command.AddParameter("new_etag", request.ETag, DbType.Binary);
        command.AddParameter("metadata", request.Metadata, DbType.Binary);
        command.AddParameter("modified_on", request.ModifiedOn, DbType.DateTimeOffset);
        if (request.ExpectedETag is { } expected)
        {
            command.AddParameter("expected_etag", expected, DbType.Binary);
            sql
                .Append(CultureInfo.InvariantCulture, $"SELECT @previous = committed_sequence_id FROM {States} WITH (XLOCK, ROWLOCK) WHERE key_hash = @key_hash AND state_identity = @state_identity AND etag = @expected_etag; ")
                .Append(CultureInfo.InvariantCulture, $"IF @@ROWCOUNT = 0 BEGIN ROLLBACK; SELECT {Conflict}, 0; RETURN; END; ");
        }
        else
        {
            command.AddParameter("service_id", key.ServiceIdText, DbType.String);
            command.AddParameter("state_name", key.StateNameText, DbType.String);
            command.AddParameter("grain_id", key.GrainIdText, DbType.String);
            sql
                .Append(CultureInfo.InvariantCulture, $"IF EXISTS (SELECT 1 FROM {States} WITH (UPDLOCK, HOLDLOCK) WHERE key_hash = @key_hash) BEGIN ROLLBACK; SELECT {Conflict}, 0; RETURN; END; ")
                .Append(CultureInfo.InvariantCulture, $"INSERT INTO {States} (key_hash, state_identity, service_id, state_name, grain_id, etag, committed_sequence_id, metadata, modified_on) ")
                .Append("VALUES (@key_hash, @state_identity, @service_id, @state_name, @grain_id, @new_etag, 0, @metadata, @modified_on); ");
        }

        if (request.AbortAfter is { } abortAfter)
        {
            command.AddParameter("abort_after", abortAfter, DbType.Int64);
            sql.Append($"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id > @abort_after AND sequence_id > @previous; SET @deleted += @@ROWCOUNT; ");
        }

        if (request.Prepared.Count > 0)
        {
            // Sequence numbers are parameters, not literals, so the text depends only on the number of prepares and SQL
            // Server reuses its cached plan instead of compiling every store.
            var sequences = new StringBuilder();
            var values = new StringBuilder();
            for (var i = 0; i < request.Prepared.Count; i++)
            {
                var entry = request.Prepared[i];
                var separator = i == 0 ? "" : ", ";
                sequences.Append(CultureInfo.InvariantCulture, $"{separator}@s{i}");
                values.Append(CultureInfo.InvariantCulture, $"{separator}(@s{i}, @t{i}, @c{i}, @m{i}, @p{i})");
                command.AddParameter($"s{i}", entry.SequenceId, DbType.Int64);
                command.AddParameter($"t{i}", entry.TransactionId, DbType.String);
                command.AddParameter($"c{i}", entry.TimestampTicks, DbType.Int64);
                command.AddParameter($"m{i}", entry.TransactionManager, DbType.Binary);
                command.AddParameter($"p{i}", entry.Payload, DbType.Binary);
            }

            sql
                .Append(CultureInfo.InvariantCulture, $"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id IN ({sequences}) AND sequence_id > @previous; ")
                .Append(CultureInfo.InvariantCulture, $"INSERT INTO {Entries} (key_hash, sequence_id, transaction_id, timestamp_ticks, transaction_manager, payload) ")
                .Append(CultureInfo.InvariantCulture, $"SELECT @key_hash, v.sequence_id, v.transaction_id, v.timestamp_ticks, v.transaction_manager, v.payload ")
                .Append(CultureInfo.InvariantCulture, $"FROM (VALUES {values}) AS v(sequence_id, transaction_id, timestamp_ticks, transaction_manager, payload) WHERE v.sequence_id > @previous; ");
        }

        sql.Append("SET @committed = @previous; ");
        if (request.CommitUpTo is { } commitUpTo)
        {
            command.AddParameter("commit_up_to", commitUpTo, DbType.Int64);
            sql
                .Append("IF @commit_up_to > @previous BEGIN ")
                .Append(CultureInfo.InvariantCulture, $"IF NOT EXISTS (SELECT 1 FROM {Entries} WHERE key_hash = @key_hash AND sequence_id = @commit_up_to) BEGIN ROLLBACK; SELECT {MissingCommitRecord}, 0; RETURN; END; ")
                .Append(CultureInfo.InvariantCulture, $"SET @committed = @commit_up_to; DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id < @committed; SET @deleted += @@ROWCOUNT; END; ");
        }

        sql
                .Append(CultureInfo.InvariantCulture, $"UPDATE {States} SET etag = @new_etag, metadata = @metadata, committed_sequence_id = @committed, modified_on = @modified_on WHERE key_hash = @key_hash; ")
                .Append(CultureInfo.InvariantCulture, $"COMMIT; SELECT {Stored}, @deleted;");
        command.CommandText = sql.ToString();
        return command;
    }
}
