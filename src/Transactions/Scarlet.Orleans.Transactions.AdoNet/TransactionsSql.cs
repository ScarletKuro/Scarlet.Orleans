using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions.AdoNet;

/// <summary>
/// The transactions SQL. Every statement is standard and identical on every engine, except the insert of a state row.
/// </summary>
internal static class TransactionsSql
{
    private const string States = TransactionsSchema.StateTableName;

    private const string Entries = TransactionsSchema.EntryTableName;

    /// <summary>
    /// The most entries one insert statement writes. Each takes five parameters, well under SQL Server's 2,100.
    /// </summary>
    public const int EntriesPerInsert = 200;

    private const string InsertStateStatement =
        $"INSERT INTO {States} (key_hash, state_identity, service_id, state_name, grain_id, etag, committed_sequence_id, metadata, modified_on) " +
        "VALUES (@key_hash, @state_identity, @service_id, @state_name, @grain_id, @etag, @commit_up_to, @metadata, @modified_on)";

    /// <summary>
    /// Replaces the state row if its identity and ETag match. The committed sequence number only moves forward, so a
    /// commit at or below the current one changes nothing.
    /// </summary>
    public const string UpdateState =
        $"UPDATE {States} SET etag = @etag, metadata = @metadata, " +
        "committed_sequence_id = CASE WHEN @commit_up_to > committed_sequence_id THEN @commit_up_to ELSE committed_sequence_id END, modified_on = @modified_on " +
        "WHERE key_hash = @key_hash AND state_identity = @state_identity AND etag = @expected_etag";

    public const string SelectState = $"SELECT state_identity, etag, committed_sequence_id, metadata FROM {States} WHERE key_hash = @key_hash";

    public const string SelectIdentity = $"SELECT state_identity FROM {States} WHERE key_hash = @key_hash";

    public const string SelectCommitted = $"SELECT committed_sequence_id FROM {States} WHERE key_hash = @key_hash";

    public const string SelectEntries =
        $"SELECT sequence_id, transaction_id, timestamp_ticks, transaction_manager, payload FROM {Entries} " +
        "WHERE key_hash = @key_hash AND sequence_id >= @sequence_id ORDER BY sequence_id";

    public const string CountEntry = $"SELECT COUNT(*) FROM {Entries} WHERE key_hash = @key_hash AND sequence_id = @sequence_id";

    public const string DeleteAbove = $"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id > @sequence_id";

    public const string DeleteBelow = $"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id < @sequence_id";

    /// <summary>
    /// Inserts a state row, reporting an existing one either as zero affected rows or as a duplicate-key error
    /// (<see cref="InsertStateReportsDuplicateAsError"/>). PostgreSQL and SQLite skip a conflicting row without an error,
    /// which on PostgreSQL also keeps the transaction usable.
    /// </summary>
    public static string InsertState(RelationalEngine engine) =>
        InsertStateReportsDuplicateAsError(engine) ? InsertStateStatement : InsertStateStatement + " ON CONFLICT (key_hash) DO NOTHING";

    public static bool InsertStateReportsDuplicateAsError(RelationalEngine engine) => engine is not (RelationalEngine.PostgreSql or RelationalEngine.Sqlite);

    /// <summary>
    /// Deletes the entries with the given sequence numbers, written as integer literals: they are numbers, so nothing can
    /// be injected, and a literal list needs no parameter per value.
    /// </summary>
    public static string DeleteSequences(IEnumerable<long> sequenceIds) =>
        $"DELETE FROM {Entries} WHERE key_hash = @key_hash AND sequence_id IN ({string.Join(", ", sequenceIds.Select(id => id.ToString(CultureInfo.InvariantCulture)))})";

    /// <summary>
    /// Inserts <paramref name="count"/> entries, whose parameters <see cref="AddEntry"/> numbers from 0.
    /// </summary>
    public static string InsertEntries(int count)
    {
        var sql = new StringBuilder($"INSERT INTO {Entries} (key_hash, sequence_id, transaction_id, timestamp_ticks, transaction_manager, payload) VALUES ");
        for (var i = 0; i < count; i++)
        {
            sql.Append(CultureInfo.InvariantCulture, $"{(i == 0 ? "" : ", ")}(@key_hash, @s{i}, @t{i}, @c{i}, @m{i}, @p{i})");
        }

        return sql.ToString();
    }

    public static void AddKey(DbCommand command, TransactionalStateKey key) => command.AddParameter("key_hash", key.Hash, DbType.Binary);

    public static void AddEntry(DbCommand command, int index, StoredEntry entry)
    {
        command.AddParameter($"s{index}", entry.SequenceId, DbType.Int64);
        command.AddParameter($"t{index}", entry.TransactionId, DbType.String);
        command.AddParameter($"c{index}", entry.TimestampTicks, DbType.Int64);
        command.AddParameter($"m{index}", entry.TransactionManager, DbType.Binary);
        command.AddParameter($"p{index}", entry.Payload, DbType.Binary);
    }

    /// <summary>
    /// Reads a row selected by <see cref="SelectEntries"/>.
    /// </summary>
    public static StoredEntry ReadEntry(DbDataReader reader) =>
        new(
            reader.GetInt64(0),
            reader.GetString(1),
            reader.GetInt64(2),
            reader.GetFieldValue<byte[]>(3),
            reader.GetFieldValue<byte[]>(4));
}
