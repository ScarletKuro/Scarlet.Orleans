using System.Data;
using System.Data.Common;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Reminders.AdoNet;

/// <summary>
/// The reminder SQL. Statements are standard and identical on every engine, except the insert.
/// </summary>
internal static class ReminderSql
{
    private const string Table = RemindersSchema.TableName;

    private const string Columns = "reminder_identity, start_at_ticks, period_ticks, etag";

    public const string Select = $"SELECT {Columns} FROM {Table} WHERE key_hash = @key_hash";

    public const string SelectGrain = $"SELECT {Columns} FROM {Table} WHERE grain_key_hash = @grain_key_hash";

    public const string SelectRange = $"SELECT {Columns} FROM {Table} WHERE service_hash = @service_hash AND grain_hash > @begin AND grain_hash <= @end";

    public const string SelectWrappedRange = $"SELECT {Columns} FROM {Table} WHERE service_hash = @service_hash AND (grain_hash > @begin OR grain_hash <= @end)";

    public const string Update = $"UPDATE {Table} SET start_at_ticks = @start_at_ticks, period_ticks = @period_ticks, etag = @etag WHERE key_hash = @key_hash AND reminder_identity = @reminder_identity";

    public const string Delete = $"DELETE FROM {Table} WHERE key_hash = @key_hash AND reminder_identity = @reminder_identity AND etag = @expected_etag";

    public const string Clear = $"DELETE FROM {Table} WHERE service_hash = @service_hash";

    private const string InsertStatement =
        $"INSERT INTO {Table} (key_hash, reminder_identity, service_hash, grain_key_hash, grain_hash, service_id, grain_id, reminder_name, start_at_ticks, period_ticks, etag) " +
        "VALUES (@key_hash, @reminder_identity, @service_hash, @grain_key_hash, @grain_hash, @service_id, @grain_id, @reminder_name, @start_at_ticks, @period_ticks, @etag)";

    /// <summary>
    /// Inserts a row, reporting an existing key either as zero affected rows or as a duplicate-key error
    /// (<see cref="InsertReportsDuplicateAsError"/>).
    /// </summary>
    /// <remarks>
    /// Both PostgreSQL and SQLite skip a conflicting row without an error. That avoids classifying the error at all,
    /// which matters for SQLite: Microsoft.Data.Sqlite exposes its result code only on its own exception type.
    /// </remarks>
    public static string Insert(RelationalEngine engine) =>
        InsertReportsDuplicateAsError(engine) ? InsertStatement : InsertStatement + " ON CONFLICT (key_hash) DO NOTHING";

    public static bool InsertReportsDuplicateAsError(RelationalEngine engine) => engine is not (RelationalEngine.PostgreSql or RelationalEngine.Sqlite);

    public static void AddInsertParameters(DbCommand command, ReminderKey key, long startAt, long period, byte[] etag)
    {
        command.AddParameter("key_hash", key.Hash, DbType.Binary);
        command.AddParameter("reminder_identity", key.Identity, DbType.Binary);
        command.AddParameter("service_hash", key.ServiceHash, DbType.Binary);
        command.AddParameter("grain_key_hash", key.GrainKeyHash, DbType.Binary);
        command.AddParameter("grain_hash", (long)key.GrainHash, DbType.Int64);
        command.AddParameter("service_id", key.ServiceIdText, DbType.String);
        command.AddParameter("grain_id", key.GrainIdText, DbType.String);
        command.AddParameter("reminder_name", key.ReminderNameText, DbType.String);
        command.AddParameter("start_at_ticks", startAt, DbType.Int64);
        command.AddParameter("period_ticks", period, DbType.Int64);
        command.AddParameter("etag", etag, DbType.Binary);
    }
}
