using System.Data;
using System.Data.Common;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence;

/// <summary>
/// The grain state SQL. Statements are standard and identical on every engine, except the insert.
/// </summary>
internal static class GrainStateSql
{
    private const string Table = PersistenceSchema.TableName;

    public const string Select = $"SELECT grain_identity, payload, etag FROM {Table} WHERE key_hash = @key_hash";

    public const string Update = $"UPDATE {Table} SET payload = @payload, etag = @etag, modified_on = @modified_on WHERE key_hash = @key_hash AND grain_identity = @grain_identity AND etag = @expected_etag";

    public const string Delete = $"DELETE FROM {Table} WHERE key_hash = @key_hash AND grain_identity = @grain_identity AND etag = @expected_etag";

    private const string InsertStatement =
        $"INSERT INTO {Table} (key_hash, grain_identity, service_id, state_name, grain_id, payload, etag, modified_on) " +
        "VALUES (@key_hash, @grain_identity, @service_id, @state_name, @grain_id, @payload, @etag, @modified_on)";

    /// <summary>
    /// Inserts a row, reporting an existing key either as zero affected rows or as a duplicate-key error
    /// (<see cref="InsertReportsDuplicateAsError"/>).
    /// </summary>
    /// <remarks>
    /// Both PostgreSQL and SQLite skip a conflicting row without an error. That avoids classifying the error at all,
    /// which matters for SQLite: Microsoft.Data.Sqlite exposes its result code only on its own exception type.
    /// Shared by both flavours, because EF Core cannot express an insert that tolerates an existing key.
    /// </remarks>
    public static string Insert(RelationalEngine engine) =>
        InsertReportsDuplicateAsError(engine) ? InsertStatement : InsertStatement + " ON CONFLICT (key_hash) DO NOTHING";

    public static bool InsertReportsDuplicateAsError(RelationalEngine engine) => engine is not (RelationalEngine.PostgreSql or RelationalEngine.Sqlite);

    public static void AddInsertParameters(DbCommand command, GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn)
    {
        command.AddParameter("key_hash", key.Hash, DbType.Binary);
        command.AddParameter("grain_identity", key.Identity, DbType.Binary);
        command.AddParameter("service_id", Display(key.ServiceId), DbType.String);
        command.AddParameter("state_name", Display(key.StateName), DbType.String);
        command.AddParameter("grain_id", Display(key.GrainId.ToString()), DbType.String);
        command.AddParameter("payload", payload, DbType.Binary);
        command.AddParameter("etag", etag, DbType.Binary);
        command.AddParameter("modified_on", modifiedOn, DbType.DateTimeOffset);
    }

    /// <summary>
    /// Makes an identity part storable as display text. PostgreSQL rejects NUL in text values, and identities may hold
    /// any byte, so NUL is written as <c>\0</c>. The text is never compared; identity lives in <c>grain_identity</c>.
    /// </summary>
    private static string Display(string value) => value.Replace("\0", "\\0", StringComparison.Ordinal);
}
