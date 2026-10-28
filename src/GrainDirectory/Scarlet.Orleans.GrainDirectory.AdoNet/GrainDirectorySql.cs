using System.Data;
using System.Data.Common;
using System.Globalization;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.GrainDirectory.AdoNet;

/// <summary>
/// The grain directory SQL. Statements are standard and identical on every engine, except the insert and the batched
/// delete of a silo's entries.
/// </summary>
internal static class GrainDirectorySql
{
    private const string Table = GrainDirectorySchema.TableName;

    public const string Select = $"SELECT grain_identity, silo_address, silo_hash, activation_id, membership_version FROM {Table} WHERE key_hash = @key_hash";

    public const string Replace =
        $"UPDATE {Table} SET silo_address = @silo_address, silo_hash = @silo_hash, activation_id = @activation_id, membership_version = @membership_version " +
        "WHERE key_hash = @key_hash AND grain_identity = @grain_identity AND activation_id = @expected_activation_id";

    public const string Delete = $"DELETE FROM {Table} WHERE key_hash = @key_hash AND grain_identity = @grain_identity AND activation_id = @expected_activation_id";

    private const string InsertStatement =
        $"INSERT INTO {Table} (key_hash, grain_identity, scope_hash, silo_hash, cluster_id, directory_name, grain_id, silo_address, activation_id, membership_version) " +
        "VALUES (@key_hash, @grain_identity, @scope_hash, @silo_hash, @cluster_id, @directory_name, @grain_id, @silo_address, @activation_id, @membership_version)";

    private const string SiloMatch = "scope_hash = @scope_hash AND silo_hash = @silo_hash AND silo_address = @silo_address";

    /// <summary>
    /// Inserts a row, reporting an existing key either as zero affected rows or as a duplicate-key error
    /// (<see cref="InsertReportsDuplicateAsError"/>).
    /// </summary>
    /// <remarks>
    /// A plain insert, never an upsert: on MySQL, <c>INSERT … ON DUPLICATE KEY UPDATE</c> takes gap locks that deadlock
    /// concurrent registrations (design section 5.2). PostgreSQL and SQLite skip a conflicting row without an error.
    /// </remarks>
    public static string Insert(RelationalEngine engine) =>
        InsertReportsDuplicateAsError(engine) ? InsertStatement : InsertStatement + " ON CONFLICT (key_hash) DO NOTHING";

    public static bool InsertReportsDuplicateAsError(RelationalEngine engine) => engine is not (RelationalEngine.PostgreSql or RelationalEngine.Sqlite);

    /// <summary>
    /// Deletes up to <paramref name="limit"/> of a silo's rows in key order, so concurrent batches lock rows in the same order.
    /// </summary>
    /// <remarks>
    /// Each engine bounds a delete differently: MySQL and MariaDB take <c>ORDER BY … LIMIT</c> on the delete itself, SQL
    /// Server deletes through an ordered <c>TOP</c>, and PostgreSQL and SQLite delete the keys an ordered subquery selects.
    /// The limit is a constant of the provider, so it is written into the statement.
    /// </remarks>
    public static string DeleteSiloBatch(RelationalEngine engine, int limit)
    {
        var n = limit.ToString(CultureInfo.InvariantCulture);
        return engine switch
        {
            RelationalEngine.MySql or RelationalEngine.MariaDb => $"DELETE FROM {Table} WHERE {SiloMatch} ORDER BY key_hash LIMIT {n}",
            RelationalEngine.SqlServer => $"WITH batch AS (SELECT TOP ({n}) * FROM {Table} WHERE {SiloMatch} ORDER BY key_hash) DELETE FROM batch",
            _ => $"DELETE FROM {Table} WHERE key_hash IN (SELECT key_hash FROM {Table} WHERE {SiloMatch} ORDER BY key_hash LIMIT {n})",
        };
    }

    public static void AddEntryParameters(DbCommand command, DirectoryEntry entry)
    {
        command.AddParameter("silo_address", entry.SiloAddress, DbType.String);
        command.AddParameter("silo_hash", entry.SiloHash, DbType.Binary);
        command.AddParameter("activation_id", entry.ActivationId, DbType.Binary);
        command.AddParameter("membership_version", entry.MembershipVersion, DbType.Int64);
    }

    public static void AddInsertParameters(DbCommand command, DirectoryKey key, DirectoryEntry entry)
    {
        command.AddParameter("key_hash", key.Hash, DbType.Binary);
        command.AddParameter("grain_identity", key.Identity, DbType.Binary);
        command.AddParameter("scope_hash", key.ScopeHash, DbType.Binary);
        command.AddParameter("cluster_id", key.ClusterIdText, DbType.String);
        command.AddParameter("directory_name", key.DirectoryNameText, DbType.String);
        command.AddParameter("grain_id", key.GrainIdText, DbType.String);
        AddEntryParameters(command, entry);
    }
}
