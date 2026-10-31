using System.Data;
using System.Data.Common;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Clustering.AdoNet;

/// <summary>
/// The clustering SQL. Every statement is standard and identical on every engine, except the creation of a version row.
/// </summary>
internal static class ClusteringSql
{
    private const string Versions = ClusteringSchema.VersionTableName;

    private const string Members = ClusteringSchema.MemberTableName;

    private const string InsertVersionStatement =
        $"INSERT INTO {Versions} (cluster_hash, cluster_identity, cluster_id, version, etag) VALUES (@cluster_hash, @cluster_identity, @cluster_id, 0, @etag)";

    public const string SelectVersion = $"SELECT cluster_identity, version, etag FROM {Versions} WHERE cluster_hash = @cluster_hash";

    public const string SwapVersion =
        $"UPDATE {Versions} SET version = @new_version, etag = @new_etag " +
        "WHERE cluster_hash = @cluster_hash AND cluster_identity = @cluster_identity AND version = @expected_version AND etag = @expected_etag";

    /// <summary>
    /// Writes the version row without changing it, which takes its row lock on every engine, so operations that change
    /// members without a new version keep the lock order of the others.
    /// </summary>
    public const string LockVersion = $"UPDATE {Versions} SET version = version WHERE cluster_hash = @cluster_hash";

    public const string DeleteVersion = $"DELETE FROM {Versions} WHERE cluster_hash = @cluster_hash AND cluster_identity = @cluster_identity";

    private const string MemberColumns =
        "member_identity, silo_address, status, proxy_port, host_name, silo_name, role_name, update_zone, fault_zone, " +
        "suspect_times, latest_suspect_ticks, start_ticks, i_am_alive_ticks, etag, key_hash";

    public const string SelectMembers = $"SELECT {MemberColumns} FROM {Members} WHERE cluster_hash = @cluster_hash";

    public const string SelectMember = $"SELECT {MemberColumns} FROM {Members} WHERE cluster_hash = @cluster_hash AND key_hash = @key_hash";

    public const string SelectMemberIdentity = $"SELECT member_identity FROM {Members} WHERE cluster_hash = @cluster_hash AND key_hash = @key_hash";

    public const string InsertMember =
        $"INSERT INTO {Members} (cluster_hash, key_hash, {EntryColumns}, member_identity, silo_address, etag) " +
        $"VALUES (@cluster_hash, @key_hash, {EntryParameters}, @member_identity, @silo_address, @etag)";

    /// <summary>
    /// Replaces an entry. The heartbeat time only moves forward, as with <see cref="Heartbeat"/>, so an update made with
    /// an entry read before the silo's latest heartbeat does not make it look older than it is.
    /// </summary>
    public const string UpdateMember =
        $"UPDATE {Members} SET status = @status, proxy_port = @proxy_port, host_name = @host_name, silo_name = @silo_name, " +
        "role_name = @role_name, update_zone = @update_zone, fault_zone = @fault_zone, suspect_times = @suspect_times, " +
        "latest_suspect_ticks = @latest_suspect_ticks, start_ticks = @start_ticks, " +
        "i_am_alive_ticks = CASE WHEN i_am_alive_ticks < @i_am_alive_ticks THEN @i_am_alive_ticks ELSE i_am_alive_ticks END, etag = @etag " +
        "WHERE cluster_hash = @cluster_hash AND key_hash = @key_hash AND member_identity = @member_identity AND etag = @expected_etag";

    public const string Heartbeat =
        $"UPDATE {Members} SET i_am_alive_ticks = @i_am_alive_ticks " +
        "WHERE cluster_hash = @cluster_hash AND key_hash = @key_hash AND member_identity = @member_identity AND i_am_alive_ticks < @i_am_alive_ticks";

    /// <summary>
    /// Deletes dead members whose every recorded time, start, heartbeat and latest suspicion, is before the cutoff.
    /// </summary>
    public const string DeleteDefunct =
        $"DELETE FROM {Members} WHERE cluster_hash = @cluster_hash AND status = 6 " +
        "AND start_ticks < @before_ticks AND i_am_alive_ticks < @before_ticks AND (latest_suspect_ticks IS NULL OR latest_suspect_ticks < @before_ticks)";

    public const string DeleteMembers = $"DELETE FROM {Members} WHERE cluster_hash = @cluster_hash";

    /// <summary>Active members (status 3) that accept client connections.</summary>
    public const string SelectGateways =
        $"SELECT member_identity, silo_address, proxy_port FROM {Members} WHERE cluster_hash = @cluster_hash AND status = 3 AND proxy_port > 0";

    private const string EntryColumns =
        "status, proxy_port, host_name, silo_name, role_name, update_zone, fault_zone, suspect_times, latest_suspect_ticks, start_ticks, i_am_alive_ticks";

    private const string EntryParameters =
        "@status, @proxy_port, @host_name, @silo_name, @role_name, @update_zone, @fault_zone, @suspect_times, @latest_suspect_ticks, @start_ticks, @i_am_alive_ticks";

    /// <summary>
    /// Creates a version row, reporting an existing one either as zero affected rows or as a duplicate-key error
    /// (<see cref="InsertVersionReportsDuplicateAsError"/>). PostgreSQL and SQLite skip a conflicting row without an error.
    /// </summary>
    public static string InsertVersion(RelationalEngine engine) =>
        InsertVersionReportsDuplicateAsError(engine) ? InsertVersionStatement : InsertVersionStatement + " ON CONFLICT (cluster_hash) DO NOTHING";

    public static bool InsertVersionReportsDuplicateAsError(RelationalEngine engine) => engine is not (RelationalEngine.PostgreSql or RelationalEngine.Sqlite);

    public static void AddCluster(DbCommand command, ClusterScope scope) => command.AddParameter("cluster_hash", scope.Hash, DbType.Binary);

    public static void AddMemberKey(DbCommand command, ClusterScope scope, byte[] keyHash, byte[] identity)
    {
        AddCluster(command, scope);
        command.AddParameter("key_hash", keyHash, DbType.Binary);
        command.AddParameter("member_identity", identity, DbType.Binary);
    }

    public static void AddVersionChange(DbCommand command, ClusterScope scope, VersionChange change)
    {
        AddCluster(command, scope);
        command.AddParameter("cluster_identity", scope.Identity, DbType.Binary);
        command.AddParameter("expected_version", change.ExpectedVersion, DbType.Int32);
        command.AddParameter("expected_etag", change.ExpectedETag, DbType.Binary);
        command.AddParameter("new_version", change.NewVersion, DbType.Int32);
        command.AddParameter("new_etag", change.NewETag, DbType.Binary);
    }

    public static void AddEntry(DbCommand command, MemberRow row)
    {
        command.AddParameter("status", row.Status, DbType.Int32);
        command.AddParameter("proxy_port", row.ProxyPort, DbType.Int32);
        command.AddParameter("host_name", row.HostName, DbType.String);
        command.AddParameter("silo_name", row.SiloName, DbType.String);
        command.AddParameter("role_name", row.RoleName, DbType.String);
        command.AddParameter("update_zone", row.UpdateZone, DbType.Int32);
        command.AddParameter("fault_zone", row.FaultZone, DbType.Int32);
        command.AddParameter("suspect_times", row.SuspectTimes, DbType.String);
        command.AddParameter("latest_suspect_ticks", row.LatestSuspectTicks, DbType.Int64);
        command.AddParameter("start_ticks", row.StartTicks, DbType.Int64);
        command.AddParameter("i_am_alive_ticks", row.IAmAliveTicks, DbType.Int64);
    }

    /// <summary>
    /// Reads a row selected with the member columns, in their order.
    /// </summary>
    public static StoredMember ReadMember(DbDataReader reader) =>
        new(
            new MemberRow(
                KeyHash: reader.GetFieldValue<byte[]>(14),
                Identity: reader.GetFieldValue<byte[]>(0),
                SiloAddress: reader.GetString(1),
                Status: reader.GetInt32(2),
                ProxyPort: reader.GetInt32(3),
                HostName: reader.IsDBNull(4) ? null : reader.GetString(4),
                SiloName: reader.IsDBNull(5) ? null : reader.GetString(5),
                RoleName: reader.IsDBNull(6) ? null : reader.GetString(6),
                UpdateZone: reader.GetInt32(7),
                FaultZone: reader.GetInt32(8),
                SuspectTimes: reader.IsDBNull(9) ? null : reader.GetString(9),
                LatestSuspectTicks: reader.IsDBNull(10) ? null : reader.GetInt64(10),
                StartTicks: reader.GetInt64(11),
                IAmAliveTicks: reader.GetInt64(12)),
            reader.GetFieldValue<byte[]>(13));
}
