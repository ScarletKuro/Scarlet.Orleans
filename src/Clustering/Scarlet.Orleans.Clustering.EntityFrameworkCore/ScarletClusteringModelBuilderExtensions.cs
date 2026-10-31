using Microsoft.EntityFrameworkCore.Migrations;
using Scarlet.Orleans.Clustering;
using Scarlet.Orleans.Clustering.EntityFrameworkCore;
using Scarlet.Orleans.Relational.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Adds the Orleans clustering tables to an EF Core model and migrations.
/// </summary>
public static class ScarletClusteringModelBuilderExtensions
{
    /// <summary>
    /// Maps the <c>orleans_cluster_version</c> and <c>orleans_cluster_member</c> tables and the
    /// <c>orleans_schema_history</c> ledger. Call from <c>OnModelCreating</c>.
    /// </summary>
    /// <remarks>
    /// Only provider-neutral configuration is used, chosen so that every supported provider generates exactly the tables
    /// of the ADO.NET flavour's SQL scripts, and either flavour can use a database created by the other. A test compares
    /// the two on each engine. The ledger can be shared with other Orleans components in the same model.
    /// </remarks>
    public static ModelBuilder ApplyOrleansClusteringModel(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<ClusterVersionRecord>(entity =>
        {
            entity.ToTable(ClusteringSchema.VersionTableName);
            entity.HasKey(r => r.ClusterHash).HasName("pk_orleans_cluster_version");
            entity.Property(r => r.ClusterHash).HasColumnName("cluster_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.ClusterIdentity).HasColumnName("cluster_identity");
            entity.Property(r => r.ClusterId).HasColumnName("cluster_id");
            entity.Property(r => r.Version).HasColumnName("version");
            entity.Property(r => r.ETag).HasColumnName("etag").HasMaxLength(16).IsFixedLength();
        });

        modelBuilder.Entity<ClusterMemberRecord>(entity =>
        {
            entity.ToTable(ClusteringSchema.MemberTableName);
            entity.HasKey(r => new { r.ClusterHash, r.KeyHash }).HasName("pk_orleans_cluster_member");
            entity.Property(r => r.ClusterHash).HasColumnName("cluster_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.KeyHash).HasColumnName("key_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.MemberIdentity).HasColumnName("member_identity");
            entity.Property(r => r.SiloAddress).HasColumnName("silo_address");
            entity.Property(r => r.Status).HasColumnName("status");
            entity.Property(r => r.ProxyPort).HasColumnName("proxy_port");
            entity.Property(r => r.HostName).HasColumnName("host_name");
            entity.Property(r => r.SiloName).HasColumnName("silo_name");
            entity.Property(r => r.RoleName).HasColumnName("role_name");
            entity.Property(r => r.UpdateZone).HasColumnName("update_zone");
            entity.Property(r => r.FaultZone).HasColumnName("fault_zone");
            entity.Property(r => r.SuspectTimes).HasColumnName("suspect_times");
            entity.Property(r => r.LatestSuspectTicks).HasColumnName("latest_suspect_ticks");
            entity.Property(r => r.StartTicks).HasColumnName("start_ticks");
            entity.Property(r => r.IAmAliveTicks).HasColumnName("i_am_alive_ticks");
            entity.Property(r => r.ETag).HasColumnName("etag").HasMaxLength(16).IsFixedLength();
        });

        SchemaHistoryModel.Configure(modelBuilder);
        return modelBuilder;
    }

    /// <summary>
    /// Records clustering schema <paramref name="version"/> as applied. Call at the end of the <c>Up</c> method of the
    /// migration that EF Core generated after adding or upgrading the model.
    /// </summary>
    /// <remarks>
    /// EF Core generates the table changes from the model, but cannot know about data changes that a schema version makes,
    /// so a version with data steps runs them here, before its ledger row. Version 1 has none. The ledger row is written
    /// by this call rather than seeded into the model, so a migration that misses the call leaves the ledger behind, and
    /// startup validation names the missing version.
    /// </remarks>
    public static MigrationBuilder ApplyOrleansClusteringSchema(this MigrationBuilder migrationBuilder, int version)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(version, ClusteringSchema.LatestVersion);

        migrationBuilder.InsertData(
            SchemaLedger.TableName,
            [SchemaHistoryModel.Component, SchemaHistoryModel.Version, SchemaHistoryModel.Script, SchemaHistoryModel.Checksum, SchemaHistoryModel.Source, SchemaHistoryModel.AppliedOn],
            [ClusteringSchema.Component, version, $"{ClusteringSchema.Component}-{version:0000}", null, SchemaLedger.EntityFrameworkSource, DateTime.UtcNow]);

        return migrationBuilder;
    }
}
