using Microsoft.EntityFrameworkCore.Migrations;
using Scarlet.Orleans.GrainDirectory;
using Scarlet.Orleans.GrainDirectory.EntityFrameworkCore;
using Scarlet.Orleans.Relational.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Adds the Orleans grain directory tables to an EF Core model and migrations.
/// </summary>
public static class ScarletGrainDirectoryModelBuilderExtensions
{
    /// <summary>
    /// Maps the <c>orleans_grain_directory</c> table and the <c>orleans_schema_history</c> ledger. Call from <c>OnModelCreating</c>.
    /// </summary>
    /// <remarks>
    /// Only provider-neutral configuration is used, chosen so that every supported provider generates exactly the tables
    /// of the ADO.NET flavour's SQL scripts, and either flavour can use a database created by the other. A test compares
    /// the two on each engine. The ledger can be shared with other Orleans components in the same model.
    /// </remarks>
    public static ModelBuilder ApplyOrleansGrainDirectoryModel(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<GrainDirectoryRecord>(entity =>
        {
            entity.ToTable(GrainDirectorySchema.TableName);
            entity.HasKey(r => r.KeyHash).HasName("pk_orleans_grain_directory");
            entity.Property(r => r.KeyHash).HasColumnName("key_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.GrainIdentity).HasColumnName("grain_identity");
            entity.Property(r => r.ScopeHash).HasColumnName("scope_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.SiloHash).HasColumnName("silo_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.ClusterId).HasColumnName("cluster_id");
            entity.Property(r => r.DirectoryName).HasColumnName("directory_name");
            entity.Property(r => r.GrainId).HasColumnName("grain_id");
            entity.Property(r => r.SiloAddress).HasColumnName("silo_address");
            entity.Property(r => r.ActivationId).HasColumnName("activation_id").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.MembershipVersion).HasColumnName("membership_version");
            entity.HasIndex(r => new { r.ScopeHash, r.SiloHash, r.KeyHash }).HasDatabaseName("ix_orleans_grain_directory_silo");
        });

        SchemaHistoryModel.Configure(modelBuilder);
        return modelBuilder;
    }

    /// <summary>
    /// Records grain directory schema <paramref name="version"/> as applied. Call at the end of the <c>Up</c> method of the
    /// migration that EF Core generated after adding or upgrading the model.
    /// </summary>
    /// <remarks>
    /// EF Core generates the table changes from the model, but cannot know about data changes that a schema version makes,
    /// so a version with data steps runs them here, before its ledger row. Version 1 has none. The ledger row is written
    /// by this call rather than seeded into the model, so a migration that misses the call leaves the ledger behind, and
    /// startup validation names the missing version.
    /// </remarks>
    public static MigrationBuilder ApplyOrleansGrainDirectorySchema(this MigrationBuilder migrationBuilder, int version)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(version, GrainDirectorySchema.LatestVersion);

        migrationBuilder.InsertData(
            SchemaLedger.TableName,
            [SchemaHistoryModel.Component, SchemaHistoryModel.Version, SchemaHistoryModel.Script, SchemaHistoryModel.Checksum, SchemaHistoryModel.Source, SchemaHistoryModel.AppliedOn],
            [GrainDirectorySchema.Component, version, $"{GrainDirectorySchema.Component}-{version:0000}", null, SchemaLedger.EntityFrameworkSource, DateTime.UtcNow]);

        return migrationBuilder;
    }
}
