using Microsoft.EntityFrameworkCore.Migrations;
using Scarlet.Orleans.Persistence;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Adds the Orleans grain state table to an EF Core model and migrations.
/// </summary>
public static class ScarletPersistenceModelBuilderExtensions
{
    /// <summary>
    /// Maps the <c>orleans_grain_state</c> table. Call from <c>OnModelCreating</c>.
    /// </summary>
    /// <remarks>
    /// Only provider-neutral configuration is used, chosen so that every supported provider generates exactly the
    /// tables of the SQL schema scripts. A test compares the two on each engine.
    /// </remarks>
    public static ModelBuilder ApplyOrleansPersistenceModel(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<GrainStateRecord>(entity =>
        {
            entity.ToTable(PersistenceSchema.TableName);
            entity.HasKey(r => r.KeyHash).HasName("pk_orleans_grain_state");
            entity.Property(r => r.KeyHash).HasColumnName("key_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.GrainIdentity).HasColumnName("grain_identity");
            entity.Property(r => r.ServiceId).HasColumnName("service_id");
            entity.Property(r => r.StateName).HasColumnName("state_name");
            entity.Property(r => r.GrainId).HasColumnName("grain_id");
            entity.Property(r => r.Payload).HasColumnName("payload");
            entity.Property(r => r.ETag).HasColumnName("etag").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.ModifiedOn).HasColumnName("modified_on");
        });

        return modelBuilder;
    }

    /// <summary>
    /// Records persistence schema <paramref name="version"/> as applied, and runs the data steps of that version.
    /// Call at the end of the <c>Up</c> method of the migration that EF generated after adding or upgrading the model.
    /// </summary>
    /// <remarks>
    /// EF generates the table changes from the model, but cannot know about data changes that a schema version makes,
    /// so those come from here. The ledger row is written by this call, not seeded into the model, so a migration that
    /// misses this call leaves the ledger behind and startup validation names the missing version.
    /// </remarks>
    public static MigrationBuilder ApplyOrleansPersistenceSchema(this MigrationBuilder migrationBuilder, int version)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);

        var engine = EntityFrameworkEngines.FromProviderName(migrationBuilder.ActiveProvider);
        var scripts = SchemaScript.Load(typeof(PersistenceSchema).Assembly, engine);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(version, scripts.Count);

        migrationBuilder.Sql(SchemaLedger.CreateTable(engine));

        // Version 1 has no data steps. Versions that do emit them here, before the ledger row.
        migrationBuilder.Sql(
            $"INSERT INTO {SchemaLedger.TableName} (component, version, script, checksum, source, applied_on) " +
            $"VALUES ('{PersistenceSchema.Component}', {version}, '{scripts[version - 1].Name}', NULL, '{SchemaLedger.EntityFrameworkSource}', CURRENT_TIMESTAMP)");

        return migrationBuilder;
    }
}
