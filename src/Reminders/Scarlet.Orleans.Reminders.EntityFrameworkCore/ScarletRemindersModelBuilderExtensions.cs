using Microsoft.EntityFrameworkCore.Migrations;
using Scarlet.Orleans.Relational.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;
using Scarlet.Orleans.Reminders;
using Scarlet.Orleans.Reminders.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Adds the Orleans reminder tables to an EF Core model and migrations.
/// </summary>
public static class ScarletRemindersModelBuilderExtensions
{
    /// <summary>
    /// Maps the <c>orleans_reminder</c> table and the <c>orleans_schema_history</c> ledger. Call from <c>OnModelCreating</c>.
    /// </summary>
    /// <remarks>
    /// Only provider-neutral configuration is used, chosen so that every supported provider generates exactly the tables
    /// of the ADO.NET flavour's SQL scripts, and either flavour can use a database created by the other. A test compares
    /// the two on each engine. The ledger can be shared with other Orleans components in the same model.
    /// </remarks>
    public static ModelBuilder ApplyOrleansRemindersModel(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<ReminderRecord>(entity =>
        {
            entity.ToTable(RemindersSchema.TableName);
            entity.HasKey(r => r.KeyHash).HasName("pk_orleans_reminder");
            entity.Property(r => r.KeyHash).HasColumnName("key_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.ReminderIdentity).HasColumnName("reminder_identity");
            entity.Property(r => r.ServiceHash).HasColumnName("service_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.GrainKeyHash).HasColumnName("grain_key_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.GrainHash).HasColumnName("grain_hash");
            entity.Property(r => r.ServiceId).HasColumnName("service_id");
            entity.Property(r => r.GrainId).HasColumnName("grain_id");
            entity.Property(r => r.ReminderName).HasColumnName("reminder_name");
            entity.Property(r => r.StartAtTicks).HasColumnName("start_at_ticks");
            entity.Property(r => r.PeriodTicks).HasColumnName("period_ticks");
            entity.Property(r => r.ETag).HasColumnName("etag").HasMaxLength(16).IsFixedLength();
            entity.HasIndex(r => new { r.ServiceHash, r.GrainHash }).HasDatabaseName("ix_orleans_reminder_range");
            entity.HasIndex(r => r.GrainKeyHash).HasDatabaseName("ix_orleans_reminder_grain");
        });

        SchemaHistoryModel.Configure(modelBuilder);
        return modelBuilder;
    }

    /// <summary>
    /// Records reminders schema <paramref name="version"/> as applied. Call at the end of the <c>Up</c> method of the
    /// migration that EF Core generated after adding or upgrading the model.
    /// </summary>
    /// <remarks>
    /// EF Core generates the table changes from the model, but cannot know about data changes that a schema version makes,
    /// so a version with data steps runs them here, before its ledger row. Version 1 has none. The ledger row is written
    /// by this call rather than seeded into the model, so a migration that misses the call leaves the ledger behind, and
    /// startup validation names the missing version.
    /// </remarks>
    public static MigrationBuilder ApplyOrleansRemindersSchema(this MigrationBuilder migrationBuilder, int version)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(version, RemindersSchema.LatestVersion);

        migrationBuilder.InsertData(
            SchemaLedger.TableName,
            [SchemaHistoryModel.Component, SchemaHistoryModel.Version, SchemaHistoryModel.Script, SchemaHistoryModel.Checksum, SchemaHistoryModel.Source, SchemaHistoryModel.AppliedOn],
            [RemindersSchema.Component, version, $"{RemindersSchema.Component}-{version:0000}", null, SchemaLedger.EntityFrameworkSource, DateTime.UtcNow]);

        return migrationBuilder;
    }
}
