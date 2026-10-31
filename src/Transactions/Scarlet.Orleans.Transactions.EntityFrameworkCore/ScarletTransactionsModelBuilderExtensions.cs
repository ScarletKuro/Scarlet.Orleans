using Microsoft.EntityFrameworkCore.Migrations;
using Scarlet.Orleans.Relational.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;
using Scarlet.Orleans.Transactions;
using Scarlet.Orleans.Transactions.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Adds the Orleans transactional state tables to an EF Core model and migrations.
/// </summary>
public static class ScarletTransactionsModelBuilderExtensions
{
    /// <summary>
    /// Maps the <c>orleans_transactional_state</c> and <c>orleans_transactional_state_entry</c> tables and the
    /// <c>orleans_schema_history</c> ledger. Call from <c>OnModelCreating</c>.
    /// </summary>
    /// <remarks>
    /// Only provider-neutral configuration is used, chosen so that every supported provider generates exactly the tables
    /// of the ADO.NET flavour's SQL scripts, and either flavour can use a database created by the other. A test compares
    /// the two on each engine. The ledger can be shared with other Orleans components in the same model.
    /// </remarks>
    public static ModelBuilder ApplyOrleansTransactionsModel(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<TransactionalStateRecord>(entity =>
        {
            entity.ToTable(TransactionsSchema.StateTableName);
            entity.HasKey(r => r.KeyHash).HasName("pk_orleans_transactional_state");
            entity.Property(r => r.KeyHash).HasColumnName("key_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.StateIdentity).HasColumnName("state_identity");
            entity.Property(r => r.ServiceId).HasColumnName("service_id");
            entity.Property(r => r.StateName).HasColumnName("state_name");
            entity.Property(r => r.GrainId).HasColumnName("grain_id");
            entity.Property(r => r.ETag).HasColumnName("etag").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.CommittedSequenceId).HasColumnName("committed_sequence_id");
            entity.Property(r => r.Metadata).HasColumnName("metadata");
            entity.Property(r => r.ModifiedOn).HasColumnName("modified_on");
        });

        modelBuilder.Entity<TransactionalStateEntryRecord>(entity =>
        {
            entity.ToTable(TransactionsSchema.EntryTableName);
            entity.HasKey(r => new { r.KeyHash, r.SequenceId }).HasName("pk_orleans_transactional_state_entry");
            entity.Property(r => r.KeyHash).HasColumnName("key_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.SequenceId).HasColumnName("sequence_id").ValueGeneratedNever();
            entity.Property(r => r.TransactionId).HasColumnName("transaction_id");
            entity.Property(r => r.TimestampTicks).HasColumnName("timestamp_ticks");
            entity.Property(r => r.TransactionManager).HasColumnName("transaction_manager");
            entity.Property(r => r.Payload).HasColumnName("payload");
        });

        SchemaHistoryModel.Configure(modelBuilder);
        return modelBuilder;
    }

    /// <summary>
    /// Records transactions schema <paramref name="version"/> as applied. Call at the end of the <c>Up</c> method of the
    /// migration that EF Core generated after adding or upgrading the model.
    /// </summary>
    /// <remarks>
    /// EF Core generates the table changes from the model, but cannot know about data changes that a schema version makes,
    /// so a version with data steps runs them here, before its ledger row. Version 1 has none.
    /// </remarks>
    public static MigrationBuilder ApplyOrleansTransactionsSchema(this MigrationBuilder migrationBuilder, int version)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(version, TransactionsSchema.LatestVersion);

        migrationBuilder.InsertData(
            SchemaLedger.TableName,
            [SchemaHistoryModel.Component, SchemaHistoryModel.Version, SchemaHistoryModel.Script, SchemaHistoryModel.Checksum, SchemaHistoryModel.Source, SchemaHistoryModel.AppliedOn],
            [TransactionsSchema.Component, version, $"{TransactionsSchema.Component}-{version:0000}", null, SchemaLedger.EntityFrameworkSource, DateTime.UtcNow]);

        return migrationBuilder;
    }
}
