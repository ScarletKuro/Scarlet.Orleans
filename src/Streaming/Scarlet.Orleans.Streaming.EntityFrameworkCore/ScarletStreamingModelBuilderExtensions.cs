using Microsoft.EntityFrameworkCore.Migrations;
using Scarlet.Orleans.Relational.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;
using Scarlet.Orleans.Streaming;
using Scarlet.Orleans.Streaming.EntityFrameworkCore;

namespace Microsoft.EntityFrameworkCore;

/// <summary>
/// Adds the Orleans streaming tables to an EF Core model and migrations.
/// </summary>
public static class ScarletStreamingModelBuilderExtensions
{
    /// <summary>
    /// Maps the <c>orleans_stream_queue</c>, <c>orleans_stream_message</c> and <c>orleans_stream_dead_letter</c> tables
    /// and the <c>orleans_schema_history</c> ledger. Call from <c>OnModelCreating</c>.
    /// </summary>
    /// <remarks>
    /// Only provider-neutral configuration is used, chosen so that every supported provider generates exactly the tables
    /// of <c>Scarlet.Orleans.Streaming.AdoNet</c>'s SQL scripts, which then runs the queues on them. A test compares the
    /// two on each engine. The message ID is each provider's default identity column, which never reuses a value.
    /// </remarks>
    public static ModelBuilder ApplyOrleansStreamingModel(this ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        modelBuilder.Entity<StreamQueueRecord>(entity =>
        {
            entity.ToTable(StreamingSchema.QueueTableName);
            entity.HasKey(r => r.QueueHash).HasName("pk_orleans_stream_queue");
            entity.Property(r => r.QueueHash).HasColumnName("queue_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.QueueIdentity).HasColumnName("queue_identity");
            entity.Property(r => r.ServiceId).HasColumnName("service_id");
            entity.Property(r => r.ProviderName).HasColumnName("provider_name");
            entity.Property(r => r.QueueId).HasColumnName("queue_id");
            entity.Property(r => r.EvictOn).HasColumnName("evict_on");
            entity.Property(r => r.CreatedOn).HasColumnName("created_on");
        });

        modelBuilder.Entity<StreamMessageRecord>(entity =>
        {
            entity.ToTable(StreamingSchema.MessageTableName);
            entity.HasKey(r => r.MessageId).HasName("pk_orleans_stream_message");
            entity.Property(r => r.MessageId).HasColumnName("message_id").ValueGeneratedOnAdd();
            entity.Property(r => r.QueueHash).HasColumnName("queue_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.MessageKey).HasColumnName("message_key").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.Dequeued).HasColumnName("dequeued");
            entity.Property(r => r.VisibleOn).HasColumnName("visible_on");
            entity.Property(r => r.ExpiresOn).HasColumnName("expires_on");
            entity.Property(r => r.CreatedOn).HasColumnName("created_on");
            entity.Property(r => r.Payload).HasColumnName("payload");
            entity.HasIndex(r => new { r.QueueHash, r.MessageId }).IsUnique().HasDatabaseName("ix_orleans_stream_message_queue");
            entity.HasIndex(r => r.MessageKey).IsUnique().HasDatabaseName("ix_orleans_stream_message_key");
        });

        modelBuilder.Entity<StreamDeadLetterRecord>(entity =>
        {
            entity.ToTable(StreamingSchema.DeadLetterTableName);
            entity.HasKey(r => new { r.QueueHash, r.MessageId }).HasName("pk_orleans_stream_dead_letter");
            entity.Property(r => r.QueueHash).HasColumnName("queue_hash").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.MessageId).HasColumnName("message_id").ValueGeneratedNever();
            entity.Property(r => r.MessageKey).HasColumnName("message_key").HasMaxLength(16).IsFixedLength();
            entity.Property(r => r.Dequeued).HasColumnName("dequeued");
            entity.Property(r => r.ExpiresOn).HasColumnName("expires_on");
            entity.Property(r => r.CreatedOn).HasColumnName("created_on");
            entity.Property(r => r.Payload).HasColumnName("payload");
            entity.Property(r => r.DeadOn).HasColumnName("dead_on");
            entity.Property(r => r.RemoveOn).HasColumnName("remove_on");
            entity.HasIndex(r => new { r.QueueHash, r.RemoveOn }).HasDatabaseName("ix_orleans_stream_dead_letter_remove_on");
        });

        SchemaHistoryModel.Configure(modelBuilder);
        return modelBuilder;
    }

    /// <summary>
    /// Records streaming schema <paramref name="version"/> as applied. Call at the end of the <c>Up</c> method of the
    /// migration that EF Core generated after adding or upgrading the model.
    /// </summary>
    /// <remarks>
    /// EF Core generates the table changes from the model, but cannot know about data changes that a schema version makes,
    /// so a version with data steps runs them here, before its ledger row. Version 1 has none.
    /// </remarks>
    public static MigrationBuilder ApplyOrleansStreamingSchema(this MigrationBuilder migrationBuilder, int version)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(version, StreamingSchema.LatestVersion);

        migrationBuilder.InsertData(
            SchemaLedger.TableName,
            [SchemaHistoryModel.Component, SchemaHistoryModel.Version, SchemaHistoryModel.Script, SchemaHistoryModel.Checksum, SchemaHistoryModel.Source, SchemaHistoryModel.AppliedOn],
            [StreamingSchema.Component, version, $"{StreamingSchema.Component}-{version:0000}", null, SchemaLedger.EntityFrameworkSource, DateTime.UtcNow]);

        return migrationBuilder;
    }
}
