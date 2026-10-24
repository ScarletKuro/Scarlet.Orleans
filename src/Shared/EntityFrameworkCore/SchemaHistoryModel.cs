using Microsoft.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Relational.EntityFrameworkCore;

/// <summary>
/// The <c>orleans_schema_history</c> ledger in the EF Core model, with the layout the ADO.NET runner creates.
/// </summary>
/// <remarks>
/// The ledger is a shared-type entity, a property bag identified by the table name. Every Orleans EF Core package that
/// maps it configures the same entity type, so several components can share one ledger in one model, while no package
/// exposes, or depends on another's, ledger type.
/// </remarks>
internal static class SchemaHistoryModel
{
    public const string Component = "component";
    public const string Version = "version";
    public const string Script = "script";
    public const string Checksum = "checksum";
    public const string Source = "source";
    public const string AppliedOn = "applied_on";

    public static void Configure(ModelBuilder modelBuilder) =>
        modelBuilder.SharedTypeEntity<Dictionary<string, object>>(SchemaLedger.TableName, entity =>
        {
            entity.ToTable(SchemaLedger.TableName);
            entity.IndexerProperty<string>(Component).HasMaxLength(128).IsRequired();
            entity.IndexerProperty<int>(Version);
            entity.IndexerProperty<string>(Script).HasMaxLength(256).IsRequired();
            entity.IndexerProperty<string?>(Checksum).HasMaxLength(64);
            entity.IndexerProperty<string>(Source).HasMaxLength(16).IsRequired();
            entity.IndexerProperty<DateTime>(AppliedOn);
            entity.HasKey(Component, Version).HasName($"pk_{SchemaLedger.TableName}");
        });

    public static IQueryable<Dictionary<string, object>> Set(DbContext context) =>
        context.Set<Dictionary<string, object>>(SchemaLedger.TableName).AsNoTracking();
}
