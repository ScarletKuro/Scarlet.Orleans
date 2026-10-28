using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Scarlet.Orleans.GrainDirectory.EntityFrameworkCore;

namespace Scarlet.Orleans.GrainDirectory.Tests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansGrainDirectoryModel();
}

internal static class FlavourDirectory
{
    private static readonly ConcurrentDictionary<string, Lazy<Task>> Schemas = new();

    public static RelationalGrainDirectory Create(TestDatabase database, string name, string clusterId, TestSchemaMode schemaMode) =>
        Create(CreateStore(database, schemaMode), name, clusterId);

    public static RelationalGrainDirectory Create(IGrainDirectoryStore store, string name, string clusterId) =>
        new(name, clusterId, store, initStage: 0, TimeProvider.System, NullLogger<RelationalGrainDirectory>.Instance);

    /// <summary>
    /// The EF Core flavour never changes the schema, so <see cref="TestSchemaMode.Apply"/> creates it first, as an
    /// application's migration would, then validates it.
    /// </summary>
    public static IGrainDirectoryStore CreateStore(TestDatabase database, TestSchemaMode schemaMode)
    {
        var store = new EntityFrameworkCoreGrainDirectoryStore<TestDbContext>(
            ContextFactory(database),
            schemaMode == TestSchemaMode.None ? SchemaMode.None : SchemaMode.Validate,
            TimeProvider.System,
            NullLogger.Instance);
        return schemaMode == TestSchemaMode.Apply ? new SchemaCreatingStore(database, store) : store;
    }

    public static void AddDirectory(ISiloBuilder silo, string name, TestDatabase database)
    {
        EnsureSchemaAsync(database, CancellationToken.None).GetAwaiter().GetResult();
        silo.Services.AddDbContextFactory<TestDbContext>(options => Configure(options, database));
        silo.AddScarletEntityFrameworkCoreGrainDirectory<TestDbContext>(name);
    }

    /// <summary>
    /// Creates the Orleans tables from the model and records schema version 1, once per database, as the application's
    /// migration would.
    /// </summary>
    public static Task EnsureSchemaAsync(TestDatabase database, CancellationToken cancellationToken) =>
        Schemas.GetOrAdd(database.ConnectionString, _ => new Lazy<Task>(() => CreateSchemaAsync(database))).Value.WaitAsync(cancellationToken);

    private static async Task CreateSchemaAsync(TestDatabase database)
    {
        await using var context = await ContextFactory(database).CreateDbContextAsync();
        if (await context.Database.EnsureCreatedAsync())
        {
            await RecordSchemaVersionAsync(context, 1, CancellationToken.None);
        }
    }

    /// <summary>
    /// Runs what <c>ApplyOrleansGrainDirectorySchema</c> adds to a migration, as <c>dotnet ef database update</c> would.
    /// </summary>
    public static async Task RecordSchemaVersionAsync(DbContext context, int version, CancellationToken cancellationToken)
    {
        var migration = new MigrationBuilder(context.Database.ProviderName);
        migration.ApplyOrleansGrainDirectorySchema(version);
        var commands = context.GetService<IMigrationsSqlGenerator>().Generate(migration.Operations, context.Model);
        await context.GetService<IMigrationCommandExecutor>().ExecuteNonQueryAsync(commands, context.GetService<IRelationalConnection>(), cancellationToken);
    }

    public static IDbContextFactory<TestDbContext> ContextFactory(TestDatabase database) =>
        new PooledDbContextFactory<TestDbContext>(Configure(new DbContextOptionsBuilder<TestDbContext>(), database).Options);

    public static TBuilder Configure<TBuilder>(TBuilder builder, TestDatabase database)
        where TBuilder : DbContextOptionsBuilder
    {
        switch (database.Engine)
        {
            case "Sqlite":
                builder.UseSqlite(database.ConnectionString);
                break;
            case "PostgreSql":
                builder.UseNpgsql(database.ConnectionString);
                break;
            case "SqlServer":
                builder.UseSqlServer(database.ConnectionString);
                break;
            case "MySql" when database.Driver == TestDrivers.MySqlData:
                builder.UseMySQL(database.ConnectionString);
                break;
            case "MySql" or "MariaDb":
                builder.UseMySql(database.ConnectionString, ServerVersion.AutoDetect(database.ConnectionString));
                break;
            default:
                throw new NotSupportedException(database.Engine);
        }

        return builder;
    }

    private sealed class SchemaCreatingStore(TestDatabase database, IGrainDirectoryStore inner) : IGrainDirectoryStore
    {
        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            await EnsureSchemaAsync(database, cancellationToken);
            await inner.InitializeAsync(cancellationToken);
        }

        public Task<StoredDirectoryEntry?> ReadAsync(DirectoryKey key, CancellationToken cancellationToken) => inner.ReadAsync(key, cancellationToken);

        public Task<bool> TryInsertAsync(DirectoryKey key, DirectoryEntry entry, CancellationToken cancellationToken) =>
            inner.TryInsertAsync(key, entry, cancellationToken);

        public Task<bool> TryReplaceAsync(DirectoryKey key, byte[] expectedActivationId, DirectoryEntry entry, CancellationToken cancellationToken) =>
            inner.TryReplaceAsync(key, expectedActivationId, entry, cancellationToken);

        public Task<bool> TryDeleteAsync(DirectoryKey key, byte[] expectedActivationId, CancellationToken cancellationToken) =>
            inner.TryDeleteAsync(key, expectedActivationId, cancellationToken);

        public Task<int> DeleteSiloBatchAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, int limit, CancellationToken cancellationToken) =>
            inner.DeleteSiloBatchAsync(scopeHash, siloHash, siloAddress, limit, cancellationToken);

        public Task<bool> HasSiloEntriesAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, CancellationToken cancellationToken) =>
            inner.HasSiloEntriesAsync(scopeHash, siloHash, siloAddress, cancellationToken);
    }
}
