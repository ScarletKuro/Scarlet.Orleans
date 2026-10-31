using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Messaging;
using Scarlet.Orleans.Clustering.EntityFrameworkCore;

namespace Scarlet.Orleans.Clustering.Tests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansClusteringModel();
}

internal static class FlavourMembership
{
    private static readonly ConcurrentDictionary<string, Lazy<Task>> Schemas = new();

    public static RelationalMembershipTable CreateTable(TestDatabase database, string clusterId, TestSchemaMode schemaMode) =>
        CreateTable(CreateStore(database, schemaMode), clusterId);

    public static RelationalMembershipTable CreateTable(IMembershipStore store, string clusterId) =>
        new(clusterId, store, TimeProvider.System, NullLogger<RelationalMembershipTable>.Instance);

    /// <summary>
    /// The EF Core flavour never changes the schema, so <see cref="TestSchemaMode.Apply"/> creates it first, as an
    /// application's migration would, then validates it.
    /// </summary>
    public static IMembershipStore CreateStore(TestDatabase database, TestSchemaMode schemaMode)
    {
        var store = new EntityFrameworkCoreMembershipStore<TestDbContext>(
            ContextFactory(database),
            schemaMode == TestSchemaMode.None ? SchemaMode.None : SchemaMode.Validate,
            TimeProvider.System,
            NullLogger.Instance);
        return schemaMode == TestSchemaMode.Apply ? new SchemaCreatingStore(database, store) : store;
    }

    public static IGatewayListProvider CreateGatewayListProvider(TestDatabase database, string clusterId) =>
        new RelationalGatewayListProvider(clusterId, CreateStore(database, TestSchemaMode.Validate), TimeSpan.FromMinutes(1));

    public static void UseClustering(ISiloBuilder silo, TestDatabase database)
    {
        EnsureSchemaAsync(database, CancellationToken.None).GetAwaiter().GetResult();
        silo.Services.AddDbContextFactory<TestDbContext>(options => Configure(options, database));
        silo.UseScarletEntityFrameworkCoreClustering<TestDbContext>();
    }

    public static void UseClustering(IClientBuilder client, TestDatabase database)
    {
        client.Services.AddDbContextFactory<TestDbContext>(options => Configure(options, database));
        client.UseScarletEntityFrameworkCoreClustering<TestDbContext>();
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
    /// Runs what <c>ApplyOrleansClusteringSchema</c> adds to a migration, as <c>dotnet ef database update</c> would.
    /// </summary>
    public static async Task RecordSchemaVersionAsync(DbContext context, int version, CancellationToken cancellationToken)
    {
        var migration = new MigrationBuilder(context.Database.ProviderName);
        migration.ApplyOrleansClusteringSchema(version);
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

    private sealed class SchemaCreatingStore(TestDatabase database, IMembershipStore inner) : IMembershipStore
    {
        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            await EnsureSchemaAsync(database, cancellationToken);
            await inner.InitializeAsync(cancellationToken);
        }

        public Task<byte[]?> TryCreateVersionAsync(ClusterScope scope, byte[] etag, CancellationToken cancellationToken) =>
            inner.TryCreateVersionAsync(scope, etag, cancellationToken);

        public Task<MembershipSnapshot> ReadAsync(ClusterScope scope, byte[]? memberKeyHash, CancellationToken cancellationToken) =>
            inner.ReadAsync(scope, memberKeyHash, cancellationToken);

        public Task<MemberInsertResult> TryInsertAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] etag, CancellationToken cancellationToken) =>
            inner.TryInsertAsync(scope, version, row, etag, cancellationToken);

        public Task<bool> TryUpdateAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] expectedETag, byte[] etag, CancellationToken cancellationToken) =>
            inner.TryUpdateAsync(scope, version, row, expectedETag, etag, cancellationToken);

        public Task UpdateIAmAliveAsync(ClusterScope scope, byte[] memberKeyHash, byte[] memberIdentity, long iAmAliveTicks, CancellationToken cancellationToken) =>
            inner.UpdateIAmAliveAsync(scope, memberKeyHash, memberIdentity, iAmAliveTicks, cancellationToken);

        public Task<(int Deleted, byte[]? VersionIdentity)> DeleteDefunctAsync(ClusterScope scope, long beforeTicks, CancellationToken cancellationToken) =>
            inner.DeleteDefunctAsync(scope, beforeTicks, cancellationToken);

        public Task<byte[]?> DeleteClusterAsync(ClusterScope scope, CancellationToken cancellationToken) => inner.DeleteClusterAsync(scope, cancellationToken);

        public Task<IReadOnlyList<StoredGateway>> ReadGatewaysAsync(ClusterScope scope, CancellationToken cancellationToken) =>
            inner.ReadGatewaysAsync(scope, cancellationToken);
    }
}
