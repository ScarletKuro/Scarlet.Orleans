using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Scarlet.Orleans.Reminders.EntityFrameworkCore;

namespace Scarlet.Orleans.Reminders.Tests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansRemindersModel();
}

internal static class FlavourTable
{
    private static readonly ConcurrentDictionary<string, Lazy<Task>> Schemas = new();

    public static RelationalReminderTable Create(TestDatabase database, string serviceId, TestSchemaMode schemaMode) =>
        Create(CreateStore(database, schemaMode), serviceId);

    public static RelationalReminderTable Create(IReminderStore store, string serviceId) =>
        new(serviceId, store, TimeProvider.System, NullLogger<RelationalReminderTable>.Instance);

    /// <summary>
    /// The EF Core flavour never changes the schema, so <see cref="TestSchemaMode.Apply"/> creates it first, as an
    /// application's migration would, then validates it.
    /// </summary>
    public static IReminderStore CreateStore(TestDatabase database, TestSchemaMode schemaMode)
    {
        var store = new EntityFrameworkCoreReminderStore<TestDbContext>(
            ContextFactory(database),
            schemaMode == TestSchemaMode.None ? SchemaMode.None : SchemaMode.Validate,
            TimeProvider.System,
            NullLogger.Instance);
        return schemaMode == TestSchemaMode.Apply ? new SchemaCreatingStore(database, store) : store;
    }

    public static void AddReminders(ISiloBuilder silo, TestDatabase database)
    {
        EnsureSchemaAsync(database, CancellationToken.None).GetAwaiter().GetResult();
        silo.Services.AddDbContextFactory<TestDbContext>(options => Configure(options, database));
        silo.UseScarletEntityFrameworkCoreReminderService<TestDbContext>();
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
    /// Runs what <c>ApplyOrleansRemindersSchema</c> adds to a migration, as <c>dotnet ef database update</c> would.
    /// </summary>
    public static async Task RecordSchemaVersionAsync(DbContext context, int version, CancellationToken cancellationToken)
    {
        var migration = new MigrationBuilder(context.Database.ProviderName);
        migration.ApplyOrleansRemindersSchema(version);
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

    private sealed class SchemaCreatingStore(TestDatabase database, IReminderStore inner) : IReminderStore
    {
        public async Task InitializeAsync(CancellationToken cancellationToken)
        {
            await EnsureSchemaAsync(database, cancellationToken);
            await inner.InitializeAsync(cancellationToken);
        }

        public Task<StoredReminder?> ReadAsync(ReminderKey key, CancellationToken cancellationToken) => inner.ReadAsync(key, cancellationToken);

        public Task<IReadOnlyList<StoredReminder>> ReadGrainAsync(byte[] grainKeyHash, CancellationToken cancellationToken) =>
            inner.ReadGrainAsync(grainKeyHash, cancellationToken);

        public Task<IReadOnlyList<StoredReminder>> ReadRangeAsync(byte[] serviceHash, uint begin, uint end, CancellationToken cancellationToken) =>
            inner.ReadRangeAsync(serviceHash, begin, end, cancellationToken);

        public Task<bool> TryInsertAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
            inner.TryInsertAsync(key, startAt, period, etag, cancellationToken);

        public Task<bool> TryUpdateAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
            inner.TryUpdateAsync(key, startAt, period, etag, cancellationToken);

        public Task<bool> TryDeleteAsync(ReminderKey key, byte[] expectedETag, CancellationToken cancellationToken) =>
            inner.TryDeleteAsync(key, expectedETag, cancellationToken);

        public Task ClearAsync(byte[] serviceHash, CancellationToken cancellationToken) => inner.ClearAsync(serviceHash, cancellationToken);
    }
}
