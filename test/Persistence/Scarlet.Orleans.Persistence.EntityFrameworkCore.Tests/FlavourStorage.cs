using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;

namespace Scarlet.Orleans.Persistence.Tests;

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansPersistenceModel();
}

internal static class FlavourStorage
{
    public static RelationalGrainStorage Create(TestDatabase database, string serviceId, bool deleteStateOnClear, TestSchemaMode schemaMode) =>
        new(
            "test",
            serviceId,
            new EntityFrameworkCoreGrainStateStore<TestDbContext>(ContextFactory(database), (SchemaMode)schemaMode, TimeProvider.System),
            new JsonTestSerializer(),
            new TestActivatorProvider(),
            deleteStateOnClear,
            initStage: 0,
            TimeProvider.System,
            NullLogger<RelationalGrainStorage>.Instance);

    public static void AddStorage(ISiloBuilder silo, string name, TestDatabase database)
    {
        silo.Services.AddDbContextFactory<TestDbContext>(options => Configure(options, database));
        silo.AddScarletEntityFrameworkCoreGrainStorage<TestDbContext>(name, options => options.SchemaMode = SchemaMode.Apply);
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
            default:
                throw new NotSupportedException(database.Engine);
        }

        return builder;
    }
}
