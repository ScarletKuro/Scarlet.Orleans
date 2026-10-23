using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Scarlet.Orleans.Persistence.AdoNet;

namespace Scarlet.Orleans.Persistence.Tests;

internal static class FlavourStorage
{
    public static RelationalGrainStorage Create(TestDatabase database, string serviceId, bool deleteStateOnClear, TestSchemaMode schemaMode) =>
        new(
            "test",
            serviceId,
            new AdoNetGrainStateStore(database.DataSource, EngineOption(database), (SchemaMode)schemaMode, TimeProvider.System),
            new JsonTestSerializer(),
            new TestActivatorProvider(),
            deleteStateOnClear,
            initStage: 0,
            TimeProvider.System,
            NullLogger<RelationalGrainStorage>.Instance);

    public static void AddStorage(ISiloBuilder silo, string name, TestDatabase database) =>
        silo.AddScarletAdoNetGrainStorage(name, options =>
        {
            options.DataSource = database.DataSource;
            options.Engine = EngineOption(database);
            options.SchemaMode = SchemaMode.Apply;
        });

    /// <summary>
    /// Leaves the engine to inference, except for MariaDB, which shares MySQL's driver.
    /// </summary>
    private static string? EngineOption(TestDatabase database) => database.Engine == "MariaDb" ? "MariaDb" : null;
}
