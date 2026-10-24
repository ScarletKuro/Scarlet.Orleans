using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Scarlet.Orleans.Persistence.AdoNet;

namespace Scarlet.Orleans.Persistence.Tests;

internal static class FlavourStorage
{
    public static RelationalGrainStorage Create(TestDatabase database, string serviceId, bool deleteStateOnClear, TestSchemaMode schemaMode) =>
        Create(CreateStore(database, schemaMode), serviceId, deleteStateOnClear);

    public static RelationalGrainStorage Create(IGrainStateStore store, string serviceId, bool deleteStateOnClear, int? maxPayloadSize = null, string name = "test") =>
        new(name, serviceId, store, new JsonTestSerializer(), new TestActivatorProvider(), deleteStateOnClear, maxPayloadSize, initStage: 0, TimeProvider.System, NullLogger<RelationalGrainStorage>.Instance);

    public static IGrainStateStore CreateStore(TestDatabase database, TestSchemaMode schemaMode) =>
        new AdoNetGrainStateStore(database.DataSource, EngineOption(database), (SchemaMode)schemaMode, TimeProvider.System, NullLogger.Instance);

    public static void AddStorage(ISiloBuilder silo, string name, TestDatabase database, bool deleteStateOnClear = false) =>
        silo.AddScarletAdoNetGrainStorage(name, options =>
        {
            options.DataSource = database.DataSource;
            options.DeleteStateOnClear = deleteStateOnClear;
            options.Engine = EngineOption(database);
            options.SchemaMode = SchemaMode.Apply;
        });

    /// <summary>
    /// Leaves the engine to inference, except for MariaDB, which shares MySQL's driver.
    /// </summary>
    private static string? EngineOption(TestDatabase database) => database.Engine == "MariaDb" ? "MariaDb" : null;
}
