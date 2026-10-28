using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Scarlet.Orleans.GrainDirectory.AdoNet;

namespace Scarlet.Orleans.GrainDirectory.Tests;

internal static class FlavourDirectory
{
    public static RelationalGrainDirectory Create(TestDatabase database, string name, string clusterId, TestSchemaMode schemaMode) =>
        Create(CreateStore(database, schemaMode), name, clusterId);

    public static RelationalGrainDirectory Create(IGrainDirectoryStore store, string name, string clusterId) =>
        new(name, clusterId, store, initStage: 0, TimeProvider.System, NullLogger<RelationalGrainDirectory>.Instance);

    public static IGrainDirectoryStore CreateStore(TestDatabase database, TestSchemaMode schemaMode) =>
        new AdoNetGrainDirectoryStore(database.DataSource, EngineOption(database), (SchemaMode)schemaMode, TimeProvider.System, NullLogger.Instance);

    public static void AddDirectory(ISiloBuilder silo, string name, TestDatabase database) =>
        silo.AddScarletAdoNetGrainDirectory(name, options =>
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
