using Microsoft.Extensions.Logging.Abstractions;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Scarlet.Orleans.Streaming.Tests;

internal static class FlavourStreaming
{
    public static IStreamQueueStore CreateStore(TestDatabase database, TestSchemaMode schemaMode) =>
        new AdoNetStreamQueueStore(database.DataSource, EngineOption(database), (SchemaMode)schemaMode, TimeProvider.System, NullLogger.Instance);

    /// <summary>
    /// Leaves the engine to inference, except for MariaDB, which shares MySQL's driver.
    /// </summary>
    private static string? EngineOption(TestDatabase database) => database.Engine == "MariaDb" ? "MariaDb" : null;
}
