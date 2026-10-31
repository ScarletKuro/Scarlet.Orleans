using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Scarlet.Orleans.Transactions.AdoNet;

namespace Scarlet.Orleans.Transactions.Tests;

internal static class FlavourTransactions
{
    public static ITransactionalStateStore CreateStore(TestDatabase database, TestSchemaMode schemaMode) =>
        new AdoNetTransactionalStateStore(database.DataSource, EngineOption(database), (SchemaMode)schemaMode, TimeProvider.System, NullLogger.Instance);

    public static void AddStorage(ISiloBuilder silo, string name, TestDatabase database) =>
        silo.AddScarletAdoNetTransactionalStateStorage(name, options =>
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
