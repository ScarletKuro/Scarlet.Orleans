using Microsoft.Extensions.Logging.Abstractions;
using Scarlet.Orleans.Reminders.AdoNet;

namespace Scarlet.Orleans.Reminders.Tests;

internal static class FlavourTable
{
    public static RelationalReminderTable Create(TestDatabase database, string serviceId, TestSchemaMode schemaMode) =>
        Create(CreateStore(database, schemaMode), serviceId);

    public static RelationalReminderTable Create(IReminderStore store, string serviceId) =>
        new(serviceId, store, TimeProvider.System, NullLogger<RelationalReminderTable>.Instance);

    public static IReminderStore CreateStore(TestDatabase database, TestSchemaMode schemaMode) =>
        new AdoNetReminderStore(database.DataSource, EngineOption(database), (SchemaMode)schemaMode, TimeProvider.System, NullLogger.Instance);

    /// <summary>
    /// Leaves the engine to inference, except for MariaDB, which shares MySQL's driver.
    /// </summary>
    private static string? EngineOption(TestDatabase database) => database.Engine == "MariaDb" ? "MariaDb" : null;
}
