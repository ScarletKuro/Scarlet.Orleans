using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Messaging;
using Scarlet.Orleans.Clustering.AdoNet;

namespace Scarlet.Orleans.Clustering.Tests;

internal static class FlavourMembership
{
    public static RelationalMembershipTable CreateTable(TestDatabase database, string clusterId, TestSchemaMode schemaMode) =>
        CreateTable(CreateStore(database, schemaMode), clusterId);

    public static RelationalMembershipTable CreateTable(IMembershipStore store, string clusterId) =>
        new(clusterId, store, TimeProvider.System, NullLogger<RelationalMembershipTable>.Instance);

    public static IMembershipStore CreateStore(TestDatabase database, TestSchemaMode schemaMode) =>
        new AdoNetMembershipStore(database.DataSource, EngineOption(database), (SchemaMode)schemaMode, TimeProvider.System, NullLogger.Instance);

    public static IGatewayListProvider CreateGatewayListProvider(TestDatabase database, string clusterId) =>
        new RelationalGatewayListProvider(clusterId, CreateStore(database, TestSchemaMode.Validate), TimeSpan.FromMinutes(1));

    public static void UseClustering(ISiloBuilder silo, TestDatabase database) =>
        silo.UseScarletAdoNetClustering(options =>
        {
            options.DataSource = database.DataSource;
            options.Engine = EngineOption(database);
            options.SchemaMode = SchemaMode.Apply;
        });

    public static void UseClustering(IClientBuilder client, TestDatabase database) =>
        client.UseScarletAdoNetClustering(options =>
        {
            options.DataSource = database.DataSource;
            options.Engine = EngineOption(database);
        });

    /// <summary>
    /// Leaves the engine to inference, except for MariaDB, which shares MySQL's driver.
    /// </summary>
    private static string? EngineOption(TestDatabase database) => database.Engine == "MariaDb" ? "MariaDb" : null;
}
