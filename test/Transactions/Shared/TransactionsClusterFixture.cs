using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;
using Orleans.Transactions.TestKit;

namespace Scarlet.Orleans.Transactions.Tests;

/// <summary>
/// A cluster for the Orleans transactions TestKit, with the storage under test registered through the package's public
/// hosting extensions as the TestKit's <see cref="TransactionTestConstants.TransactionStore"/>, on a database of
/// <paramref name="database"/>'s engine.
/// </summary>
/// <param name="database">The database the storage uses.</param>
/// <param name="skewedClock">Whether each silo's transaction clock runs seconds apart from the others.</param>
public abstract class TransactionsClusterFixture(DatabaseFixture database, bool skewedClock = false) : IAsyncLifetime
{
    private InProcessTestCluster? cluster;

    public DatabaseFixture Database { get; } = database;

    /// <summary>The cluster's client, which skips the calling test when the engine is not configured.</summary>
    public IGrainFactory GrainFactory => Cluster.Client;

    public IServiceProvider SiloServices => Cluster.Silos[0].ServiceProvider;

    public IServiceProvider ClientServices => Cluster.Client.ServiceProvider;

    private InProcessTestCluster Cluster
    {
        get
        {
            Database.Require();
            return cluster!;
        }
    }

    public async ValueTask InitializeAsync()
    {
        await Database.InitializeAsync();
        if (!Database.IsAvailable)
        {
            return;
        }

        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureHost(host => host.Logging.AddProvider(new WarningLog()));
        builder.ConfigureSilo((_, silo) =>
        {
            silo.Services.AddKeyedSingleton<IRemoteCommitService, RemoteCommitService>(TransactionTestConstants.RemoteCommitService);
            ConfigureStorage(silo, TransactionTestConstants.TransactionStore);
            silo.UseTransactions();
            silo.Services.Configure<ExceptionSerializationOptions>(SerializeDriverExceptions);
            if (skewedClock)
            {
                new SkewedClockConfigurator().Configure(silo);
            }
        });
        builder.ConfigureClient(client =>
        {
            client.UseTransactions();
            client.Services.Configure<ExceptionSerializationOptions>(SerializeDriverExceptions);
        });
        cluster = builder.Build();
        await cluster.DeployAsync();
    }

    /// <summary>
    /// Registers the storage of transactional state under <paramref name="name"/>: the storage under test.
    /// </summary>
    protected virtual void ConfigureStorage(ISiloBuilder silo, string name) => FlavourTransactions.AddStorage(silo, name, Database.Database);

    /// <summary>
    /// Lets a storage error reach the test as itself: Orleans serializes only exceptions from allowed namespaces, and
    /// without the drivers' a failed store shows up as a missing codec instead of its error.
    /// </summary>
    internal static void SerializeDriverExceptions(ExceptionSerializationOptions options)
    {
        // MySql.Data and MySqlConnector.
        options.SupportedNamespacePrefixes.Add("MySql");
        options.SupportedNamespacePrefixes.Add("Npgsql");
    }

    public async ValueTask DisposeAsync()
    {
        if (cluster is not null)
        {
            await cluster.DisposeAsync();
        }

        await Database.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}

[GenerateSerializer]
public sealed class TestState : IEquatable<TestState>
{
    [Id(0)]
    public int State { get; set; }

    [Id(1)]
    public string? Payload { get; set; }

    public bool Equals(TestState? other) => other is not null && State == other.State && Payload == other.Payload;

    public override bool Equals(object? obj) => Equals(obj as TestState);

    public override int GetHashCode() => HashCode.Combine(State, Payload);
}
