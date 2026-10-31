using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
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
        builder.ConfigureSilo((_, silo) =>
        {
            silo.Services.AddKeyedSingleton<IRemoteCommitService, RemoteCommitService>(TransactionTestConstants.RemoteCommitService);
            FlavourTransactions.AddStorage(silo, TransactionTestConstants.TransactionStore, Database.Database);
            silo.UseTransactions();
            if (skewedClock)
            {
                new SkewedClockConfigurator().Configure(silo);
            }
        });
        builder.ConfigureClient(client => client.UseTransactions());
        cluster = builder.Build();
        await cluster.DeployAsync();
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
