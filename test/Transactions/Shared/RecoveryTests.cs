using System.Collections.Concurrent;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;
using Orleans.Transactions.TestKit;
using Orleans.Transactions.TestKit.xUnit;

namespace Scarlet.Orleans.Transactions.Tests;

/// <summary>
/// Orleans' recovery runner: transactions in flight while a silo of four shuts down or is killed, and at each recovery
/// phase, must all resolve, with the storage under test. Each test deploys its own cluster, joined through the ADO.NET
/// clustering provider on the same database, because the test cluster's own membership depends on a primary silo the
/// runner may kill. Each test runs a cluster of four silos for up to a minute and a half, so like upstream's long-running
/// suites the runner runs only when <c>SCARLET_TEST_RECOVERY</c> is <c>1</c>.
/// </summary>
public abstract class RecoveryTests(DatabaseFixture fixture, ITestOutputHelper output) : IAsyncLifetime
{
    private const string DatabaseKey = "ScarletTestDatabase";

    private static readonly ConcurrentDictionary<string, TestDatabase> Databases = new();

    private readonly string databaseId = Guid.NewGuid().ToString("N");

    private TestCluster? cluster;

    private static bool OptedIn => Environment.GetEnvironmentVariable("SCARLET_TEST_RECOVERY") == "1";

    private TransactionRecoveryTestsRunnerxUnit Runner
    {
        get
        {
            Assert.SkipUnless(OptedIn, "Set SCARLET_TEST_RECOVERY=1 to run the recovery runner.");
            fixture.Require();
            return new TransactionRecoveryTestsRunnerxUnit(cluster!, output);
        }
    }

    public async ValueTask InitializeAsync()
    {
        if (!OptedIn || !fixture.IsAvailable)
        {
            return;
        }

        Databases[databaseId] = fixture.Database;
        var builder = new TestClusterBuilder(initialSilosCount: 4);
        builder.Options.UseTestClusterMembership = false;
        builder.Properties[DatabaseKey] = databaseId;
        builder.AddSiloBuilderConfigurator<Configurator>();
        builder.AddClientBuilderConfigurator<Configurator>();
        cluster = builder.Build();
        await cluster.DeployAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (cluster is not null)
        {
            await cluster.StopAllSilosAsync();
            await cluster.DisposeAsync();
        }

        Databases.TryRemove(databaseId, out _);
        GC.SuppressFinalize(this);
    }

    [Theory]
    [InlineData(TransactionTestConstants.SingleStateTransactionalGrain, 30)]
    [InlineData(TransactionTestConstants.DoubleStateTransactionalGrain, 20)]
    public Task TransactionWillRecoverAfterRandomSiloGracefulShutdown(string grainStates, int concurrent) =>
        Runner.TransactionWillRecoverAfterRandomSiloGracefulShutdown(grainStates, concurrent, TestContext.Current.CancellationToken);

    [Theory]
    [InlineData(TransactionTestConstants.SingleStateTransactionalGrain, 30)]
    [InlineData(TransactionTestConstants.DoubleStateTransactionalGrain, 20)]
    public Task TransactionWillRecoverAfterRandomSiloUnGracefulShutdown(string grainStates, int concurrent) =>
        Runner.TransactionWillRecoverAfterRandomSiloUnGracefulShutdown(grainStates, concurrent, TestContext.Current.CancellationToken);

    [Fact]
    public Task TransactionWillRecoverAfterManagerWait() =>
        Runner.TransactionWillRecoverAfterManagerWait(TransactionTestConstants.SingleStateTransactionalGrain);

    [Fact]
    public Task TransactionWillRecoverAfterRemotePreparePersisted() =>
        Runner.TransactionWillRecoverAfterRemotePreparePersisted(TransactionTestConstants.SingleStateTransactionalGrain);

    [Fact]
    public Task TransactionWillRecoverAfterLocalCommitStored() =>
        Runner.TransactionWillRecoverAfterLocalCommitStored(TransactionTestConstants.SingleStateTransactionalGrain);

    /// <summary>
    /// Configures the test cluster's silos and client with the storage under test and clustering, on the database the
    /// test registered under <see cref="DatabaseKey"/>, since the test cluster creates configurators from their types.
    /// </summary>
    public sealed class Configurator : ISiloConfigurator, IClientBuilderConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            var database = Databases[siloBuilder.Configuration[DatabaseKey]!];
            siloBuilder.Services.AddKeyedSingleton<IRemoteCommitService, RemoteCommitService>(TransactionTestConstants.RemoteCommitService);
            FlavourTransactions.AddStorage(siloBuilder, TransactionTestConstants.TransactionStore, database);
            siloBuilder.UseTransactions();
            siloBuilder.UseScarletAdoNetClustering(options =>
            {
                options.DataSource = database.DataSource;
                options.Engine = database.Engine == "MariaDb" ? "MariaDb" : null;
                options.SchemaMode = Clustering.AdoNet.SchemaMode.Apply;
            });
        }

        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
        {
            var database = Databases[configuration[DatabaseKey]!];
            clientBuilder.UseTransactions();
            clientBuilder.UseScarletAdoNetClustering(options =>
            {
                options.DataSource = database.DataSource;
                options.Engine = database.Engine == "MariaDb" ? "MariaDb" : null;
            });
        }
    }
}
