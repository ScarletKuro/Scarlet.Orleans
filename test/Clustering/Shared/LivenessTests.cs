using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Scarlet.Orleans.Clustering.Tests;

public interface ILivenessGrain : IGrainWithIntegerKey
{
    Task<string> GetLabelAsync();

    Task StartTimerAsync();
}

public sealed class LivenessGrain : Grain, ILivenessGrain
{
    private IGrainTimer? timer;

    public Task<string> GetLabelAsync() => Task.FromResult(this.GetPrimaryKeyLong().ToString(CultureInfo.InvariantCulture));

    public Task StartTimerAsync()
    {
        timer ??= this.RegisterGrainTimer(static () => Task.CompletedTask, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        return Task.CompletedTask;
    }
}

/// <summary>
/// Orleans' liveness scenarios (LivenessTestsBase in dotnet/orleans), over this provider: silos stopped, restarted on
/// their ports with a new generation, and killed without notice while grains run timers, with the cluster detecting
/// it through the table and serving the same and new grains afterwards. Each test deploys its own cluster.
/// </summary>
public abstract class LivenessTests(DatabaseFixture fixture) : IAsyncLifetime
{
    private const string DatabaseKey = "ScarletTestDatabase";

    private const int GrainCount = 20;

    private static readonly ConcurrentDictionary<string, TestDatabase> Databases = new();

    private readonly string databaseId = Guid.NewGuid().ToString("N");

    private TestCluster? cluster;

    private TestCluster Cluster => cluster!;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        if (!fixture.IsAvailable)
        {
            return;
        }

        Databases[databaseId] = fixture.Database;
        var builder = new TestClusterBuilder(initialSilosCount: 2);
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

    [Fact]
    public async Task StoppedSilo_LeavesTheOthersActive()
    {
        fixture.Require();
        var third = await Cluster.StartAdditionalSiloAsync();
        await Cluster.WaitForLivenessToStabilizeAsync();
        var management = Cluster.GrainFactory.GetGrain<IManagementGrain>(0);

        var hosts = await management.GetHosts(onlyActive: false, CancellationToken);
        Assert.Equal(3, hosts.Count);
        Assert.All(hosts.Values, status => Assert.Equal(SiloStatus.Active, status));

        await Cluster.StopSiloAsync(third, CancellationToken);
        await Cluster.WaitForLivenessToStabilizeAsync();

        foreach (var (silo, status) in await management.GetHosts(onlyActive: false, CancellationToken))
        {
            if (silo.Endpoint.Equals(third.SiloAddress.Endpoint))
            {
                Assert.Contains(status, new[] { SiloStatus.ShuttingDown, SiloStatus.Stopping, SiloStatus.Dead });
            }
            else
            {
                Assert.Equal(SiloStatus.Active, status);
            }
        }
    }

    [Fact]
    public Task RestartedFirstSilo_KeepsTheClusterServing() => RestartOrKillAsync(0, restart: true);

    [Fact]
    public Task RestartedGateway_KeepsTheClusterServing() => RestartOrKillAsync(1, restart: true);

    [Fact]
    public Task RestartedAdditionalSilo_KeepsTheClusterServing() => RestartOrKillAsync(2, restart: true);

    [Fact]
    public Task KilledSiloRunningTimers_IsDeclaredDead_AndTheClusterKeepsServing() => RestartOrKillAsync(2, restart: false, startTimers: true);

    private async Task RestartOrKillAsync(int index, bool restart, bool startTimers = false)
    {
        fixture.Require();
        await Cluster.StartAdditionalSilosAsync(1);
        await Cluster.WaitForLivenessToStabilizeAsync();
        await SendTrafficAsync(1, GrainCount, startTimers);

        var silo = Cluster.Silos[index];
        if (restart)
        {
            await Cluster.RestartSiloAsync(silo);
        }
        else
        {
            await Cluster.KillSiloAsync(silo);
        }

        await Cluster.WaitForLivenessToStabilizeAsync(didKill: !restart);

        var table = FlavourMembership.CreateTable(fixture.Database, Cluster.Options.ClusterId, TestSchemaMode.None);
        await table.InitializeMembershipTableAsync(false, CancellationToken);
        var entry = (await table.ReadRowAsync(silo.SiloAddress, CancellationToken)).Members.Single().Item1;
        Assert.Equal(SiloStatus.Dead, entry.Status);
        if (!restart)
        {
            // A killed silo cannot write its own status: the survivors declared it dead with their votes.
            Assert.NotNull(entry.SuspectTimes);
            Assert.NotEmpty(entry.SuspectTimes);
        }

        await SendTrafficAsync(1, 2 * GrainCount);
    }

    private async Task SendTrafficAsync(int first, int last, bool startTimers = false)
    {
        for (var key = first; key <= last; key++)
        {
            var grain = Cluster.GrainFactory.GetGrain<ILivenessGrain>(key);
            Assert.Equal(key.ToString(CultureInfo.InvariantCulture), await grain.GetLabelAsync());
            if (startTimers)
            {
                await grain.StartTimerAsync();
            }
        }
    }

    /// <summary>
    /// Configures the test cluster's silos and client with the flavour's clustering, on the database the test registered
    /// under <see cref="DatabaseKey"/>, since the test cluster creates configurators from their types.
    /// </summary>
    public sealed class Configurator : ISiloConfigurator, IClientBuilderConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder) =>
            FlavourMembership.UseClustering(siloBuilder, Databases[siloBuilder.Configuration[DatabaseKey]!]);

        public void Configure(IConfiguration configuration, IClientBuilder clientBuilder) =>
            FlavourMembership.UseClustering(clientBuilder, Databases[configuration[DatabaseKey]!]);
    }
}
