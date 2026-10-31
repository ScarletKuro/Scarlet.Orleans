using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;

namespace Scarlet.Orleans.Clustering.Tests;

public interface ISiloNameGrain : IGrainWithGuidKey
{
    Task<string> GetSiloAsync();
}

public sealed class SiloNameGrain : Grain, ISiloNameGrain
{
    public Task<string> GetSiloAsync() => Task.FromResult(GrainContext.Address.SiloAddress!.ToParsableString());
}

/// <summary>
/// Real silo and client hosts, wired only through the package's public hosting extensions, finding each other through
/// the database over loopback TCP. Each test deploys its own cluster.
/// </summary>
/// <remarks>
/// The in-process test cluster always installs its own membership table, so these tests build hosts the way an
/// application does.
/// </remarks>
public abstract class ClusteringHostTests(DatabaseFixture fixture) : IAsyncLifetime
{
    private readonly string clusterId = $"hosts-{Guid.NewGuid():N}";
    private readonly List<IHost> silos = [];
    private IHost? client;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    private IClusterClient Client => client!.Services.GetRequiredService<IClusterClient>();

    public async ValueTask InitializeAsync()
    {
        await fixture.InitializeAsync();
        if (!fixture.IsAvailable)
        {
            return;
        }

        silos.Add(await StartSiloAsync());
        silos.Add(await StartSiloAsync());

        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.UseOrleansClient(orleans =>
        {
            orleans.Configure<ClusterOptions>(ConfigureCluster);
            FlavourMembership.UseClustering(orleans, fixture.Database);
        });
        client = builder.Build();
        await client.StartAsync(CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var host in silos.Prepend(client).OfType<IHost>())
        {
            await host.StopAsync(CancellationToken.None);
            host.Dispose();
        }

        await fixture.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task Silos_JoinThroughTheDatabase_AndTheClientReachesBoth()
    {
        fixture.Require();
        var addresses = silos.Select(SiloAddressOf).ToHashSet();

        var members = await ReadMembersAsync();
        Assert.Equal(addresses, members.Where(m => m.Status == SiloStatus.Active).Select(m => m.SiloAddress.ToParsableString()).ToHashSet());

        var gateways = FlavourMembership.CreateGatewayListProvider(fixture.Database, clusterId);
        await gateways.InitializeGatewayListProvider();
        Assert.Equal(2, (await gateways.GetGateways()).Count);

        var reached = new HashSet<string>();
        await UntilAsync(async () =>
        {
            reached.Add(await Client.GetGrain<ISiloNameGrain>(Guid.NewGuid()).GetSiloAsync());
            return reached.SetEquals(addresses);
        }, "The client's calls did not reach both silos.");
    }

    [Fact]
    public async Task StoppedSilo_IsRecordedDead_AndTheClusterKeepsServing()
    {
        fixture.Require();
        var survivor = SiloAddressOf(silos[0]);
        var stopped = SiloAddressOf(silos[1]);

        await silos[1].StopAsync(CancellationToken);

        await UntilAsync(
            async () => (await ReadMembersAsync()).Single(m => m.SiloAddress.ToParsableString() == stopped).Status == SiloStatus.Dead,
            "The stopped silo is not recorded as dead.");
        await UntilAsync(
            async () => await Client.GetGrain<ISiloNameGrain>(Guid.NewGuid()).GetSiloAsync() == survivor,
            "New activations do not land on the surviving silo.");
    }

    private async Task<IHost> StartSiloAsync()
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.UseOrleans(orleans =>
        {
            orleans.Configure<ClusterOptions>(ConfigureCluster);
            orleans.ConfigureEndpoints(IPAddress.Loopback, FreePort(), FreePort());
            FlavourMembership.UseClustering(orleans, fixture.Database);
        });
        var host = builder.Build();
        await host.StartAsync(CancellationToken);
        return host;
    }

    private void ConfigureCluster(ClusterOptions options)
    {
        options.ClusterId = clusterId;
        options.ServiceId = "scarlet-clustering-hosts";
    }

    private async Task<List<MembershipEntry>> ReadMembersAsync()
    {
        var table = FlavourMembership.CreateTable(fixture.Database, clusterId, TestSchemaMode.None);
        await table.InitializeMembershipTableAsync(false, CancellationToken);
        return [.. (await table.ReadAllAsync(CancellationToken)).Members.Select(m => m.Item1)];
    }

    private static string SiloAddressOf(IHost silo) => silo.Services.GetRequiredService<ILocalSiloDetails>().SiloAddress.ToParsableString();

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private static async Task UntilAsync(Func<Task<bool>> condition, string message)
    {
        var deadline = Stopwatch.StartNew();
        while (!await condition())
        {
            Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(60), message);
            await Task.Delay(TimeSpan.FromMilliseconds(250), CancellationToken);
        }
    }
}
