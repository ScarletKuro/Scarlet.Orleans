using System.Data;
using System.Diagnostics;
using Orleans;
using Orleans.GrainDirectory;
using Orleans.Placement;
using Orleans.TestingHost;

namespace Scarlet.Orleans.GrainDirectory.Tests;

public interface IFirstDirectoryGrain : IGrainWithStringKey
{
    Task<string> GetSiloAsync();
}

public interface ISecondDirectoryGrain : IGrainWithStringKey
{
    Task<string> GetSiloAsync();
}

// Random placement spreads activations over both silos, so stopping one leaves entries to clean up.
[RandomPlacement]
[GrainDirectory(GrainDirectoryName = GrainDirectoryClusterTests.FirstDirectory)]
public sealed class FirstDirectoryGrain : Grain, IFirstDirectoryGrain
{
    public Task<string> GetSiloAsync() => Task.FromResult(GrainContext.Address.SiloAddress!.ToParsableString());
}

[GrainDirectory(GrainDirectoryName = GrainDirectoryClusterTests.SecondDirectory)]
public sealed class SecondDirectoryGrain : Grain, ISecondDirectoryGrain
{
    public Task<string> GetSiloAsync() => Task.FromResult(GrainContext.Address.SiloAddress!.ToParsableString());
}

/// <summary>
/// Silos registering their activations through the package's public hosting extensions, in two directories chosen by
/// <see cref="GrainDirectoryAttribute"/>. Each test deploys its own cluster on a fresh fixture.
/// </summary>
/// <remarks>
/// The directories are named because the in-process test cluster registers its own default directory after the silo's
/// configuration, which would replace one registered as the default. A regular host has no such directory.
/// </remarks>
public abstract class GrainDirectoryClusterTests(DatabaseFixture fixture) : IAsyncLifetime
{
    public const string FirstDirectory = "first";

    public const string SecondDirectory = "second";

    private InProcessTestCluster? cluster;

    private IClusterClient Client
    {
        get
        {
            fixture.Require();
            return cluster!.Client;
        }
    }

    public async ValueTask InitializeAsync()
    {
        await fixture.InitializeAsync();
        if (!fixture.IsAvailable)
        {
            return;
        }

        var builder = new InProcessTestClusterBuilder(2);
        builder.ConfigureSilo((_, silo) =>
        {
            FlavourDirectory.AddDirectory(silo, FirstDirectory, fixture.Database);
            FlavourDirectory.AddDirectory(silo, SecondDirectory, fixture.Database);
        });
        cluster = builder.Build();
        await cluster.DeployAsync(TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (cluster is not null)
        {
            await cluster.DisposeAsync();
        }

        await fixture.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task Activations_AreRegisteredInTheirDirectory()
    {
        var key = $"{Guid.NewGuid():N}";
        var firstSilo = await Client.GetGrain<IFirstDirectoryGrain>(key).GetSiloAsync();
        var secondSilo = await Client.GetGrain<ISecondDirectoryGrain>(key).GetSiloAsync();

        Assert.Equal([(FirstDirectory, firstSilo)], await EntriesAsync($"firstdirectory/{key}"));
        Assert.Equal([(SecondDirectory, secondSilo)], await EntriesAsync($"seconddirectory/{key}"));
    }

    [Fact]
    public async Task Deactivation_RemovesTheEntry()
    {
        var grain = Client.GetGrain<IFirstDirectoryGrain>($"{Guid.NewGuid():N}");
        await grain.GetSiloAsync();
        var grainId = grain.GetGrainId().ToString();

        await cluster!.DeactivateAsync(grain);

        await UntilAsync(async () => (await EntriesAsync(grainId)).Count == 0, "The deactivated grain kept its entry.");
    }

    [Fact]
    public async Task EveryCall_ReachesTheOneRegisteredActivation()
    {
        var grains = Enumerable.Range(0, 20).Select(_ => Client.GetGrain<IFirstDirectoryGrain>($"{Guid.NewGuid():N}")).ToArray();

        var first = await Task.WhenAll(grains.Select(g => g.GetSiloAsync()));
        var again = await Task.WhenAll(grains.Select(g => g.GetSiloAsync()));

        Assert.Equal(first, again);
        for (var i = 0; i < grains.Length; i++)
        {
            Assert.Equal([(FirstDirectory, first[i])], await EntriesAsync(grains[i].GetGrainId().ToString()));
        }
    }

    [Fact]
    public async Task StoppedSilo_LeavesNoEntries()
    {
        var grains = Enumerable.Range(0, 20).Select(_ => Client.GetGrain<IFirstDirectoryGrain>($"{Guid.NewGuid():N}")).ToArray();
        var silos = await Task.WhenAll(grains.Select(g => g.GetSiloAsync()));
        var stopped = cluster!.Silos[1];
        var stoppedAddress = stopped.SiloAddress.ToParsableString();
        Assert.Contains(stoppedAddress, silos);

        await cluster.StopSiloAsync(stopped, TestContext.Current.CancellationToken);

        await UntilAsync(async () =>
        {
            foreach (var grain in grains)
            {
                if ((await EntriesAsync(grain.GetGrainId().ToString())).Any(e => e.Silo == stoppedAddress))
                {
                    return false;
                }
            }

            return true;
        }, "Entries still point at the stopped silo.");
        Assert.All(await Task.WhenAll(grains.Select(g => g.GetSiloAsync())), silo => Assert.NotEqual(stoppedAddress, silo));
    }

    private async Task<List<(string Directory, string Silo)>> EntriesAsync(string grainId)
    {
        await using var command = fixture.Database.DataSource.CreateCommand(
            $"SELECT directory_name, silo_address FROM {GrainDirectorySchema.TableName} WHERE grain_id = @grain_id");
        var parameter = command.CreateParameter();
        parameter.ParameterName = "grain_id";
        parameter.Value = grainId;
        parameter.DbType = DbType.String;
        command.Parameters.Add(parameter);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var entries = new List<(string, string)>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            entries.Add((reader.GetString(0), reader.GetString(1)));
        }

        return entries;
    }

    private static async Task UntilAsync(Func<Task<bool>> condition, string failure)
    {
        var deadline = Stopwatch.StartNew();
        while (!await condition())
        {
            Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(30), failure);
            await Task.Delay(200, TestContext.Current.CancellationToken);
        }
    }
}
