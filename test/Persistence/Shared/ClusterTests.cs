using Orleans;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Scarlet.Orleans.Persistence.Tests;

public interface ICounterGrain : IGrainWithStringKey
{
    Task<int> GetAsync();

    Task SetAsync(int value);

    Task ClearAsync();
}

[GenerateSerializer]
public sealed class CounterState
{
    [Id(0)]
    public int Value { get; set; }
}

public sealed class CounterGrain([PersistentState("counter", ClusterTests.StorageName)] IPersistentState<CounterState> state) : Grain, ICounterGrain
{
    public Task<int> GetAsync() => Task.FromResult(state.State.Value);

    public Task SetAsync(int value)
    {
        state.State.Value = value;
        return state.WriteStateAsync();
    }

    public Task ClearAsync() => state.ClearStateAsync();
}

/// <summary>
/// Grain state through a real silo, configured with the package's public hosting extensions and default serializer.
/// </summary>
public sealed class ClusterTests : IAsyncLifetime
{
    public const string StorageName = "store";

    private readonly SqliteDatabaseFixture fixture = new();
    private InProcessTestCluster? cluster;

    public async ValueTask InitializeAsync()
    {
        await fixture.InitializeAsync();
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureSilo((_, silo) => FlavourStorage.AddStorage(silo, StorageName, fixture.Database));
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
    }

    [Fact]
    public async Task State_SurvivesDeactivation()
    {
        var grain = cluster!.Client.GetGrain<ICounterGrain>("survives");
        await grain.SetAsync(5);

        await cluster.DeactivateAsync(grain);

        Assert.Equal(5, await grain.GetAsync());
    }

    [Fact]
    public async Task ClearedState_IsEmptyAfterDeactivation()
    {
        var grain = cluster!.Client.GetGrain<ICounterGrain>("cleared");
        await grain.SetAsync(5);
        await grain.ClearAsync();

        await cluster.DeactivateAsync(grain);

        Assert.Equal(0, await grain.GetAsync());
        await grain.SetAsync(6);
        await cluster.DeactivateAsync(grain);
        Assert.Equal(6, await grain.GetAsync());
    }
}
