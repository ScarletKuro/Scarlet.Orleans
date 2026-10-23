using Orleans;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Scarlet.Orleans.Persistence.Tests;

public interface ICounterGrain : IGrain
{
    Task<int> GetAsync();

    Task SetAsync(int value);

    Task ClearAsync();
}

public interface IStringCounterGrain : ICounterGrain, IGrainWithStringKey;

public interface IGuidCounterGrain : ICounterGrain, IGrainWithGuidKey;

public interface IIntegerCounterGrain : ICounterGrain, IGrainWithIntegerKey;

public interface IGuidCompoundCounterGrain : ICounterGrain, IGrainWithGuidCompoundKey;

public interface IIntegerCompoundCounterGrain : ICounterGrain, IGrainWithIntegerCompoundKey;

public interface IValueGrain<T> : IGrainWithIntegerKey
{
    Task<T?> GetAsync();

    Task SetAsync(T value);
}

[GenerateSerializer]
public sealed class CounterState
{
    [Id(0)]
    public int Value { get; set; }
}

[GenerateSerializer]
public sealed class ValueState<T>
{
    [Id(0)]
    public T? Value { get; set; }
}

public abstract class CounterGrain(IPersistentState<CounterState> state) : Grain, ICounterGrain
{
    public Task<int> GetAsync() => Task.FromResult(state.State.Value);

    public Task SetAsync(int value)
    {
        state.State.Value = value;
        return state.WriteStateAsync();
    }

    public Task ClearAsync() => state.ClearStateAsync();
}

public sealed class StringCounterGrain([PersistentState("counter", ClusterTests.StorageName)] IPersistentState<CounterState> state) : CounterGrain(state), IStringCounterGrain;

public sealed class GuidCounterGrain([PersistentState("counter", ClusterTests.StorageName)] IPersistentState<CounterState> state) : CounterGrain(state), IGuidCounterGrain;

public sealed class IntegerCounterGrain([PersistentState("counter", ClusterTests.StorageName)] IPersistentState<CounterState> state) : CounterGrain(state), IIntegerCounterGrain;

public sealed class GuidCompoundCounterGrain([PersistentState("counter", ClusterTests.StorageName)] IPersistentState<CounterState> state) : CounterGrain(state), IGuidCompoundCounterGrain;

public sealed class IntegerCompoundCounterGrain([PersistentState("counter", ClusterTests.StorageName)] IPersistentState<CounterState> state) : CounterGrain(state), IIntegerCompoundCounterGrain;

public sealed class ValueGrain<T>([PersistentState("value", ClusterTests.StorageName)] IPersistentState<ValueState<T>> state) : Grain, IValueGrain<T>
{
    public Task<T?> GetAsync() => Task.FromResult(state.State.Value);

    public Task SetAsync(T value)
    {
        state.State.Value = value;
        return state.WriteStateAsync();
    }
}

/// <summary>
/// Grain state through a real silo, configured with the package's public hosting extensions and default serializer.
/// </summary>
/// <remarks>Covers the scenarios of Orleans' <c>GrainPersistenceTestsRunner</c>.</remarks>
public sealed class ClusterTests : IAsyncLifetime
{
    public const string StorageName = "store";

    private readonly SqliteDatabaseFixture fixture = new();
    private InProcessTestCluster? cluster;

    private IClusterClient Client => cluster!.Client;

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

    public static TheoryData<string> KeyKinds => ["String", "Guid", "Integer", "GuidCompound", "IntegerCompound"];

    [Theory]
    [MemberData(nameof(KeyKinds))]
    public async Task State_SurvivesDeactivation_ForEveryKeyKind(string keyKind)
    {
        var grain = Counter(keyKind, Random.Shared.Next(), "extension");
        await grain.SetAsync(5);

        await cluster!.DeactivateAsync(grain);

        Assert.Equal(5, await grain.GetAsync());
    }

    [Theory]
    [MemberData(nameof(KeyKinds))]
    public async Task KeysDifferingOnlyInTheirLastPart_AreIndependent(string keyKind)
    {
        var key = Random.Shared.Next();
        var first = Counter(keyKind, key, "a");
        var second = Counter(keyKind, key, "b");
        await first.SetAsync(1);

        await second.SetAsync(2);
        await cluster!.DeactivateAsync(first);
        await cluster.DeactivateAsync(second);

        Assert.Equal(1, await first.GetAsync());
        Assert.Equal(2, await second.GetAsync());
    }

    [Fact]
    public async Task GenericGrains_WithTheSameKey_AreIsolatedByTypeArgument()
    {
        var key = Random.Shared.NextInt64();
        var integer = Client.GetGrain<IValueGrain<int>>(key);
        var text = Client.GetGrain<IValueGrain<string>>(key);
        var nested = Client.GetGrain<IValueGrain<List<int>>>(key);
        await integer.SetAsync(7);
        await text.SetAsync("seven");
        await nested.SetAsync([7, 8]);

        await cluster!.DeactivateAsync(integer);
        await cluster.DeactivateAsync(text);
        await cluster.DeactivateAsync(nested);

        Assert.Equal(7, await integer.GetAsync());
        Assert.Equal("seven", await text.GetAsync());
        Assert.Equal([7, 8], await nested.GetAsync());
    }

    [Fact]
    public async Task ClearedState_IsEmptyAfterDeactivation()
    {
        var grain = Client.GetGrain<IStringCounterGrain>("cleared");
        await grain.SetAsync(5);
        await grain.ClearAsync();

        await cluster!.DeactivateAsync(grain);

        Assert.Equal(0, await grain.GetAsync());
        await grain.SetAsync(6);
        await cluster.DeactivateAsync(grain);
        Assert.Equal(6, await grain.GetAsync());
    }

    [Fact]
    public async Task State_SurvivesASiloRestart()
    {
        var keptKey = Guid.NewGuid();
        var clearedKey = Guid.NewGuid();
        await Client.GetGrain<IGuidCounterGrain>(keptKey).SetAsync(1);
        var cleared = Client.GetGrain<IGuidCounterGrain>(clearedKey);
        await cleared.SetAsync(1);
        await cleared.ClearAsync();

        await cluster!.RestartSiloAsync(cluster.Silos[0]);
        await cluster.InitializeClientAsync(TestContext.Current.CancellationToken);

        var kept = Client.GetGrain<IGuidCounterGrain>(keptKey);
        Assert.Equal(1, await kept.GetAsync());
        Assert.Equal(0, await Client.GetGrain<IGuidCounterGrain>(clearedKey).GetAsync());
        await kept.SetAsync(2);
        Assert.Equal(2, await kept.GetAsync());
    }

    private ICounterGrain Counter(string keyKind, int key, string last) => keyKind switch
    {
        "String" => Client.GetGrain<IStringCounterGrain>($"{key}-{last}"),
        "Guid" => Client.GetGrain<IGuidCounterGrain>(new Guid(key, (short)last[0], 0, new byte[8])),
        "Integer" => Client.GetGrain<IIntegerCounterGrain>(((long)key << 8) | last[0]),
        "GuidCompound" => Client.GetGrain<IGuidCompoundCounterGrain>(new Guid(key, 0, 0, new byte[8]), last),
        "IntegerCompound" => Client.GetGrain<IIntegerCompoundCounterGrain>(key, last),
        _ => throw new ArgumentOutOfRangeException(nameof(keyKind)),
    };
}
