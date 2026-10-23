using Orleans.Runtime;

namespace Scarlet.Orleans.Persistence.Tests;

public sealed class GrainStateKeyTests
{
    [Fact]
    public void Create_WhenPartsMoveBetweenFields_ProducesDistinctKeys()
    {
        var first = GrainStateKey.Create("ab", "c", GrainId.Create("type", "key"));
        var second = GrainStateKey.Create("a", "bc", GrainId.Create("type", "key"));

        Assert.NotEqual(first.Identity, second.Identity);
        Assert.NotEqual(first.Hash, second.Hash);
    }

    [Fact]
    public void Create_IsDeterministic()
    {
        var grainId = GrainId.Create("type", "key");

        Assert.Equal(GrainStateKey.Create("service", "state", grainId).Hash, GrainStateKey.Create("service", "state", grainId).Hash);
        Assert.Equal(16, GrainStateKey.Create("service", "state", grainId).Hash.Length);
    }

    [Fact]
    public void Create_WithUnpairedSurrogate_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => GrainStateKey.Create("service", "state\uD800", GrainId.Create("type", "key")));
    }
}
