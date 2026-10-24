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

    /// <summary>
    /// Stored rows are found by this hash, so the encoding of a key can never change.
    /// </summary>
    [Fact]
    public void Create_ProducesTheStoredHash()
    {
        var key = GrainStateKey.Create("service", "state", GrainId.Create("type", "key"));

        Assert.Equal("4E3176227B6CAEC7B06762119346039E", Convert.ToHexString(key.Hash));
    }

    [Fact]
    public void Create_WithUnpairedSurrogate_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => GrainStateKey.Create("service", "state\uD800", GrainId.Create("type", "key")));
    }
}

public sealed class SchemaScriptTests
{
    public static TheoryData<string> Engines => [.. Scarlet.Orleans.Relational.RelationalEngines.Names];

    [Theory]
    [MemberData(nameof(Engines))]
    public void EveryEngine_ShipsOneScriptPerSchemaVersion(string engine)
    {
        var scripts = Scarlet.Orleans.Relational.Schema.SchemaScript.Load(typeof(PersistenceSchema).Assembly, Scarlet.Orleans.Relational.RelationalEngines.Parse(engine));

        Assert.Equal(Enumerable.Range(1, PersistenceSchema.LatestVersion), scripts.Select(script => script.Version));
    }

    [Theory]
    [MemberData(nameof(Engines))]
    public void EveryScript_CreatesTheLedgerExactlyAsTheRunnerDoes(string engine)
    {
        var parsed = Scarlet.Orleans.Relational.RelationalEngines.Parse(engine);
        var ledger = Scarlet.Orleans.Relational.Schema.SchemaLedgerSql.CreateTable(parsed).ReplaceLineEndings("\n").Trim();

        Assert.All(
            Scarlet.Orleans.Relational.Schema.SchemaScript.Load(typeof(PersistenceSchema).Assembly, parsed),
            script => Assert.Contains(ledger, script.Text.ReplaceLineEndings("\n"), StringComparison.Ordinal));
    }
}
