using System.Diagnostics;
using Orleans.Runtime;

namespace Scarlet.Orleans.GrainDirectory.Tests;

/// <summary>
/// Grain registrations through real connection failures: sessions killed by the database, as in a failover, and
/// responses lost on the way back, as in a network partition after the server has committed.
/// </summary>
/// <remarks>
/// Each writer moves its own grain to a new activation again and again, replacing the entry it last saw. A failed call
/// has an unknown outcome; the next registration finds out what is stored, as a silo does. Afterwards each grain must hold
/// an entry its writer registered, and must hold one if any registration was acknowledged.
/// </remarks>
public abstract class GrainDirectoryConnectionFailureTests(DatabaseFixture fixture)
{
    private const int Writers = 8;
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(4);
    private static readonly SiloAddress Silo = SiloAddress.FromParsableString("10.0.23.12:1000@5678");

    [Fact]
    public async Task Registrations_WhileSessionsAreKilled_KeepOneRegisteredEntry()
    {
        var database = fixture.Database;
        var directory = FlavourDirectory.Create(database, "default", $"cluster-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await directory.InitializeAsync(TestContext.Current.CancellationToken);
        using var stop = new CancellationTokenSource(Duration);

        var killer = ConnectionFaults.KillSessionsAsync(database, stop.Token);
        var results = await Task.WhenAll(Enumerable.Range(0, Writers).Select(i => MoveAsync(directory, GrainId.Create("failure-grain", $"{Guid.NewGuid():N}-{i}"), stop.Token)));
        var kills = await killer;

        Assert.True(kills > 0, "The test database had no sessions to kill.");
        Assert.True(results.Sum(r => r.Failures) > 0, $"Killing {kills} sessions failed no registration, so this test exercised nothing.");
        await VerifyAsync(directory, results);
    }

    [Fact]
    public async Task Registrations_WhoseResponsesAreLost_KeepOneRegisteredEntry()
    {
        var (host, port, connect) = ConnectionFaults.Route(fixture.Database);
        await using var proxy = new ChaosProxy(host, port);
        var (connectionString, dataSource) = connect(proxy.Port);
        await using var database = new TestDatabase(fixture.Database.Engine, fixture.Database.Driver, connectionString, dataSource, () => Task.CompletedTask);
        var directory = FlavourDirectory.Create(database, "default", $"cluster-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await directory.InitializeAsync(TestContext.Current.CancellationToken);
        using var stop = new CancellationTokenSource(Duration);

        proxy.DropProbability = 0.05;
        var results = await Task.WhenAll(Enumerable.Range(0, Writers).Select(i => MoveAsync(directory, GrainId.Create("failure-grain", $"{Guid.NewGuid():N}-{i}"), stop.Token)));
        proxy.DropProbability = 0;

        Assert.True(proxy.Drops > 0 && results.Sum(r => r.Failures) > 0, $"{proxy.Drops} dropped responses failed no registration, so this test exercised nothing.");
        await VerifyAsync(directory, results);
    }

    private static async Task VerifyAsync(RelationalGrainDirectory directory, WriterResult[] results)
    {
        Assert.True(results.Sum(r => r.Acknowledged) > 0, "No registration succeeded.");
        foreach (var result in results)
        {
            var stored = await UntilAvailableAsync(() => directory.Lookup(result.GrainId));
            if (stored is null)
            {
                Assert.Equal(0, result.Acknowledged);
            }
            else
            {
                Assert.Contains(stored, result.Attempted);
            }

            var final = Address(result.GrainId);
            Assert.Equal(final, await UntilAvailableAsync(() => directory.Register(final, stored)));
            Assert.Equal(final, await UntilAvailableAsync(() => directory.Lookup(result.GrainId)));
        }
    }

    private static async Task<WriterResult> MoveAsync(RelationalGrainDirectory directory, GrainId grainId, CancellationToken stop)
    {
        var result = new WriterResult(grainId);
        GrainAddress? current = null;
        while (!stop.IsCancellationRequested)
        {
            var next = Address(grainId);
            result.Attempted.Add(next);
            try
            {
                current = await directory.Register(next, current, CancellationToken.None);
                result.Acknowledged++;
            }
            catch (Exception exception) when (exception is not GrainDirectoryKeyCollisionException)
            {
                // Some drivers fail every open at once for a while after a broken connection; spinning through those
                // would only starve the tests running alongside.
                result.Failures++;
                await Task.Delay(10, CancellationToken.None);
            }
        }

        return result;
    }

    /// <summary>
    /// Runs an operation as a silo would once the database is reachable again, retrying while pooled connections broken
    /// by the failures are discarded.
    /// </summary>
    private static async Task<T> UntilAvailableAsync<T>(Func<Task<T>> operation)
    {
        var deadline = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                return await operation();
            }
            catch (Exception exception) when (exception is not GrainDirectoryKeyCollisionException && deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                await Task.Delay(50);
            }
        }
    }

    private static GrainAddress Address(GrainId grainId) => new()
    {
        ActivationId = ActivationId.NewId(),
        GrainId = grainId,
        SiloAddress = Silo,
        MembershipVersion = new MembershipVersion(3),
    };

    private sealed class WriterResult(GrainId grainId)
    {
        public GrainId GrainId { get; } = grainId;

        public List<GrainAddress> Attempted { get; } = [];

        public int Acknowledged { get; set; }

        public int Failures { get; set; }
    }
}
