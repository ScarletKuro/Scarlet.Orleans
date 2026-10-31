using System.Diagnostics;
using System.Net;
using Orleans;
using Orleans.Runtime;

namespace Scarlet.Orleans.Clustering.Tests;

/// <summary>
/// Membership writes through real connection failures: sessions killed by the database, as in a failover, and responses
/// lost on the way back, as in a network partition after the server has committed.
/// </summary>
/// <remarks>
/// Each writer keeps writing its own silo's row with the next table version, recording that version in the row's update
/// zone, and heartbeats in between. A failed call has an unknown outcome; the writer reads the table again, as a silo
/// does. Since every versioned write changes exactly one row, the table is atomic only if afterwards the zones are
/// distinct and the largest is the table version, and every acknowledged write is still there.
/// </remarks>
public abstract class ClusteringConnectionFailureTests(DatabaseFixture fixture)
{
    private const int Writers = 8;
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(4);
    private static readonly DateTime Start = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task Writes_WhileSessionsAreKilled_CommitVersionAndRowTogether()
    {
        var database = fixture.Database;
        var table = await CreateTableAsync(database);
        using var stop = new CancellationTokenSource(Duration);

        var killer = ConnectionFaults.KillSessionsAsync(database, stop.Token);
        var results = await Task.WhenAll(Enumerable.Range(1, Writers).Select(i => WriteAsync(table, i, stop.Token)));
        var kills = await killer;

        Assert.True(kills > 0, "The test database had no sessions to kill.");
        Assert.True(results.Sum(r => r.Failures) > 0, $"Killing {kills} sessions failed no write, so this test exercised nothing.");
        await VerifyAsync(table, results);
    }

    [Fact]
    public async Task Writes_WhoseResponsesAreLost_CommitVersionAndRowTogether()
    {
        var (host, port, connect) = ConnectionFaults.Route(fixture.Database);
        await using var proxy = new ChaosProxy(host, port);
        var (connectionString, dataSource) = connect(proxy.Port);
        await using var database = new TestDatabase(fixture.Database.Engine, fixture.Database.Driver, connectionString, dataSource, () => Task.CompletedTask);
        var table = await CreateTableAsync(database);
        using var stop = new CancellationTokenSource(Duration);

        proxy.DropProbability = 0.05;
        var results = await Task.WhenAll(Enumerable.Range(1, Writers).Select(i => WriteAsync(table, i, stop.Token)));
        proxy.DropProbability = 0;

        Assert.True(proxy.Drops > 0 && results.Sum(r => r.Failures) > 0, $"{proxy.Drops} dropped responses failed no write, so this test exercised nothing.");
        await VerifyAsync(table, results);
    }

    private static async Task<RelationalMembershipTable> CreateTableAsync(TestDatabase database)
    {
        var table = FlavourMembership.CreateTable(database, $"cluster-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await table.InitializeMembershipTableAsync(tryInitTableVersion: true, TestContext.Current.CancellationToken);
        return table;
    }

    private static async Task VerifyAsync(RelationalMembershipTable table, WriterResult[] results)
    {
        Assert.True(results.Sum(r => r.Acknowledged) > 0, "No write succeeded.");
        var data = await UntilAvailableAsync(() => table.ReadAllAsync(CancellationToken.None));
        var zones = data.Members.Select(m => m.Item1.UpdateZone).ToList();
        Assert.Equal(zones.Count, zones.Distinct().Count());
        Assert.Equal(data.Version.Version, zones.Max());

        foreach (var result in results.Where(r => r.Acknowledged > 0))
        {
            var (entry, _) = Assert.Single(data.Members, m => m.Item1.SiloAddress.Equals(Silo(result.Silo)));
            Assert.True(entry.UpdateZone >= result.LastAcknowledgedVersion, $"Silo {result.Silo} lost its write of version {result.LastAcknowledgedVersion}.");
        }

        foreach (var result in results)
        {
            Assert.NotNull(await UntilAvailableAsync(() => TryWriteAsync(table, result.Silo)));
        }
    }

    private static async Task<WriterResult> WriteAsync(RelationalMembershipTable table, int silo, CancellationToken stop)
    {
        var result = new WriterResult(silo);
        while (!stop.IsCancellationRequested)
        {
            try
            {
                if (await TryWriteAsync(table, silo) is { } version)
                {
                    result.Acknowledged++;
                    result.LastAcknowledgedVersion = version;
                    await table.UpdateIAmAliveAsync(Entry(silo, version, Start.AddSeconds(version)), CancellationToken.None);
                }
            }
            catch (Exception exception) when (exception is not MembershipKeyCollisionException)
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
    /// Writes the silo's row with the next version, as a silo does after reading the table. Returns the version written,
    /// or <see langword="null"/> if another write came first.
    /// </summary>
    private static async Task<int?> TryWriteAsync(RelationalMembershipTable table, int silo)
    {
        var data = await table.ReadAllAsync(CancellationToken.None);
        var next = data.Version.Next();
        var entry = Entry(silo, next.Version, Start);
        var own = data.Members.FirstOrDefault(m => m.Item1.SiloAddress.Equals(entry.SiloAddress));
        var written = own is null
            ? await table.InsertRowAsync(entry, next, CancellationToken.None)
            : await table.UpdateRowAsync(entry, own.Item2, next, CancellationToken.None);
        return written ? next.Version : null;
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
            catch (Exception exception) when (exception is not MembershipKeyCollisionException && deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                await Task.Delay(50);
            }
        }
    }

    private static SiloAddress Silo(int silo) => SiloAddress.New(IPAddress.Loopback, 11000 + silo, 1000 + silo);

    private static MembershipEntry Entry(int silo, int version, DateTime iAmAlive) => new()
    {
        SiloAddress = Silo(silo),
        Status = SiloStatus.Active,
        ProxyPort = 30000,
        HostName = "host",
        SiloName = $"silo-{silo}",
        RoleName = "role",
        UpdateZone = version,
        StartTime = Start,
        IAmAliveTime = iAmAlive,
    };

    private sealed class WriterResult(int silo)
    {
        public int Silo { get; } = silo;

        public int Acknowledged { get; set; }

        public int LastAcknowledgedVersion { get; set; }

        public int Failures { get; set; }
    }
}
