using System.Diagnostics;
using Orleans;
using Orleans.Runtime;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// Reminder upserts through real connection failures: sessions killed by the database, as in a failover, and responses
/// lost on the way back, as in a network partition after the server has committed.
/// </summary>
/// <remarks>
/// Each writer upserts its own reminder with a start time that grows by a second each time. A failed upsert has an
/// unknown outcome, so afterwards the stored start time must be one that was written, no older than the last
/// acknowledged one, in exactly one row.
/// </remarks>
public abstract class ReminderConnectionFailureTests(DatabaseFixture fixture)
{
    private const string ReminderName = "failure";
    private const int Writers = 8;
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(4);
    private static readonly DateTime BaseTime = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task Upserts_WhileSessionsAreKilled_StayDurableInOneRow()
    {
        var database = fixture.Database;
        var table = FlavourTable.Create(database, $"service-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await table.StartAsync(TestContext.Current.CancellationToken);
        using var stop = new CancellationTokenSource(Duration);

        var killer = ConnectionFaults.KillSessionsAsync(database, stop.Token);
        var results = await Task.WhenAll(Enumerable.Range(0, Writers).Select(i => UpsertAsync(table, GrainId.Create("failure-grain", $"{Guid.NewGuid():N}-{i}"), stop.Token)));
        var kills = await killer;

        Assert.True(kills > 0, "The test database had no sessions to kill.");
        Assert.True(results.Sum(r => r.Failures) > 0, $"Killing {kills} sessions failed no upsert, so this test exercised nothing.");
        await VerifyAsync(table, results);
    }

    [Fact]
    public async Task Upserts_WhoseResponsesAreLost_StayDurableInOneRow()
    {
        var (host, port, connect) = ConnectionFaults.Route(fixture.Database);
        await using var proxy = new ChaosProxy(host, port);
        var (connectionString, dataSource) = connect(proxy.Port);
        await using var database = new TestDatabase(fixture.Database.Engine, fixture.Database.Driver, connectionString, dataSource, () => Task.CompletedTask);
        var table = FlavourTable.Create(database, $"service-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await table.StartAsync(TestContext.Current.CancellationToken);
        using var stop = new CancellationTokenSource(Duration);

        proxy.DropProbability = 0.05;
        var results = await Task.WhenAll(Enumerable.Range(0, Writers).Select(i => UpsertAsync(table, GrainId.Create("failure-grain", $"{Guid.NewGuid():N}-{i}"), stop.Token)));
        proxy.DropProbability = 0;

        Assert.True(proxy.Drops > 0 && results.Sum(r => r.Failures) > 0, $"{proxy.Drops} dropped responses failed no upsert, so this test exercised nothing.");
        await VerifyAsync(table, results);
    }

    private static async Task VerifyAsync(RelationalReminderTable table, WriterResult[] results)
    {
        Assert.True(results.Sum(r => r.Acknowledged) > 0, "No upsert succeeded.");
        foreach (var result in results)
        {
            var stored = (await UntilAvailableAsync(() => table.ReadRows(result.GrainId))).Reminders;
            if (stored.Count == 0)
            {
                // No upsert committed, which is possible only if none was acknowledged.
                Assert.Equal(0, result.LastAcknowledged);
            }
            else
            {
                Assert.InRange(Seconds(Assert.Single(stored).StartAt), result.LastAcknowledged, result.LastAttempted);
            }

            var final = result.LastAttempted + 1;
            var etag = await UntilAvailableAsync(() => table.UpsertRow(Entry(result.GrainId, final)));
            var read = await UntilAvailableAsync(() => table.ReadRow(result.GrainId, ReminderName));
            Assert.Equal(final, Seconds(read!.StartAt));
            Assert.Equal(etag, read.ETag);
        }
    }

    private static async Task<WriterResult> UpsertAsync(RelationalReminderTable table, GrainId grainId, CancellationToken stop)
    {
        var result = new WriterResult(grainId);
        while (!stop.IsCancellationRequested)
        {
            var value = result.LastAttempted + 1;
            result.LastAttempted = value;
            try
            {
                await table.UpsertRow(Entry(grainId, value), CancellationToken.None);
                result.LastAcknowledged = value;
                result.Acknowledged++;
            }
            catch (Exception exception) when (exception is not ReminderKeyCollisionException)
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
    /// Runs an operation as the reminder service would once the database is reachable again, retrying while pooled
    /// connections broken by the failures are discarded.
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
            catch (Exception exception) when (exception is not ReminderKeyCollisionException && deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                await Task.Delay(50);
            }
        }
    }

    private static ReminderEntry Entry(GrainId grainId, int seconds) => new()
    {
        GrainId = grainId,
        ReminderName = ReminderName,
        StartAt = BaseTime.AddSeconds(seconds),
        Period = TimeSpan.FromMinutes(1),
    };

    private static int Seconds(DateTime startAt) => (int)(startAt - BaseTime).TotalSeconds;

    private sealed class WriterResult(GrainId grainId)
    {
        public GrainId GrainId { get; } = grainId;

        public int LastAttempted { get; set; }

        public int LastAcknowledged { get; set; }

        public int Acknowledged { get; set; }

        public int Failures { get; set; }
    }
}
