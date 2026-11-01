using System.Diagnostics;
using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions.Tests;

/// <summary>
/// The store under concurrency and real connection failures. Writers drive their states as Orleans does: load, then
/// either commit the pending version or prepare the next one. Every payload is its sequence number, so a load that mixed
/// two stores, or a store applied in part, shows as a committed number without its entry, or a version out of place.
/// </summary>
public abstract class TransactionalStateConcurrencyTests(DatabaseFixture fixture)
{
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(4);

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Loads_DuringConcurrentStores_AreEachOneSnapshot()
    {
        var store = await CreateStoreAsync(fixture.Database);
        await RunSnapshotWorkloadAsync(store);
    }

    /// <summary>
    /// On SQL Server, the same with <c>READ_COMMITTED_SNAPSHOT</c> on, which changes locking reads but not
    /// <c>REPEATABLE READ</c>, so loads stay snapshots without relying on the setting.
    /// </summary>
    [Fact]
    public async Task Loads_DuringConcurrentStores_AreEachOneSnapshot_WithReadCommittedSnapshot()
    {
        Assert.SkipUnless(fixture.Database.Engine == "SqlServer", "READ_COMMITTED_SNAPSHOT is a SQL Server setting.");
        var database = await fixture.CreateDatabaseAsync();
        await using (var command = database.DataSource.CreateCommand("DECLARE @sql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + N' SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE'; EXEC (@sql);"))
        {
            await command.ExecuteNonQueryAsync(CancellationToken);
        }

        await RunSnapshotWorkloadAsync(await CreateStoreAsync(database));
    }

    [Fact]
    public async Task Stores_WhileSessionsAreKilled_AreAllOrNothing()
    {
        Assert.SkipWhen(fixture.Database.Engine == "Sqlite", "SQLite has no sessions to kill.");
        Assert.SkipWhen(fixture.Database.Driver == TestDrivers.MySqlData, "MySql.Data hangs on connections killed by the server.");
        var database = fixture.Database;
        var store = await CreateStoreAsync(database);
        using var stop = new CancellationTokenSource(Duration);

        var killer = ConnectionFaults.KillSessionsAsync(database, stop.Token);
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => DriveAsync(store, NewKey(), stop.Token)));
        var kills = await killer;

        Assert.True(kills > 0, "The test database had no sessions to kill.");
        Assert.True(results.Sum(r => r.Failures) > 0, $"Killing {kills} sessions failed no store, so this test exercised nothing.");
        await VerifyAsync(store, results);
    }

    [Fact]
    public async Task Stores_WhoseResponsesAreLost_AreAllOrNothing()
    {
        Assert.SkipWhen(fixture.Database.Engine == "Sqlite", "SQLite has no connection to break.");
        Assert.SkipWhen(fixture.Database.Driver == TestDrivers.MySqlData, "MySql.Data hangs on connections broken mid-response.");
        var (host, port, connect) = ConnectionFaults.Route(fixture.Database);
        await using var proxy = new ChaosProxy(host, port);
        var (connectionString, dataSource) = connect(proxy.Port);
        await using var database = new TestDatabase(fixture.Database.Engine, fixture.Database.Driver, connectionString, dataSource, () => Task.CompletedTask);
        var store = await CreateStoreAsync(database);
        using var stop = new CancellationTokenSource(Duration);

        proxy.DropProbability = 0.05;
        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => DriveAsync(store, NewKey(), stop.Token)));
        proxy.DropProbability = 0;

        Assert.True(proxy.Drops > 0 && results.Sum(r => r.Failures) > 0, $"{proxy.Drops} dropped responses failed no store, so this test exercised nothing.");
        await VerifyAsync(store, results);
    }

    private static async Task RunSnapshotWorkloadAsync(ITransactionalStateStore store)
    {
        var key = NewKey();
        await StepAsync(store, key);
        using var stop = new CancellationTokenSource(Duration);
        var loads = 0;

        // SQLite completes its async calls synchronously, so each worker gets its own task.
        var writers = Enumerable.Range(0, 4).Select(_ => Task.Run(() => DriveAsync(store, key, stop.Token, tolerateFailures: false))).ToArray();
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                AssertConsistent(await store.LoadAsync(key, CancellationToken.None));
                Interlocked.Increment(ref loads);
            }
        })).ToArray();
        var results = await Task.WhenAll(writers);
        await Task.WhenAll(readers);

        Assert.True(results.Sum(r => r.Acknowledged) > 10, $"Too few stores ran to test anything: {results.Sum(r => r.Acknowledged)} stores, {loads} loads.");
        Assert.True(loads > 10, $"Too few loads ran to test anything: {results.Sum(r => r.Acknowledged)} stores, {loads} loads.");
    }

    private static async Task VerifyAsync(ITransactionalStateStore store, DriveResult[] results)
    {
        Assert.True(results.Sum(r => r.Acknowledged) > 0, "No store succeeded.");
        foreach (var result in results)
        {
            var loaded = await UntilAvailableAsync(() => store.LoadAsync(result.Key, CancellationToken.None));
            AssertConsistent(loaded);
            if (result.Acknowledged > 0)
            {
                Assert.NotNull(loaded);
                Assert.True(loaded.Entries.Count > 0 && loaded.Entries[^1].SequenceId >= result.LastAcknowledged, $"A store acknowledged at sequence {result.LastAcknowledged} was lost.");
            }

            await UntilAvailableAsync(() => StepAsync(store, result.Key));
        }
    }

    /// <summary>
    /// The committed entry, if any is committed, and at most the next version pending, each holding its own sequence number.
    /// </summary>
    private static void AssertConsistent(StoredTransactionalState? loaded)
    {
        if (loaded is null)
        {
            return;
        }

        var expected = Enumerable.Range(0, loaded.Entries.Count).Select(i => loaded.CommittedSequenceId + i + (loaded.CommittedSequenceId == 0 ? 1 : 0)).ToArray();
        Assert.Equal(expected, loaded.Entries.Select(e => e.SequenceId).ToArray());
        Assert.InRange(loaded.Entries.Count, loaded.CommittedSequenceId == 0 ? 0 : 1, 2);
        Assert.All(loaded.Entries, e => Assert.Equal(e.SequenceId, BitConverter.ToInt64(e.Payload)));
    }

    /// <summary>
    /// Keeps advancing the state until <paramref name="stop"/>. A failed store has an unknown outcome; the next load finds
    /// out what is stored, as Orleans does.
    /// </summary>
    private static async Task<DriveResult> DriveAsync(ITransactionalStateStore store, TransactionalStateKey key, CancellationToken stop, bool tolerateFailures = true)
    {
        var result = new DriveResult(key);
        while (!stop.IsCancellationRequested)
        {
            try
            {
                if (await StepAsync(store, key) is { } sequence)
                {
                    result.Acknowledged++;
                    result.LastAcknowledged = Math.Max(result.LastAcknowledged, sequence);
                }
            }
            catch (Exception) when (tolerateFailures)
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
    /// Commits the pending version if there is one, or prepares the next. Returns the sequence number written, or
    /// <see langword="null"/> on a conflict.
    /// </summary>
    private static async Task<long?> StepAsync(ITransactionalStateStore store, TransactionalStateKey key)
    {
        var loaded = await store.LoadAsync(key, CancellationToken.None);
        var committed = loaded?.CommittedSequenceId ?? 0;
        var pending = loaded?.Entries.Where(e => e.SequenceId > committed).Select(e => e.SequenceId).FirstOrDefault() ?? 0;
        var request = pending > 0
            ? Request(loaded!.ETag, [], commitUpTo: pending)
            : Request(loaded?.ETag, [Entry(committed + 1)], commitUpTo: null);
        var result = await store.StoreAsync(key, request, CancellationToken.None);
        Assert.NotEqual(StoreStatus.MissingCommitRecord, result.Status);
        return result.Status == StoreStatus.Stored ? (pending > 0 ? pending : committed + 1) : null;
    }

    private static async Task<ITransactionalStateStore> CreateStoreAsync(TestDatabase database)
    {
        var store = FlavourTransactions.CreateStore(database, TestSchemaMode.Apply);
        await store.InitializeAsync(CancellationToken);
        return store;
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
            catch (Exception) when (deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                await Task.Delay(50);
            }
        }
    }

    private static StoreRequest Request(byte[]? expectedETag, StoredEntry[] prepared, long? commitUpTo) =>
        new(expectedETag, ETags.New(TimeProvider.System), [7], prepared, commitUpTo, null, DateTimeOffset.UtcNow);

    private static StoredEntry Entry(long sequenceId) => new(sequenceId, $"transaction-{sequenceId}", sequenceId, [1], BitConverter.GetBytes(sequenceId));

    private static TransactionalStateKey NewKey() => TransactionalStateKey.Create("service", "state", GrainId.Create("concurrency", Guid.NewGuid().ToString("N")));

    private sealed class DriveResult(TransactionalStateKey key)
    {
        public TransactionalStateKey Key { get; } = key;

        public int Acknowledged { get; set; }

        public long LastAcknowledged { get; set; }

        public int Failures { get; set; }
    }
}
