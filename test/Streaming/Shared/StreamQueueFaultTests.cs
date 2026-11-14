using System.Collections.Concurrent;

namespace Scarlet.Orleans.Streaming.Tests;

/// <summary>
/// The queue store through a connection that loses responses: an enqueue retried with its key stores the message once,
/// and a lost dequeue or confirmation leaves the message to be delivered again, so every message is delivered.
/// </summary>
public abstract class StreamQueueFaultTests(DatabaseFixture fixture)
{
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan Visibility = TimeSpan.FromSeconds(2);

    /// <summary>With one response in twenty dropped, twenty failures in a row mean something else is wrong.</summary>
    private const int MaxEnqueueAttempts = 20;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Messages_ThroughLostResponses_AreAllDelivered()
    {
        fixture.Require();
        Assert.SkipWhen(fixture.Database.Engine == "Sqlite", "SQLite has no connection to break.");
        Assert.SkipWhen(fixture.Database.Driver == TestDrivers.MySqlData, "MySql.Data hangs on connections broken mid-response.");
        var direct = FlavourStreaming.CreateStore(fixture.Database, TestSchemaMode.Apply);
        await direct.InitializeAsync(CancellationToken);
        var (host, port, connect) = ConnectionFaults.Route(fixture.Database);
        await using var proxy = new ChaosProxy(host, port);
        var (connectionString, dataSource) = connect(proxy.Port);
        await using var database = new TestDatabase(fixture.Database.Engine, fixture.Database.Driver, connectionString, dataSource, () => Task.CompletedTask);
        var store = FlavourStreaming.CreateStore(database, TestSchemaMode.None);
        await store.InitializeAsync(CancellationToken);
        var queue = StreamQueueKey.Create("service", "provider", Guid.NewGuid().ToString("N"));
        var enqueued = new ConcurrentBag<Guid>();
        var delivered = new ConcurrentDictionary<Guid, int>();
        var failures = 0;
        using var stop = new CancellationTokenSource(Duration);

        proxy.DropProbability = 0.05;
        var producers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var key = Guid.NewGuid();
                for (var attempt = 1; ; attempt++)
                {
                    try
                    {
                        var now = DateTimeOffset.UtcNow;
                        await store.EnqueueAsync(queue, key, key.ToByteArray(), now, now.AddMinutes(10), CancellationToken);
                        enqueued.Add(key);
                        break;
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException && attempt < MaxEnqueueAttempts)
                    {
                        Interlocked.Increment(ref failures);
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        throw new InvalidOperationException($"An enqueue still failed after {MaxEnqueueAttempts} attempts, more than dropped responses explain.", exception);
                    }
                }
            }
        }, CancellationToken));
        var consumer = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await ReceiveAsync(store, queue, delivered);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    Interlocked.Increment(ref failures);
                }
            }
        }, CancellationToken);
        await Task.WhenAll([.. producers, consumer]);
        proxy.DropProbability = 0;

        Assert.True(proxy.Drops > 0 && failures > 0, $"{proxy.Drops} dropped responses failed no operation, so this test exercised nothing.");
        var deadline = DateTimeOffset.UtcNow.AddSeconds(30);
        while (!enqueued.All(delivered.ContainsKey) && DateTimeOffset.UtcNow < deadline)
        {
            await ReceiveAsync(direct, queue, delivered);
            await Task.Delay(100, CancellationToken);
        }

        Assert.DoesNotContain(enqueued, key => !delivered.ContainsKey(key));
    }

    /// <summary>Dequeues a batch, records it, and confirms it.</summary>
    private static async Task ReceiveAsync(IStreamQueueStore store, StreamQueueKey queue, ConcurrentDictionary<Guid, int> delivered)
    {
        var now = DateTimeOffset.UtcNow;
        var messages = await store.DequeueAsync(queue, 50, 100, now, now + Visibility, CancellationToken.None);
        foreach (var message in messages)
        {
            delivered.AddOrUpdate(new Guid(message.Payload), 1, static (_, n) => n + 1);
        }

        if (messages.Count > 0)
        {
            await store.ConfirmAsync(queue, [.. messages.Select(m => m.ToReceipt())], CancellationToken.None);
        }
    }
}
