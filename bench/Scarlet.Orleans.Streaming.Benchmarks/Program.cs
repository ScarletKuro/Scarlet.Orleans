using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.TestingHost;
using Scarlet.Orleans.Streaming.Benchmarks;

// Relational stream providers compared in one silo per engine, on the same database: Scarlet's, and the in-box
// Microsoft.Orleans.Streaming.AdoNet on the server engines. Server engines come from the SCARLET_TEST_* administrative
// connection strings; SQLite always runs.
var output = args.FirstOrDefault(a => a.EndsWith(".md", StringComparison.OrdinalIgnoreCase)) ?? "streaming-results.md";
var engines = new List<Engine> { Engine.Sqlite() };
foreach (var (name, variable) in new[] { ("PostgreSql", "SCARLET_TEST_POSTGRESQL"), ("SqlServer", "SCARLET_TEST_SQLSERVER"), ("MySql", "SCARLET_TEST_MYSQL") })
{
    if (Environment.GetEnvironmentVariable(variable) is { Length: > 0 } admin)
    {
        engines.Add(await Engine.CreateAsync(name, admin));
    }
}

var failures = new List<string>();
var report = new StringBuilder()
    .AppendLine(CultureInfo.InvariantCulture, $"# Streaming benchmarks, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
    .AppendLine()
    .AppendLine(CultureInfo.InvariantCulture, $"{Environment.ProcessorCount} logical processors, .NET {Environment.Version}, {Bench.QueueCount} queues per provider. Enqueue: {Bench.Sequential} events sent one at a time from a client. Delivery: {Bench.Producers} client producers, each sending to its own stream for {Bench.Duration.TotalSeconds:0} s as fast as they can, read by one implicitly subscribed consumer grain per stream; throughput counts deliveries over the time from the first send to the last delivery, and latency runs from send to consumer. Faster enqueues mean more load, so the paced run sends one event every {Bench.PacedInterval.TotalMilliseconds:0} ms per producer ({Bench.Producers * 1000 / (int)Bench.PacedInterval.TotalMilliseconds} events per second), and compares latency at the same load. Latencies in milliseconds. Every event must be delivered within {Bench.DrainTimeout.TotalSeconds:0} s of the last send, or the run is incomplete; deliveries beyond the events sent are duplicates.")
    .AppendLine()
    .AppendLine("| Engine | Provider | Enqueue p50 | Enqueue p99 | Sent | Delivered/s | Delivery p50 | Delivery p99 | Paced p50 | Paced p99 | Duplicates |")
    .AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
try
{
    foreach (var engine in engines)
    {
        var providers = Bench.ProvidersOf(engine);
        try
        {
            await engine.InstallInBoxSchemaAsync("Streaming");
        }
        catch (Exception exception)
        {
            // The in-box scripts cannot be installed on this engine, so the in-box provider is not measured here.
            failures.Add($"- {engine.Name} / {Engine.InBox}: its schema scripts failed to install: {exception.GetType().Name}: {exception.Message.Split('\n')[0]}");
            report.AppendLine(CultureInfo.InvariantCulture, $"| {engine.Name} | {Engine.InBox} | failed, see below |  |  |  |  |  |  |  |  |");
            providers = [Providers.Scarlet];
        }

        await using var cluster = Bench.CreateCluster(engine, withInBox: providers.Contains(Providers.InBox));
        await cluster.DeployAsync();
        foreach (var provider in providers)
        {
            var label = provider == Providers.Scarlet ? Engine.ScarletAdoNet : Engine.InBox;
            Console.WriteLine($"{engine.Name} / {label}...");
            try
            {
                var result = await Bench.RunAsync(cluster.Client, provider);
                report.AppendLine(CultureInfo.InvariantCulture,
                    $"| {engine.Name} | {label} | {result.EnqueueP50:0.00} | {result.EnqueueP99:0.00} | {result.Sent} | {result.PerSecond:0} | {result.DeliveryP50:0.0} | {result.DeliveryP99:0.0} | {result.PacedP50:0.0} | {result.PacedP99:0.0} | {result.Duplicates} |");
            }
            catch (Exception exception)
            {
                failures.Add($"- {engine.Name} / {label}: {exception.GetType().Name}: {exception.Message.Split('\n')[0]}");
                report.AppendLine(CultureInfo.InvariantCulture, $"| {engine.Name} | {label} | failed, see below |  |  |  |  |  |  |  |  |");
            }
        }

        await File.WriteAllTextAsync(output, report.ToString());
    }

    if (failures.Count > 0)
    {
        report.AppendLine().AppendLine("## Incomplete runs").AppendLine();
        foreach (var failure in failures)
        {
            report.AppendLine(failure);
        }

        await File.WriteAllTextAsync(output, report.ToString());
    }

    Console.WriteLine(report);
}
finally
{
    foreach (var engine in engines)
    {
        await engine.DisposeAsync();
    }
}

internal sealed record BenchResult(double EnqueueP50, double EnqueueP99, int Sent, double PerSecond, double DeliveryP50, double DeliveryP99, double PacedP50, double PacedP99, int Duplicates);

internal static class Bench
{
    public const int QueueCount = 4;
    public const int Sequential = 1000;
    public const int Producers = 16;
    public static readonly TimeSpan Duration = TimeSpan.FromSeconds(10);

    /// <summary>One event every 20 ms per producer: 800 events per second in all, below either provider's throughput.</summary>
    public static readonly TimeSpan PacedInterval = TimeSpan.FromMilliseconds(20);
    public static readonly TimeSpan DrainTimeout = TimeSpan.FromMinutes(2);

    public static IReadOnlyList<string> ProvidersOf(Engine engine) => engine.Name == "Sqlite" ? [Providers.Scarlet] : [Providers.Scarlet, Providers.InBox];

    public static async Task<BenchResult> RunAsync(IClusterClient client, string provider)
    {
        var streams = client.GetStreamProvider(provider);
        var space = Providers.Namespace(provider);

        // Warm up: activate a consumer and the queues' readers.
        var warmUp = streams.GetStream<long>(StreamId.Create(space, Guid.NewGuid()));
        Deliveries.Reset();
        await warmUp.OnNextAsync(Stopwatch.GetTimestamp());
        await DrainAsync(1);

        Deliveries.Reset();
        var enqueue = new double[Sequential];
        var single = streams.GetStream<long>(StreamId.Create(space, Guid.NewGuid()));
        for (var i = 0; i < Sequential; i++)
        {
            var started = Stopwatch.GetTimestamp();
            await single.OnNextAsync(started);
            enqueue[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        await DrainAsync(Sequential);
        Array.Sort(enqueue);

        var flat = await DeliverAsync(streams, space, pace: null);
        var paced = await DeliverAsync(streams, space, PacedInterval);
        return new(
            Percentile(enqueue, 0.5), Percentile(enqueue, 0.99), flat.Sent, flat.PerSecond, flat.P50, flat.P99,
            paced.P50, paced.P99, flat.Duplicates + paced.Duplicates);
    }

    /// <summary>
    /// <see cref="Producers"/> producers send to their own streams for <see cref="Duration"/>, as fast as they can, or one
    /// event every <paramref name="pace"/> each, which offers both providers the same load.
    /// </summary>
    private static async Task<(int Sent, double PerSecond, double P50, double P99, int Duplicates)> DeliverAsync(IStreamProvider streams, string space, TimeSpan? pace)
    {
        Deliveries.Reset();
        var sent = 0;
        var start = Stopwatch.GetTimestamp();
        using var stop = new CancellationTokenSource(Duration);
        await Task.WhenAll(Enumerable.Range(0, Producers).Select(_ => Task.Run(async () =>
        {
            var stream = streams.GetStream<long>(StreamId.Create(space, Guid.NewGuid()));
            using var timer = pace is { } interval ? new PeriodicTimer(interval) : null;
            while (!stop.IsCancellationRequested)
            {
                await stream.OnNextAsync(Stopwatch.GetTimestamp());
                Interlocked.Increment(ref sent);
                if (timer is not null && !await timer.WaitForNextTickAsync(CancellationToken.None))
                {
                    return;
                }
            }
        })));

        await DrainAsync(sent);
        var elapsed = Stopwatch.GetElapsedTime(start);
        var latencies = Deliveries.Latencies();
        return (sent, latencies.Length / elapsed.TotalSeconds, Percentile(latencies, 0.5), Percentile(latencies, 0.99), latencies.Length - sent);
    }

    public static InProcessTestCluster CreateCluster(Engine engine, bool withInBox)
    {
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureHost(host => host.Logging.SetMinimumLevel(LogLevel.Warning));
        builder.ConfigureSilo((_, silo) =>
        {
            silo.AddScarletAdoNetGrainStorage("PubSubStore", options =>
            {
                options.DataSource = engine.DataSource;
                options.SchemaMode = Scarlet.Orleans.Persistence.AdoNet.SchemaMode.Apply;
            });
            silo.AddScarletAdoNetStreams(Providers.Scarlet, options => Configure(options, engine), streams =>
                streams.Configure<HashRingStreamQueueMapperOptions>(o => o.Configure(q => q.TotalQueueCount = QueueCount)));
            if (withInBox)
            {
                silo.AddAdoNetStreams(Providers.InBox, streams => streams
                    .ConfigureAdoNet(o => o.Configure(options => ConfigureInBox(options, engine)))
                    .ConfigurePartitioning(QueueCount));
            }
        });
        builder.ConfigureClient(client =>
        {
            client.AddScarletAdoNetStreams(Providers.Scarlet, options => Configure(options, engine), streams =>
                streams.Configure<HashRingStreamQueueMapperOptions>(o => o.Configure(q => q.TotalQueueCount = QueueCount)));
            if (withInBox)
            {
                client.AddAdoNetStreams(Providers.InBox, streams => streams
                    .ConfigureAdoNet(o => o.Configure(options => ConfigureInBox(options, engine)))
                    .ConfigurePartitioning(QueueCount));
            }
        });
        return builder.Build();
    }

    private static void Configure(Scarlet.Orleans.Streaming.AdoNet.ScarletAdoNetStreamOptions options, Engine engine)
    {
        options.DataSource = engine.DataSource;
        options.SchemaMode = Scarlet.Orleans.Streaming.AdoNet.SchemaMode.Apply;
    }

    private static void ConfigureInBox(AdoNetStreamOptions options, Engine engine)
    {
        options.Invariant = engine.Name switch
        {
            "PostgreSql" => "Npgsql",
            "SqlServer" => "Microsoft.Data.SqlClient",
            _ => "MySql.Data.MySqlConnector",
        };
        options.ConnectionString = engine.ConnectionString;
    }

    /// <summary>Waits until <paramref name="expected"/> events were delivered, or fails the run.</summary>
    private static async Task DrainAsync(int expected)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(DrainTimeout.TotalSeconds * Stopwatch.Frequency);
        while (Deliveries.Count < expected)
        {
            if (Stopwatch.GetTimestamp() > deadline)
            {
                throw new TimeoutException($"Only {Deliveries.Count} of {expected} events were delivered within {DrainTimeout.TotalSeconds:0} s.");
            }

            await Task.Delay(20);
        }
    }

    private static double Percentile(double[] sorted, double percentile) =>
        sorted.Length == 0 ? 0 : sorted[(int)Math.Min(sorted.Length - 1, Math.Floor(percentile * sorted.Length))];
}
