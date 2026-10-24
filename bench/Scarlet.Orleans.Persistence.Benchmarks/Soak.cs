using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

/// <summary>
/// A long mixed workload against several storages at once, sampling throughput, latency, errors, memory and database
/// connections, to show drift that short benchmarks cannot: leaks, pool growth, slowing down.
/// </summary>
internal static class Soak
{
    private const int WorkersPerStorage = 8;
    private const int GrainsPerWorker = 250;
    private static readonly TimeSpan SampleInterval = TimeSpan.FromSeconds(30);

    public static async Task RunAsync(IReadOnlyList<Engine> engines, TimeSpan duration, string output)
    {
        var clusters = new List<InProcessTestCluster>();
        var targets = new List<Target>();
        try
        {
            foreach (var engine in engines)
            {
                // The silo also hosts the in-box provider, which the soak does not drive but which needs its schema.
                await engine.InstallInBoxSchemaAsync();
                var builder = new InProcessTestClusterBuilder(1);
                builder.ConfigureSilo((_, silo) => engine.AddProviders(silo));
                builder.ConfigureHost(host => host.Logging.SetMinimumLevel(LogLevel.Warning));
                var cluster = builder.Build();
                await cluster.DeployAsync();
                clusters.Add(cluster);
                foreach (var provider in new[] { Engine.ScarletAdoNet, Engine.ScarletEntityFrameworkCore })
                {
                    targets.Add(new Target(engine, provider, cluster.Silos[0].ServiceProvider.GetRequiredKeyedService<IGrainStorage>(provider)));
                }
            }

            using var stop = new CancellationTokenSource(duration);
            var workers = targets.SelectMany(t => Enumerable.Range(0, WorkersPerStorage).Select(w => RunWorkerAsync(t, w, stop.Token))).ToArray();

            var samples = new List<string>();
            var started = Stopwatch.StartNew();
            var gen2 = GC.CollectionCount(2);
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(SampleInterval, stop.Token);
                }
                catch (OperationCanceledException)
                {
                }

                foreach (var target in targets)
                {
                    var (operations, errors, p99) = target.TakeInterval();
                    var connections = await target.Engine.CountConnectionsAsync();
                    samples.Add(string.Create(CultureInfo.InvariantCulture,
                        $"| {started.Elapsed.TotalMinutes:0.0} | {target.Engine.Name} | {target.Provider} | {operations / SampleInterval.TotalSeconds:0} | {p99:0.00} | {errors} | {connections} | {Environment.WorkingSet / 1048576.0:0} | {GC.GetTotalMemory(false) / 1048576.0:0} | {GC.CollectionCount(2) - gen2} |"));
                }

                Console.WriteLine(samples[^1]);
            }

            await Task.WhenAll(workers);
            var report = new StringBuilder()
                .AppendLine(CultureInfo.InvariantCulture, $"# Soak, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
                .AppendLine()
                .AppendLine(CultureInfo.InvariantCulture, $"{duration.TotalMinutes:0} minutes, {WorkersPerStorage} workers per storage over {WorkersPerStorage * GrainsPerWorker} grains each: 60% writes, 30% reads, 10% clears. Memory is the whole process, every storage together. Connections are those the database reports for its benchmark database.")
                .AppendLine()
                .AppendLine("| Minute | Engine | Provider | Operations/s | p99 ms | Errors | Connections | Working set MB | Managed heap MB | Gen 2 GCs |")
                .AppendLine("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|");
            foreach (var sample in samples)
            {
                report.AppendLine(sample);
            }

            report.AppendLine().AppendLine("## Totals").AppendLine();
            foreach (var target in targets)
            {
                report.AppendLine(CultureInfo.InvariantCulture, $"- {target.Engine.Name} / {target.Provider}: {target.TotalOperations} operations, {target.TotalErrors} errors{(target.FirstError is { } e ? $" (first: {e})" : "")}");
            }

            await File.WriteAllTextAsync(output, report.ToString());
            Console.WriteLine(report);
        }
        finally
        {
            foreach (var cluster in clusters)
            {
                await cluster.DisposeAsync();
            }
        }
    }

    private static async Task RunWorkerAsync(Target target, int worker, CancellationToken stop)
    {
        var random = new Random(HashCode.Combine(target.Provider, worker));
        var payload = new byte[1024];
        var grains = Enumerable.Range(0, GrainsPerWorker)
            .Select(i => (Id: GrainId.Create("soak-grain", $"{target.Provider}-{worker}-{i}-{Guid.NewGuid():N}"), State: new GrainState<BenchState> { State = new BenchState() }))
            .ToArray();
        while (!stop.IsCancellationRequested)
        {
            var (id, state) = grains[random.Next(grains.Length)];
            var roll = random.Next(10);
            var started = Stopwatch.GetTimestamp();
            try
            {
                if (roll < 6)
                {
                    random.NextBytes(payload);
                    state.State = new BenchState { Data = payload };
                    await target.Storage.WriteStateAsync("soak", id, state);
                }
                else if (roll < 9)
                {
                    await target.Storage.ReadStateAsync("soak", id, new GrainState<BenchState>());
                }
                else
                {
                    await target.Storage.ClearStateAsync("soak", id, state);
                }

                target.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            }
            catch (Exception exception)
            {
                target.RecordError(exception);

                // Resynchronise as a reactivated grain would.
                try
                {
                    await target.Storage.ReadStateAsync("soak", id, state);
                }
                catch (Exception)
                {
                }
            }
        }
    }

    private sealed class Target(Engine engine, string provider, IGrainStorage storage)
    {
        private ConcurrentQueue<double> latencies = new();
        private int errors;

        public Engine Engine { get; } = engine;

        public string Provider { get; } = provider;

        public IGrainStorage Storage { get; } = storage;

        public long TotalOperations { get; private set; }

        public long TotalErrors { get; private set; }

        public string? FirstError { get; private set; }

        public void Record(double milliseconds) => latencies.Enqueue(milliseconds);

        public void RecordError(Exception exception)
        {
            Interlocked.Increment(ref errors);
            FirstError ??= $"{exception.GetType().Name}: {exception.Message}";
        }

        public (int Operations, int Errors, double P99) TakeInterval()
        {
            var interval = Interlocked.Exchange(ref latencies, new ConcurrentQueue<double>()).ToArray();
            var intervalErrors = Interlocked.Exchange(ref errors, 0);
            TotalOperations += interval.Length;
            TotalErrors += intervalErrors;
            Array.Sort(interval);
            var p99 = interval.Length == 0 ? 0 : interval[(int)Math.Min(interval.Length - 1, Math.Floor(0.99 * interval.Length))];
            return (interval.Length, intervalErrors, p99);
        }
    }
}
