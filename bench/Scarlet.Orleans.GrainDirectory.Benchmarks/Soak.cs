using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.GrainDirectory;
using Orleans.Runtime;
using Orleans.TestingHost;

/// <summary>
/// A long mixed workload against several tables at once, sampling throughput, latency, errors, memory and database
/// connections, to show drift that short benchmarks cannot: leaks, pool growth, slowing down.
/// </summary>
internal static class Soak
{
    private const int WorkersPerDirectory = 8;
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
                await engine.InstallInBoxSchemaAsync("GrainDirectory");
                var cluster = engine.CreateCluster();
                clusters.Add(cluster);
                await cluster.DeployAsync();
                foreach (var provider in new[] { Engine.ScarletAdoNet, Engine.ScarletEntityFrameworkCore })
                {
                    targets.Add(new Target(engine, provider, cluster.Silos[0].ServiceProvider.GetRequiredKeyedService<IGrainDirectory>(provider)));
                }
            }

            using var stop = new CancellationTokenSource(duration);
            var samples = new List<string>();
            var started = Stopwatch.StartNew();
            var sampledAt = TimeSpan.Zero;
            var gen2 = GC.CollectionCount(2);
            var workers = targets.SelectMany(t => Enumerable.Range(0, WorkersPerDirectory)
                .Select(w => Task.Run(() => RunWorkerAsync(t, w, stop.Token)))).ToArray();
            while (true)
            {
                try
                {
                    await Task.Delay(SampleInterval, stop.Token);
                }
                catch (OperationCanceledException)
                {
                }

                var finished = stop.IsCancellationRequested;
                if (finished)
                {
                    await Task.WhenAll(workers);
                }

                var now = started.Elapsed;
                var intervalSeconds = (now - sampledAt).TotalSeconds;
                sampledAt = now;
                foreach (var target in targets)
                {
                    var (operations, errors, p99) = target.TakeInterval();
                    var connections = await target.Engine.CountConnectionsAsync();
                    samples.Add(string.Create(CultureInfo.InvariantCulture,
                        $"| {started.Elapsed.TotalMinutes:0.0} | {target.Engine.Name} | {target.Provider} | {operations / intervalSeconds:0} | {p99:0.00} | {errors} | {connections} | {Environment.WorkingSet / 1048576.0:0} | {GC.GetTotalMemory(false) / 1048576.0:0} | {GC.CollectionCount(2) - gen2} |"));
                }

                Console.WriteLine(string.Join(Environment.NewLine, samples.TakeLast(targets.Count)));
                if (finished)
                {
                    break;
                }
            }

            var report = new StringBuilder()
                .AppendLine(CultureInfo.InvariantCulture, $"# Grain directory soak, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
                .AppendLine()
                .AppendLine(CultureInfo.InvariantCulture, $"{duration.TotalMinutes:0.##} minutes, {WorkersPerDirectory} workers per directory over {WorkersPerDirectory * GrainsPerWorker} grains each: 40% registrations, 40% lookups, 20% unregistrations (including absent entries). Memory is the whole process, every directory together. Connections are those the database reports for its benchmark database.")
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
        var entries = Enumerable.Range(0, GrainsPerWorker)
            .Select(i => Workloads.Address($"{target.Provider}-{worker}-{i}-{Guid.NewGuid():N}"))
            .ToArray();
        var registered = new bool[entries.Length];
        while (!stop.IsCancellationRequested)
        {
            var index = random.Next(entries.Length);
            var entry = entries[index];
            var roll = random.Next(10);
            var started = Stopwatch.GetTimestamp();
            try
            {
                if (roll < 4)
                {
                    await Workloads.RegisterAsync(target.Directory, entry);
                    registered[index] = true;
                }
                else if (roll < 8)
                {
                    var actual = await target.Directory.Lookup(entry.GrainId, CancellationToken.None);
                    if (registered[index])
                    {
                        Workloads.Verify(actual, entry);
                    }
                    else if (actual is not null)
                    {
                        throw new InvalidOperationException("An unregistered grain was found.");
                    }
                }
                else
                {
                    await target.Directory.Unregister(entry, CancellationToken.None);
                    registered[index] = false;
                }

                target.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            }
            catch (Exception exception)
            {
                target.RecordError(exception);
                try
                {
                    var actual = await target.Directory.Lookup(entry.GrainId, CancellationToken.None);
                    registered[index] = actual is not null;
                    if (actual is not null)
                    {
                        Workloads.Verify(actual, entry);
                    }
                }
                catch (Exception)
                {
                }
            }
        }
    }

    private sealed class Target(Engine engine, string provider, IGrainDirectory directory)
    {
        private readonly object sampleLock = new();
        private List<double> latencies = [];
        private int errors;

        public Engine Engine { get; } = engine;

        public string Provider { get; } = provider;

        public IGrainDirectory Directory { get; } = directory;

        public long TotalOperations { get; private set; }

        public long TotalErrors { get; private set; }

        public string? FirstError { get; private set; }

        public void Record(double milliseconds)
        {
            lock (sampleLock)
            {
                latencies.Add(milliseconds);
            }
        }

        public void RecordError(Exception exception)
        {
            Interlocked.Increment(ref errors);
            FirstError ??= $"{exception.GetType().Name}: {exception.Message}";
        }

        public (int Operations, int Errors, double P99) TakeInterval()
        {
            double[] interval;
            lock (sampleLock)
            {
                interval = latencies.ToArray();
                latencies = [];
            }
            var intervalErrors = Interlocked.Exchange(ref errors, 0);
            TotalOperations += interval.Length;
            TotalErrors += intervalErrors;
            Array.Sort(interval);
            var p99 = interval.Length == 0 ? 0 : interval[(int)Math.Min(interval.Length - 1, Math.Floor(0.99 * interval.Length))];
            return (interval.Length, intervalErrors, p99);
        }
    }
}
