using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using Orleans.TestingHost;

/// <summary>
/// A long mixed workload against several tables at once, sampling throughput, latency, errors, memory and database
/// connections, to show drift that short benchmarks cannot: leaks, pool growth, slowing down.
/// </summary>
internal static class Soak
{
    private const int WorkersPerTable = 8;
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
                foreach (var provider in new[] { Engine.ScarletAdoNet, Engine.ScarletEntityFrameworkCore })
                {
                    var cluster = engine.CreateCluster(provider);
                    clusters.Add(cluster);
                    await cluster.DeployAsync();
                    targets.Add(new Target(engine, provider, cluster.Silos[0].ServiceProvider.GetRequiredService<IReminderTable>()));
                }
            }

            using var stop = new CancellationTokenSource(duration);
            var samples = new List<string>();
            var started = Stopwatch.StartNew();
            var sampledAt = TimeSpan.Zero;
            var gen2 = GC.CollectionCount(2);
            var workers = targets.SelectMany(t => Enumerable.Range(0, WorkersPerTable)
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
                .AppendLine(CultureInfo.InvariantCulture, $"# Reminder soak, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
                .AppendLine()
                .AppendLine(CultureInfo.InvariantCulture, $"{duration.TotalMinutes:0.##} minutes, {WorkersPerTable} workers per table over {WorkersPerTable * GrainsPerWorker} grains each: 60% upserts, 30% point reads, 10% removes (absent rows are no-ops). Memory is the whole process, every table together. Connections are those the database reports for its benchmark database.")
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
            .Select(i => Workloads.Entry($"{target.Provider}-{worker}-{i}-{Guid.NewGuid():N}"))
            .ToArray();
        while (!stop.IsCancellationRequested)
        {
            var entry = entries[random.Next(entries.Length)];
            var roll = random.Next(10);
            var started = Stopwatch.GetTimestamp();
            try
            {
                if (roll < 6)
                {
                    await Workloads.UpsertAsync(target.Table, entry);
                }
                else if (roll < 9)
                {
                    await target.Table.ReadRow(entry.GrainId, entry.ReminderName, CancellationToken.None);
                }
                else
                {
                    if (entry.ETag is not null)
                    {
                        await Workloads.RemoveAsync(target.Table, entry);
                        entry.ETag = null;
                    }
                }

                target.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            }
            catch (Exception exception)
            {
                target.RecordError(exception);

                // Resynchronise as a reactivated grain would.
                try
                {
                    entry.ETag = (await target.Table.ReadRow(entry.GrainId, entry.ReminderName, CancellationToken.None))?.ETag;
                }
                catch (Exception)
                {
                }
            }
        }
    }

    private sealed class Target(Engine engine, string provider, IReminderTable table)
    {
        private readonly object sampleLock = new();
        private List<double> latencies = [];
        private int errors;

        public Engine Engine { get; } = engine;

        public string Provider { get; } = provider;

        public IReminderTable Table { get; } = table;

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
