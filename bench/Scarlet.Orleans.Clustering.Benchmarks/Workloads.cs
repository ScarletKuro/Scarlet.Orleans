using System.Diagnostics;
using System.Net;
using Orleans;
using Orleans.Runtime;

internal sealed record WorkloadResult(double InsertP50, double ReadAllP50, double ReadAllP99, double ReadRowP50,
    double UpdateP50, double UpdateP99, double HeartbeatP50, double HeartbeatP99, double CleanupMilliseconds,
    double HeartbeatsPerSecond, double ConcurrentHeartbeatP99, double UpdatesPerSecond, double ConflictsPerSecond,
    int Errors, string? FirstError);

internal static class Workloads
{
    public const int Silos = 100;
    public const int Operations = 1000;
    public const int Writers = 32;
    public static readonly TimeSpan Throughput = TimeSpan.FromSeconds(10);
    private static readonly DateTime Start = DateTime.UtcNow;

    public static async Task<WorkloadResult> RunAsync(IMembershipTable table)
    {
        await table.InitializeMembershipTableAsync(tryInitTableVersion: true);

        // Each write needs the version as last read: Next() keeps the read version's ETag, which a write replaces.
        var inserts = new double[Silos];
        for (var i = 0; i < Silos; i++)
        {
            var version = (await table.ReadAllAsync()).Version;
            var started = Stopwatch.GetTimestamp();
            Check(await table.InsertRowAsync(Entry(i), version.Next()), "insert");
            inserts[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        Array.Sort(inserts);
        var readAll = await MeasureAsync(async _ => Check((await table.ReadAllAsync()).Members.Count == Silos, "read all"));
        var readRow = await MeasureAsync(async i => Check((await table.ReadRowAsync(Address(i % Silos))).Members.Count == 1, "read row"));
        var updates = await MeasureAsync(async i =>
        {
            var current = await table.ReadRowAsync(Address(i % Silos));
            var etag = current.Members[0].Item2;
            var started = Stopwatch.GetTimestamp();
            Check(await table.UpdateRowAsync(Entry(i % Silos, Start.AddSeconds(i)), etag, current.Version.Next()), "update");
            return Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        });
        var heartbeats = await MeasureAsync(i => table.UpdateIAmAliveAsync(Entry(i % Silos, Start.AddSeconds(Operations + i))));

        for (var i = 0; i < Silos; i++)
        {
            var version = (await table.ReadAllAsync()).Version;
            Check(await table.InsertRowAsync(Entry(Silos + i, Start.AddDays(-2), SiloStatus.Dead), version.Next()), "dead insert");
        }

        var cleanupStarted = Stopwatch.GetTimestamp();
        await table.CleanupDefunctSiloEntriesAsync(Start.AddDays(-1));
        var cleanupMilliseconds = Stopwatch.GetElapsedTime(cleanupStarted).TotalMilliseconds;
        Check((await table.ReadAllAsync()).Members.Count == Silos, "cleanup");

        var (heartbeatLatencies, heartbeatsPerSecond, heartbeatErrors, heartbeatError) = await RunConcurrentAsync(async writer =>
        {
            await table.UpdateIAmAliveAsync(Entry(writer, DateTime.UtcNow));
            return true;
        });
        var (_, updatesPerSecond, updateErrors, updateError, conflictsPerSecond) = await RunContendedAsync(table);

        return new WorkloadResult(Percentile(inserts, 0.5), Percentile(readAll, 0.5), Percentile(readAll, 0.99), Percentile(readRow, 0.5),
            Percentile(updates, 0.5), Percentile(updates, 0.99), Percentile(heartbeats, 0.5), Percentile(heartbeats, 0.99), cleanupMilliseconds,
            heartbeatsPerSecond, Percentile(heartbeatLatencies, 0.99), updatesPerSecond, conflictsPerSecond,
            heartbeatErrors + updateErrors, heartbeatError ?? updateError);
    }

    private static async Task<(double[] Latencies, double PerSecond, int Errors, string? FirstError, double ConflictsPerSecond)> RunContendedAsync(IMembershipTable table)
    {
        var conflicts = 0;
        var (latencies, perSecond, errors, firstError) = await RunConcurrentAsync(async writer =>
        {
            var data = await table.ReadAllAsync();
            var etag = data.Members.Single(m => m.Item1.SiloAddress.Equals(Address(writer))).Item2;
            if (await table.UpdateRowAsync(Entry(writer, DateTime.UtcNow), etag, data.Version.Next()))
            {
                return true;
            }

            Interlocked.Increment(ref conflicts);
            return false;
        });
        return (latencies, perSecond, errors, firstError, conflicts / Throughput.TotalSeconds);
    }

    /// <summary>
    /// Runs one silo per worker for <see cref="Throughput"/>, timing the operations that succeed. Errors are counted, and
    /// the benchmark adds no retries.
    /// </summary>
    private static async Task<(double[] Latencies, double PerSecond, int Errors, string? FirstError)> RunConcurrentAsync(Func<int, Task<bool>> operation)
    {
        var latencies = new List<double>[Writers];
        var failures = new int[Writers];
        var firstErrors = new string?[Writers];
        var stop = Stopwatch.StartNew();
        // SQLite's async operations can complete synchronously, so independently schedule each worker.
        await Task.WhenAll(Enumerable.Range(0, Writers).Select(writer => Task.Run(async () =>
        {
            var own = new List<double>();
            latencies[writer] = own;
            while (stop.Elapsed < Throughput)
            {
                var started = Stopwatch.GetTimestamp();
                try
                {
                    if (await operation(writer))
                    {
                        own.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                    }
                }
                catch (Exception exception)
                {
                    failures[writer]++;
                    firstErrors[writer] ??= $"{exception.GetType().Name}: {exception.Message}";
                }
            }
        })));
        var elapsed = stop.Elapsed.TotalSeconds;
        var all = latencies.SelectMany(l => l).Order().ToArray();
        return (all, all.Length / elapsed, failures.Sum(), firstErrors.FirstOrDefault(e => e is not null));
    }

    private static SiloAddress Address(int silo) => SiloAddress.New(new IPAddress([10, 0, (byte)(silo / 250), (byte)(silo % 250 + 1)]), 11111, 1000 + silo);

    private static MembershipEntry Entry(int silo, DateTime? iAmAlive = null, SiloStatus status = SiloStatus.Active) => new()
    {
        SiloAddress = Address(silo),
        Status = status,
        ProxyPort = 30000,
        HostName = $"host-{silo}",
        SiloName = $"silo-{silo}",
        RoleName = "bench",
        UpdateZone = silo % 4,
        FaultZone = silo % 3,
        StartTime = status == SiloStatus.Dead ? iAmAlive ?? Start : Start,
        IAmAliveTime = iAmAlive ?? Start,
    };

    private static void Check(bool condition, string operation)
    {
        if (!condition)
        {
            throw new InvalidOperationException($"The {operation} did not succeed.");
        }
    }

    private static async Task<double[]> MeasureAsync(Func<int, Task> operation) =>
        await MeasureAsync(async i =>
        {
            var started = Stopwatch.GetTimestamp();
            await operation(i);
            return Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        });

    /// <summary>Runs <see cref="Operations"/> operations, each returning its own timed latency.</summary>
    private static async Task<double[]> MeasureAsync(Func<int, Task<double>> operation)
    {
        var samples = new double[Operations];
        for (var i = 0; i < samples.Length; i++)
        {
            samples[i] = await operation(i);
        }

        Array.Sort(samples);
        return samples;
    }

    private static double Percentile(double[] sorted, double percentile) =>
        sorted.Length == 0 ? 0 : sorted[(int)Math.Min(sorted.Length - 1, Math.Floor(percentile * sorted.Length))];
}
