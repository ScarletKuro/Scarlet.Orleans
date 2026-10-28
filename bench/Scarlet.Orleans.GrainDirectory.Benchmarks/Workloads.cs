using System.Diagnostics;
using Orleans.GrainDirectory;
using Orleans.Runtime;

internal sealed record WorkloadResult(double RegisterP50, double RegisterP99, double ExistingP50, double ExistingP99,
    double LookupP50, double LookupP99, double MissingP50, double UnregisterP50, double CleanupMilliseconds,
    double ConcurrentPerSecond, double ConcurrentP99, int ConcurrentErrors, string? FirstError);

internal static class Workloads
{
    public const int Operations = 1000;
    public const int Writers = 32;
    public static readonly TimeSpan Throughput = TimeSpan.FromSeconds(10);
    public static readonly SiloAddress Silo = SiloAddress.FromParsableString("10.0.23.12:1000@5678");
    private static readonly SiloAddress CleanupSilo = SiloAddress.FromParsableString("10.0.23.14:1000@4583");

    public static GrainAddress Address(string name, SiloAddress? silo = null) => new()
    {
        GrainId = GrainId.Create("bench-grain", name),
        ActivationId = ActivationId.NewId(),
        SiloAddress = silo ?? Silo,
    };

    public static void Verify(GrainAddress? actual, GrainAddress expected)
    {
        // The in-box provider does not persist MembershipVersion, so compare the common contract fields.
        if (actual is null || !actual.GrainId.Equals(expected.GrainId)
            || !actual.ActivationId.Equals(expected.ActivationId) || !actual.SiloAddress!.Equals(expected.SiloAddress))
        {
            throw new InvalidOperationException($"Expected activation {expected}, got {actual}.");
        }
    }

    public static async Task RegisterAsync(IGrainDirectory directory, GrainAddress address) =>
        Verify(await directory.Register(address), address);

    public static async Task<WorkloadResult> RunAsync(IGrainDirectory directory)
    {
        for (var i = 0; i < 100; i++)
        {
            var warmup = Address($"warmup-{i}");
            await CycleAsync(directory, warmup);
        }

        var entries = Enumerable.Range(0, Operations).Select(i => Address($"register-{i}")).ToArray();
        var candidates = Enumerable.Range(0, Operations).Select(i => Address($"register-{i}")).ToArray();
        var missing = Enumerable.Range(0, Operations).Select(i => Address($"missing-{i}").GrainId).ToArray();
        var registrations = await MeasureAsync(i => RegisterAsync(directory, entries[i]));
        var existing = await MeasureAsync(async i => Verify(await directory.Register(candidates[i]), entries[i]));
        var lookups = await MeasureAsync(async i => Verify(await directory.Lookup(entries[i].GrainId), entries[i]));
        var misses = await MeasureAsync(async i =>
        {
            if (await directory.Lookup(missing[i]) is not null)
            {
                throw new InvalidOperationException("Missing lookup returned a registration.");
            }
        });
        var unregisters = await MeasureAsync(i => directory.Unregister(entries[i]));
        await VerifyAbsentAsync(directory, entries);

        var cleanupEntries = Enumerable.Range(0, Operations).Select(i => Address($"cleanup-{i}", CleanupSilo)).ToArray();
        foreach (var entry in cleanupEntries)
        {
            await RegisterAsync(directory, entry);
        }

        // Keep a live silo registration to check cleanup does not remove unrelated entries.
        var live = Address("cleanup-survivor");
        await RegisterAsync(directory, live);
        var cleanupStarted = Stopwatch.GetTimestamp();
        await directory.UnregisterSilos([CleanupSilo]);
        var cleanupMilliseconds = Stopwatch.GetElapsedTime(cleanupStarted).TotalMilliseconds;
        await VerifyAbsentAsync(directory, cleanupEntries);
        Verify(await directory.Lookup(live.GrainId), live);
        await directory.Unregister(live);

        var latencies = new List<double>[Writers];
        var failures = new int[Writers];
        var firstErrors = new string?[Writers];
        var stop = Stopwatch.StartNew();
        // SQLite's async operations can complete synchronously, so independently schedule each worker.
        await Task.WhenAll(Enumerable.Range(0, Writers).Select(writer => Task.Run(async () =>
        {
            var own = new List<double>();
            latencies[writer] = own;
            var address = Address($"concurrent-{writer}");
            while (stop.Elapsed < Throughput)
            {
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await CycleAsync(directory, address);
                    own.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                }
                catch (Exception exception)
                {
                    // Report baseline failures as well as successes; do not add benchmark-level retries.
                    failures[writer]++;
                    firstErrors[writer] ??= $"{exception.GetType().Name}: {exception.Message}";
                }
            }
        })));
        var elapsed = stop.Elapsed.TotalSeconds;
        var concurrent = latencies.SelectMany(l => l).Order().ToArray();
        await directory.UnregisterSilos([Silo]);

        return new WorkloadResult(Percentile(registrations, 0.5), Percentile(registrations, 0.99),
            Percentile(existing, 0.5), Percentile(existing, 0.99), Percentile(lookups, 0.5), Percentile(lookups, 0.99),
            Percentile(misses, 0.5), Percentile(unregisters, 0.5), cleanupMilliseconds,
            concurrent.Length / elapsed, Percentile(concurrent, 0.99), failures.Sum(), firstErrors.FirstOrDefault(e => e is not null));
    }

    private static async Task CycleAsync(IGrainDirectory directory, GrainAddress address)
    {
        await RegisterAsync(directory, address);
        Verify(await directory.Lookup(address.GrainId), address);
        await directory.Unregister(address);
    }

    private static async Task VerifyAbsentAsync(IGrainDirectory directory, IEnumerable<GrainAddress> entries)
    {
        foreach (var entry in entries)
        {
            if (await directory.Lookup(entry.GrainId) is not null)
            {
                throw new InvalidOperationException($"Unregistration left an entry for {entry.GrainId}.");
            }
        }
    }

    private static async Task<double[]> MeasureAsync(Func<int, Task> operation)
    {
        var samples = new double[Operations];
        for (var i = 0; i < samples.Length; i++)
        {
            var started = Stopwatch.GetTimestamp();
            await operation(i);
            samples[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        Array.Sort(samples);
        return samples;
    }

    private static double Percentile(double[] sorted, double percentile) =>
        sorted.Length == 0 ? 0 : sorted[(int)Math.Min(sorted.Length - 1, Math.Floor(percentile * sorted.Length))];
}
