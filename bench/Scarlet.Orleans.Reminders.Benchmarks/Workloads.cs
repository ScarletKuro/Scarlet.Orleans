using System.Diagnostics;
using Orleans;
using Orleans.Runtime;

internal sealed record WorkloadResult(double InsertP50, double UpdateP50, double UpdateP99, double ReadP50, double ReadP99, double GrainReadP50, double RangeReadP50, double RemoveP50, double ConcurrentPerSecond, double ConcurrentP99);

internal static class Workloads
{
    public const int Operations = 1000;
    public const int Writers = 32;
    public static readonly TimeSpan Throughput = TimeSpan.FromSeconds(10);

    public static ReminderEntry Entry(string name) => new()
    {
        GrainId = GrainId.Create("bench-grain", name),
        ReminderName = "bench",
        // Keep reminder delivery outside the table benchmark, including long soak runs.
        StartAt = DateTime.UtcNow.AddDays(7),
        Period = TimeSpan.FromMinutes(1),
    };

    public static async Task UpsertAsync(IReminderTable table, ReminderEntry entry) =>
        entry.ETag = await table.UpsertRow(entry) ?? throw new InvalidOperationException("Upsert returned no ETag.");

    public static async Task RemoveAsync(IReminderTable table, ReminderEntry entry)
    {
        if (!await table.RemoveRow(entry.GrainId, entry.ReminderName, entry.ETag ?? throw new InvalidOperationException("Reminder has no ETag.")))
        {
            throw new InvalidOperationException("Remove rejected the current ETag.");
        }
    }

    public static async Task<WorkloadResult> RunAsync(IReminderTable table)
    {
        for (var i = 0; i < 100; i++)
        {
            await UpsertAsync(table, Entry($"warmup-{i}"));
        }

        await table.TestOnlyClearTable();
        var entries = Enumerable.Range(0, Operations).Select(i => Entry($"insert-{i}")).ToArray();
        var inserts = await MeasureAsync(i => UpsertAsync(table, entries[i]));
        var entry = entries[0];
        var updates = await MeasureAsync(_ => UpsertAsync(table, entry));
        var reads = await MeasureAsync(async _ =>
        {
            if (await table.ReadRow(entry.GrainId, entry.ReminderName) is null)
            {
                throw new InvalidOperationException("Read returned no reminder.");
            }
        });
        var grainReads = await MeasureAsync(_ => table.ReadRows(entry.GrainId));
        var rangeReads = await MeasureAsync(_ => table.ReadRows(0, uint.MaxValue / 4));
        var removes = await MeasureAsync(i => RemoveAsync(table, entries[i]));

        var latencies = new List<double>[Writers];
        var stop = Stopwatch.StartNew();
        // SQLite's async methods can complete synchronously; schedule workers independently so all writers run.
        await Task.WhenAll(Enumerable.Range(0, Writers).Select(writer => Task.Run(async () =>
        {
            var own = new List<double>();
            latencies[writer] = own;
            var reminder = Entry($"concurrent-{writer}");
            while (stop.Elapsed < Throughput)
            {
                var started = Stopwatch.GetTimestamp();
                await UpsertAsync(table, reminder);
                own.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            }
        })));
        var concurrent = latencies.SelectMany(l => l).Order().ToArray();
        return new WorkloadResult(Percentile(inserts, 0.5), Percentile(updates, 0.5), Percentile(updates, 0.99),
            Percentile(reads, 0.5), Percentile(reads, 0.99), Percentile(grainReads, 0.5), Percentile(rangeReads, 0.5),
            Percentile(removes, 0.5), concurrent.Length / stop.Elapsed.TotalSeconds, Percentile(concurrent, 0.99));
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
