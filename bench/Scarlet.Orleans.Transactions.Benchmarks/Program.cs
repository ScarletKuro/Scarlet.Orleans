using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;
using Orleans.Transactions;
using Scarlet.Orleans.Transactions.Benchmarks;

// The storage of transactional state compared through real transactions in one silo per engine: Scarlet's dedicated
// stores, and Orleans' grain storage wrapper over Scarlet and over the in-box ADO.NET persistence. Server engines come
// from the SCARLET_TEST_* administrative connection strings; SQLite always runs.
var output = args.FirstOrDefault(a => a.EndsWith(".md", StringComparison.OrdinalIgnoreCase)) ?? "transactions-results.md";
var engines = new List<Engine> { Engine.Sqlite() };
foreach (var (name, variable) in new[] { ("PostgreSql", "SCARLET_TEST_POSTGRESQL"), ("SqlServer", "SCARLET_TEST_SQLSERVER"), ("MySql", "SCARLET_TEST_MYSQL") })
{
    if (Environment.GetEnvironmentVariable(variable) is { Length: > 0 } admin)
    {
        engines.Add(await Engine.CreateAsync(name, admin));
    }
}

using var bytes = new ByteMeter();
var report = new StringBuilder()
    .AppendLine(CultureInfo.InvariantCulture, $"# Transactional state benchmarks, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
    .AppendLine()
    .AppendLine(CultureInfo.InvariantCulture, $"{Environment.ProcessorCount} logical processors, .NET {Environment.Version}, {Storages.StateSize} B account state. Single account: {Workloads.Sequential} sequential transactions on one account, each prepared and committed in one store. Hot transfers: {Workloads.Callers} callers each moving money from its own account into one shared account for {Workloads.Duration.TotalSeconds:0} s; each transfer is one transaction over both, so on the shared account prepared versions wait for their transaction managers and pile up. Spread transfers: each caller between its own two accounts. Callers start transfers for the stated time; throughput counts those that commit over the time until the last one finished. Latencies in milliseconds. Bytes are the serialized bytes the storage writes; state copies per store are those bytes over the state size: the dedicated stores write each prepared version once, the wrapper rewrites the committed state and every pending version. The in-box provider does not report its bytes.")
    .AppendLine()
    .AppendLine("| Engine | Provider | Single p50 | Single p99 | Single bytes/tx | Hot transfers/s | Hot p99 | Hot bytes/transfer | Hot state copies/store | Spread transfers/s | Spread p99 | Aborted |")
    .AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
try
{
    foreach (var engine in engines)
    {
        await engine.InstallInBoxSchemaAsync("Persistence");
        await using var cluster = engine.CreateCluster();
        await cluster.DeployAsync();
        foreach (var provider in engine.TransactionProviders)
        {
            Console.WriteLine($"{engine.Name} / {provider}...");
            var result = await Workloads.RunAsync(cluster.Client, cluster.Client.ServiceProvider.GetRequiredService<ITransactionClient>(), provider, bytes);
            report.AppendLine(CultureInfo.InvariantCulture,
                $"| {engine.Name} | {provider} | {result.SingleP50:0.00} | {result.SingleP99:0.00} | {Number(result.SingleBytes, "0")} | {result.HotPerSecond:0} | {result.HotP99:0.00} | {Number(result.HotBytes, "0")} | {Number(result.HotCopiesPerStore, "0.0")} | {result.SpreadPerSecond:0} | {result.SpreadP99:0.00} | {result.Aborted} |");
        }

        // Preserve completed engines if a later setup or workload fails.
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

static string Number(double? value, string format) => value is { } v ? v.ToString(format, CultureInfo.InvariantCulture) : "n/a";

internal sealed class BenchDbContext(DbContextOptions<BenchDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansTransactionsModel();
}

internal sealed record WorkloadResult(
    double SingleP50, double SingleP99, double? SingleBytes,
    double HotPerSecond, double HotP99, double? HotBytes, double? HotCopiesPerStore,
    double SpreadPerSecond, double SpreadP99, int Aborted);

internal static class Workloads
{
    public const int Sequential = 300;
    public const int Callers = 16;
    public static readonly TimeSpan Duration = TimeSpan.FromSeconds(10);

    public static async Task<WorkloadResult> RunAsync(IGrainFactory grains, ITransactionClient transactions, string provider, ByteMeter bytes)
    {
        IAccountGrain Account(long key) => grains.GetGrain<IAccountGrain>(key, Storages.GrainClass(provider));

        var single = Account(1);
        for (var i = 0; i < 20; i++)
        {
            await single.AddAsync(1);
        }

        bytes.Reset();
        var samples = new double[Sequential];
        for (var i = 0; i < samples.Length; i++)
        {
            var started = Stopwatch.GetTimestamp();
            await single.AddAsync(1);
            samples[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        Array.Sort(samples);
        var singleBytes = bytes.PerTransaction(provider, Sequential);
        if (await single.GetAsync() != 20 + Sequential)
        {
            throw new InvalidOperationException($"{provider} lost a committed transaction.");
        }

        bytes.Reset();
        var hot = await RunTransfersAsync(transactions, caller => (Account(1000 + caller), Account(2)));
        var hotBytes = bytes.PerTransaction(provider, hot.Count);
        var hotCopies = bytes.PerStore(provider) / Storages.StateSize;
        await VerifyConservedAsync(provider, [Account(2), .. Enumerable.Range(0, Callers).Select(caller => Account(1000 + caller))]);

        var spread = await RunTransfersAsync(transactions, caller => (Account(100 + (2 * caller)), Account(101 + (2 * caller))));

        return new WorkloadResult(
            Percentile(samples, 0.5), Percentile(samples, 0.99), singleBytes,
            hot.Count / hot.Elapsed.TotalSeconds, Percentile(hot.Latencies, 0.99), hotBytes, hotCopies,
            spread.Count / spread.Elapsed.TotalSeconds, Percentile(spread.Latencies, 0.99), hot.Aborted + spread.Aborted);
    }

    /// <summary>
    /// Runs <see cref="Callers"/> callers for <see cref="Duration"/>, each moving one unit per transaction between the two
    /// accounts <paramref name="accounts"/> gives it, and times the transfers that commit. Aborted ones, which contention
    /// makes normal, are counted, and the benchmark adds no retries.
    /// </summary>
    private static async Task<(int Count, double[] Latencies, int Aborted, TimeSpan Elapsed)> RunTransfersAsync(ITransactionClient transactions, Func<int, (IAccountGrain From, IAccountGrain To)> accounts)
    {
        var latencies = new ConcurrentBag<double>();
        var aborted = 0;
        var stop = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, Callers).Select(caller => Task.Run(async () =>
        {
            var (from, to) = accounts(caller);
            while (stop.Elapsed < Duration)
            {
                var started = Stopwatch.GetTimestamp();
                try
                {
                    await transactions.RunTransaction(TransactionOption.Create, () => Task.WhenAll(from.ChangeAsync(-1), to.ChangeAsync(1)));
                    latencies.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
                }
                catch (Exception)
                {
                    Interlocked.Increment(ref aborted);
                }
            }
        })));
        // Callers stop starting transfers at the deadline, but those in flight still finish and count, so the rate is over
        // the time until the last one finished.
        var elapsed = stop.Elapsed;
        var sorted = latencies.Order().ToArray();
        return (sorted.Length, sorted, aborted, elapsed);
    }

    /// <summary>
    /// Transfers only move money, so the accounts must still sum to zero: a lost or doubled half of a transfer
    /// would show here.
    /// </summary>
    private static async Task VerifyConservedAsync(string provider, IAccountGrain[] accounts)
    {
        if ((await Task.WhenAll(accounts.Select(a => a.GetAsync()))).Sum() != 0)
        {
            throw new InvalidOperationException($"{provider} did not conserve the transferred total.");
        }
    }

    private static double Percentile(double[] sorted, double percentile) =>
        sorted.Length == 0 ? 0 : sorted[(int)Math.Min(sorted.Length - 1, Math.Floor(percentile * sorted.Length))];
}

/// <summary>
/// Sums the bytes the Scarlet storages report writing, per storage name: the transactions payload size for the dedicated
/// stores, and the persistence payload size for grain storage behind Orleans' wrapper.
/// </summary>
internal sealed class ByteMeter : IDisposable
{
    private readonly MeterListener listener = new();
    private readonly ConcurrentDictionary<string, long> totals = new();
    private readonly ConcurrentDictionary<string, long> stores = new();

    public ByteMeter()
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Scarlet.Orleans" && instrument.Name is "scarlet.orleans.transactions.payload.size" or "scarlet.orleans.persistence.payload.size")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "orleans.storage.name" && tag.Value is string name)
                {
                    totals.AddOrUpdate(name, value, (_, total) => total + value);
                    stores.AddOrUpdate(name, 1, (_, count) => count + 1);
                }
            }
        });
        listener.Start();
    }

    public void Reset()
    {
        totals.Clear();
        stores.Clear();
    }

    public double? PerTransaction(string provider, int transactions) =>
        totals.TryGetValue(provider, out var total) && transactions > 0 ? (double)total / transactions : null;

    public double? PerStore(string provider) =>
        totals.TryGetValue(provider, out var total) && stores.TryGetValue(provider, out var count) ? (double)total / count : null;

    public void Dispose() => listener.Dispose();
}

internal sealed partial class Engine
{
    /// <summary>The in-box persistence runs on server engines only.</summary>
    public IReadOnlyList<string> TransactionProviders => Name == "Sqlite"
        ? [Storages.ScarletAdoNet, Storages.ScarletEntityFrameworkCore, Storages.ScarletWrapper]
        : [Storages.ScarletAdoNet, Storages.ScarletEntityFrameworkCore, Storages.ScarletWrapper, Storages.InBoxWrapper];

    public InProcessTestCluster CreateCluster()
    {
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureHost(host => host.Logging.SetMinimumLevel(LogLevel.Warning));
        builder.ConfigureSilo((siloOptions, silo) =>
        {
            silo.UseTransactions();
            silo.AddScarletAdoNetTransactionalStateStorage(Storages.ScarletAdoNet, options =>
            {
                options.DataSource = DataSource;
                options.SchemaMode = Scarlet.Orleans.Transactions.AdoNet.SchemaMode.Apply;
            });

            // Shares the tables the ADO.NET provider creates; the EF Core flavour never changes the schema.
            silo.Services.AddDbContextFactory<BenchDbContext>(options => _ = Name switch
            {
                "Sqlite" => options.UseSqlite(ConnectionString),
                "PostgreSql" => options.UseNpgsql(ConnectionString),
                "SqlServer" => options.UseSqlServer(ConnectionString),
                _ => options.UseMySql(ConnectionString, ServerVersion.AutoDetect(ConnectionString)),
            });
            silo.AddScarletEntityFrameworkCoreTransactionalStateStorage<BenchDbContext>(Storages.ScarletEntityFrameworkCore, options =>
                options.SchemaMode = Scarlet.Orleans.Transactions.EntityFrameworkCore.SchemaMode.None);

            silo.AddScarletAdoNetGrainStorage(Storages.ScarletWrapper, options =>
            {
                options.DataSource = DataSource;
                options.SchemaMode = Scarlet.Orleans.Persistence.AdoNet.SchemaMode.Apply;
            });

            if (Name != "Sqlite")
            {
                silo.AddAdoNetGrainStorage(Storages.InBoxWrapper, options =>
                {
                    options.Invariant = Name switch
                    {
                        "PostgreSql" => "Npgsql",
                        "SqlServer" => "Microsoft.Data.SqlClient",
                        _ => "MySql.Data.MySqlConnector",
                    };
                    options.ConnectionString = ConnectionString;
                });
            }
        });
        builder.ConfigureClient(client => client.UseTransactions());
        return builder.Build();
    }
}
