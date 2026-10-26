using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;
using Scarlet.Orleans.Persistence.AdoNet;

// Grain storage providers compared through IGrainStorage, on the same database and silo, with the default Orleans
// serializer. Server engines come from the SCARLET_TEST_* administrative connection strings; SQLite always runs.
var soak = Array.IndexOf(args, "--soak") is var at and >= 0 ? TimeSpan.FromMinutes(double.Parse(args[at + 1], CultureInfo.InvariantCulture)) : (TimeSpan?)null;
var output = args.FirstOrDefault(a => a.EndsWith(".md", StringComparison.OrdinalIgnoreCase)) ?? "results.md";
var engines = soak is null ? new List<Engine> { Engine.Sqlite() } : [];
foreach (var (name, variable) in new[] { ("PostgreSql", "SCARLET_TEST_POSTGRESQL"), ("SqlServer", "SCARLET_TEST_SQLSERVER"), ("MySql", "SCARLET_TEST_MYSQL") })
{
    if (Environment.GetEnvironmentVariable(variable) is { Length: > 0 } admin)
    {
        engines.Add(await Engine.CreateAsync(name, admin));
    }
}

if (soak is { } soakDuration)
{
    try
    {
        await Soak.RunAsync(engines, soakDuration, output);
    }
    finally
    {
        foreach (var engine in engines)
        {
            await engine.DisposeAsync();
        }
    }

    return;
}

var report = new StringBuilder()
    .AppendLine(CultureInfo.InvariantCulture, $"# Grain storage benchmarks, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
    .AppendLine()
    .AppendLine(CultureInfo.InvariantCulture, $"{Environment.ProcessorCount} logical processors, .NET {Environment.Version}, {Workloads.PayloadSize} B state, {Workloads.Writers} concurrent writers for {Workloads.Throughput.TotalSeconds:0} s. Latencies in milliseconds.")
    .AppendLine()
    .AppendLine("| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Concurrent writes/s | Concurrent p99 |")
    .AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|");

foreach (var engine in engines)
{
    try
    {
        await engine.InstallInBoxSchemaAsync();
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureSilo((_, silo) => engine.AddProviders(silo));
        builder.ConfigureHost(host => host.Logging.SetMinimumLevel(LogLevel.Warning));
        await using var cluster = builder.Build();
        await cluster.DeployAsync();
        foreach (var provider in engine.Providers)
        {
            var storage = cluster.Silos[0].ServiceProvider.GetRequiredKeyedService<IGrainStorage>(provider);
            Console.WriteLine($"{engine.Name} / {provider}...");
            var result = await Workloads.RunAsync(storage, provider);
            report.AppendLine(CultureInfo.InvariantCulture,
                $"| {engine.Name} | {provider} | {result.InsertP50:0.00} | {result.UpdateP50:0.00} | {result.UpdateP99:0.00} | {result.ReadP50:0.00} | {result.ReadP99:0.00} | {result.ConcurrentPerSecond:0} | {result.ConcurrentP99:0.00} |");
        }
    }
    finally
    {
        await engine.DisposeAsync();
    }
}

await File.WriteAllTextAsync(output, report.ToString());
Console.WriteLine();
Console.WriteLine(report);

[GenerateSerializer]
internal sealed class BenchState
{
    [Id(0)]
    public byte[] Data { get; set; } = [];
}

internal sealed class BenchDbContext(DbContextOptions<BenchDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansPersistenceModel();
}

internal sealed record WorkloadResult(double InsertP50, double UpdateP50, double UpdateP99, double ReadP50, double ReadP99, double ConcurrentPerSecond, double ConcurrentP99);

internal static class Workloads
{
    public const int PayloadSize = 1024;
    public const int Writers = 32;
    public static readonly TimeSpan Throughput = TimeSpan.FromSeconds(10);
    private const int Operations = 1000;
    private const int Warmup = 100;
    private const string StateName = "bench";

    public static async Task<WorkloadResult> RunAsync(IGrainStorage storage, string provider)
    {
        var payload = new byte[PayloadSize];
        Random.Shared.NextBytes(payload);
        GrainId Grain(string name) => GrainId.Create("bench-grain", $"{provider}-{name}-{Guid.NewGuid():N}");

        for (var i = 0; i < Warmup; i++)
        {
            await storage.WriteStateAsync(StateName, Grain("warmup"), new GrainState<BenchState> { State = new BenchState { Data = payload } });
        }

        var inserts = await MeasureAsync(Operations, _ => storage.WriteStateAsync(StateName, Grain("insert"), new GrainState<BenchState> { State = new BenchState { Data = payload } }));

        var updated = Grain("update");
        var state = new GrainState<BenchState> { State = new BenchState { Data = payload } };
        await storage.WriteStateAsync(StateName, updated, state);
        var updates = await MeasureAsync(Operations, _ => storage.WriteStateAsync(StateName, updated, state));
        var reads = await MeasureAsync(Operations, _ => storage.ReadStateAsync(StateName, updated, new GrainState<BenchState>()));

        var latencies = new List<double>[Writers];
        var stop = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, Writers).Select(async writer =>
        {
            var own = new List<double>();
            latencies[writer] = own;
            var grain = Grain($"concurrent-{writer}");
            var writerState = new GrainState<BenchState> { State = new BenchState { Data = payload } };
            while (stop.Elapsed < Throughput)
            {
                var started = Stopwatch.GetTimestamp();
                await storage.WriteStateAsync(StateName, grain, writerState);
                own.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
            }
        }));
        var concurrent = latencies.SelectMany(l => l).Order().ToArray();

        return new WorkloadResult(
            Percentile(inserts, 0.50),
            Percentile(updates, 0.50),
            Percentile(updates, 0.99),
            Percentile(reads, 0.50),
            Percentile(reads, 0.99),
            concurrent.Length / stop.Elapsed.TotalSeconds,
            Percentile(concurrent, 0.99));
    }

    private static async Task<double[]> MeasureAsync(int count, Func<int, Task> operation)
    {
        var samples = new double[count];
        for (var i = 0; i < count; i++)
        {
            var started = Stopwatch.GetTimestamp();
            await operation(i);
            samples[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        Array.Sort(samples);
        return samples;
    }

    private static double Percentile(double[] sorted, double percentile) => sorted[(int)Math.Min(sorted.Length - 1, Math.Floor(percentile * sorted.Length))];
}

internal sealed partial class Engine
{
    public void AddProviders(ISiloBuilder silo)
    {
        silo.AddScarletAdoNetGrainStorage(ScarletAdoNet, options =>
        {
            options.DataSource = DataSource;
            options.SchemaMode = SchemaMode.Apply;
        });

        // Shares the tables the ADO.NET provider creates; the EF Core flavour never changes the schema.
        silo.Services.AddDbContextFactory<BenchDbContext>(options => _ = Name switch
        {
            "Sqlite" => options.UseSqlite(ConnectionString),
            "PostgreSql" => options.UseNpgsql(ConnectionString),
            "SqlServer" => options.UseSqlServer(ConnectionString),
            _ => options.UseMySql(ConnectionString, ServerVersion.AutoDetect(ConnectionString)),
        });
        silo.AddScarletEntityFrameworkCoreGrainStorage<BenchDbContext>(ScarletEntityFrameworkCore, options =>
            options.SchemaMode = Scarlet.Orleans.Persistence.EntityFrameworkCore.SchemaMode.None);

        if (Providers.Contains(InBox))
        {
            silo.AddAdoNetGrainStorage(InBox, options =>
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
    }
}
