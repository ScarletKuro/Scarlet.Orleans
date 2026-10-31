using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;

var output = args.FirstOrDefault(a => a.EndsWith(".md", StringComparison.OrdinalIgnoreCase)) ?? "clustering-results.md";
var engines = new List<Engine>();
try
{
    engines.Add(Engine.Sqlite());
    foreach (var (name, variable) in new[] { ("PostgreSql", "SCARLET_TEST_POSTGRESQL"), ("SqlServer", "SCARLET_TEST_SQLSERVER"), ("MySql", "SCARLET_TEST_MYSQL") })
    {
        if (Environment.GetEnvironmentVariable(variable) is { Length: > 0 } admin)
        {
            engines.Add(await Engine.CreateAsync(name, admin));
        }
    }

    var report = new StringBuilder()
        .AppendLine(CultureInfo.InvariantCulture, $"# Clustering benchmarks, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
        .AppendLine()
        .AppendLine(CultureInfo.InvariantCulture, $"{Environment.ProcessorCount} logical processors, .NET {Environment.Version}. A table of {Workloads.Silos} silos; {Workloads.Operations} operations per sequential workload; {Workloads.Writers} concurrent silos for {Workloads.Throughput.TotalSeconds:0} s per concurrent workload. Latencies in milliseconds; cleanup is one call removing {Workloads.Silos} dead silos. A contended update reads the table and swaps the silo's row with the next version, as silos do; a conflict is another silo's update coming first.")
        .AppendLine()
        .AppendLine("| Engine | Provider | Insert p50 | Read all p50 | Read all p99 | Read row p50 | Update p50 | Update p99 | Heartbeat p50 | Heartbeat p99 | Cleanup ms | Concurrent heartbeats/s | Concurrent heartbeat p99 | Contended updates/s | Conflicts/s | Errors |")
        .AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    var errors = new List<string>();
    foreach (var engine in engines)
    {
        await engine.InstallInBoxSchemaAsync("Clustering");
        foreach (var provider in engine.Providers)
        {
            Console.WriteLine($"{engine.Name} / {provider}...");
            using var host = engine.CreateHost(provider);
            var result = await Workloads.RunAsync(host.Services.GetRequiredService<IMembershipTable>());
            report.AppendLine(CultureInfo.InvariantCulture,
                $"| {engine.Name} | {provider} | {result.InsertP50:0.00} | {result.ReadAllP50:0.00} | {result.ReadAllP99:0.00} | {result.ReadRowP50:0.00} | {result.UpdateP50:0.00} | {result.UpdateP99:0.00} | {result.HeartbeatP50:0.00} | {result.HeartbeatP99:0.00} | {result.CleanupMilliseconds:0.00} | {result.HeartbeatsPerSecond:0} | {result.ConcurrentHeartbeatP99:0.00} | {result.UpdatesPerSecond:0} | {result.ConflictsPerSecond:0} | {result.Errors} |");
            if (result.FirstError is { } error)
            {
                errors.Add($"- {engine.Name} / {provider}: {error}");
            }
        }

        // Preserve completed engines if a later setup or workload fails.
        await File.WriteAllTextAsync(output, report.ToString());
    }

    if (errors.Count > 0)
    {
        report.AppendLine().AppendLine("## First concurrent errors").AppendLine();
        foreach (var error in errors)
        {
            report.AppendLine(error);
        }
    }

    await File.WriteAllTextAsync(output, report.ToString());
    Console.WriteLine(report);
}
finally
{
    foreach (var engine in engines)
    {
        await engine.DisposeAsync();
    }
}

internal sealed class BenchDbContext(DbContextOptions<BenchDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansClusteringModel();
}

internal sealed partial class Engine
{
    /// <summary>
    /// A silo host configured with one provider's clustering, built but not started: a silo has a single membership
    /// table, so each provider gets its own host, and the benchmark drives the table directly.
    /// </summary>
    public IHost CreateHost(string provider)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.UseOrleans(silo =>
        {
            silo.Configure<ClusterOptions>(options =>
            {
                options.ClusterId = $"bench-{Guid.NewGuid():N}";
                options.ServiceId = "scarlet-clustering-bench";
            });
            switch (provider)
            {
                case ScarletAdoNet:
                    silo.UseScarletAdoNetClustering(options =>
                    {
                        options.DataSource = DataSource;
                        options.SchemaMode = Scarlet.Orleans.Clustering.AdoNet.SchemaMode.Apply;
                    });
                    break;

                case ScarletEntityFrameworkCore:
                    silo.Services.AddDbContextFactory<BenchDbContext>(options => _ = Name switch
                    {
                        "Sqlite" => options.UseSqlite(ConnectionString),
                        "PostgreSql" => options.UseNpgsql(ConnectionString),
                        "SqlServer" => options.UseSqlServer(ConnectionString),
                        _ => options.UseMySql(ConnectionString, ServerVersion.AutoDetect(ConnectionString)),
                    });
                    // The ADO.NET provider, run first, creates the shared Scarlet schema.
                    silo.UseScarletEntityFrameworkCoreClustering<BenchDbContext>(options =>
                        options.SchemaMode = Scarlet.Orleans.Clustering.EntityFrameworkCore.SchemaMode.None);
                    break;

                default:
                    silo.UseAdoNetClustering(options =>
                    {
                        options.Invariant = Name switch
                        {
                            "PostgreSql" => "Npgsql",
                            "SqlServer" => "Microsoft.Data.SqlClient",
                            _ => "MySql.Data.MySqlConnector",
                        };
                        options.ConnectionString = ConnectionString;
                    });
                    break;
            }
        });
        return builder.Build();
    }
}
