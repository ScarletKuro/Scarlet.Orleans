using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.GrainDirectory;
using Orleans.Hosting;
using Orleans.TestingHost;

var soak = Array.IndexOf(args, "--soak") is var at and >= 0 ? TimeSpan.FromMinutes(double.Parse(args[at + 1], CultureInfo.InvariantCulture)) : (TimeSpan?)null;
var output = args.FirstOrDefault(a => a.EndsWith(".md", StringComparison.OrdinalIgnoreCase)) ?? "grain-directory-results.md";
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

    if (soak is { } duration)
    {
        await Soak.RunAsync(engines, duration, output);
        return;
    }

    var report = new StringBuilder()
        .AppendLine(CultureInfo.InvariantCulture, $"# Grain directory benchmarks, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
        .AppendLine()
        .AppendLine(CultureInfo.InvariantCulture, $"{Environment.ProcessorCount} logical processors, .NET {Environment.Version}, {Workloads.Operations} operations per sequential workload, {Workloads.Writers} concurrent workers for {Workloads.Throughput.TotalSeconds:0} s. A concurrent cycle registers, looks up and unregisters one activation. Latencies in milliseconds; cleanup is one call removing {Workloads.Operations} registrations.")
        .AppendLine()
        .AppendLine("| Engine | Provider | Register p50 | Register p99 | Existing register p50 | Existing register p99 | Lookup p50 | Lookup p99 | Missing lookup p50 | Unregister p50 | Cleanup 1000 ms | Concurrent cycles/s | Concurrent p99 | Concurrent errors |")
        .AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    var errors = new List<string>();
    foreach (var engine in engines)
    {
        await engine.InstallInBoxSchemaAsync("GrainDirectory");
        await using var cluster = engine.CreateCluster();
        await cluster.DeployAsync();
        foreach (var provider in engine.Providers)
        {
            var directory = cluster.Silos[0].ServiceProvider.GetRequiredKeyedService<IGrainDirectory>(provider);
            Console.WriteLine($"{engine.Name} / {provider}...");
            var result = await Workloads.RunAsync(directory);
            report.AppendLine(CultureInfo.InvariantCulture,
                $"| {engine.Name} | {provider} | {result.RegisterP50:0.00} | {result.RegisterP99:0.00} | {result.ExistingP50:0.00} | {result.ExistingP99:0.00} | {result.LookupP50:0.00} | {result.LookupP99:0.00} | {result.MissingP50:0.00} | {result.UnregisterP50:0.00} | {result.CleanupMilliseconds:0.00} | {result.ConcurrentPerSecond:0} | {result.ConcurrentP99:0.00} | {result.ConcurrentErrors} |");
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
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansGrainDirectoryModel();
}

internal sealed partial class Engine
{
    public InProcessTestCluster CreateCluster()
    {
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureHost(host => host.Logging.SetMinimumLevel(LogLevel.Warning)
            // Existing-registration workloads deliberately hit duplicate keys. Avoid logging each expected conflict.
            .AddFilter("Microsoft.EntityFrameworkCore.Database.Command", LogLevel.None)
            .AddFilter("Microsoft.EntityFrameworkCore.Update", LogLevel.None));
        builder.ConfigureSilo((siloOptions, silo) =>
        {
            silo.AddScarletAdoNetGrainDirectory(ScarletAdoNet, options =>
            {
                options.DataSource = DataSource;
                options.SchemaMode = Scarlet.Orleans.GrainDirectory.AdoNet.SchemaMode.Apply;
            });
            silo.Services.AddDbContextFactory<BenchDbContext>(options => _ = Name switch
            {
                "Sqlite" => options.UseSqlite(ConnectionString),
                "PostgreSql" => options.UseNpgsql(ConnectionString),
                "SqlServer" => options.UseSqlServer(ConnectionString),
                _ => options.UseMySql(ConnectionString, ServerVersion.AutoDetect(ConnectionString)),
            });
            // The ADO.NET directory creates the shared Scarlet schema during silo startup.
            silo.AddScarletEntityFrameworkCoreGrainDirectory<BenchDbContext>(ScarletEntityFrameworkCore, options =>
                options.SchemaMode = Scarlet.Orleans.GrainDirectory.EntityFrameworkCore.SchemaMode.None);
            if (Providers.Contains(InBox))
            {
                silo.AddAdoNetGrainDirectory(InBox, options =>
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
        return builder.Build();
    }
}
