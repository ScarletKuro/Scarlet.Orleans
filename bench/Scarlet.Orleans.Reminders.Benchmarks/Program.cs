using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.TestingHost;

var soak = Array.IndexOf(args, "--soak") is var at and >= 0 ? TimeSpan.FromMinutes(double.Parse(args[at + 1], CultureInfo.InvariantCulture)) : (TimeSpan?)null;
var output = args.FirstOrDefault(a => a.EndsWith(".md", StringComparison.OrdinalIgnoreCase)) ?? "reminder-results.md";
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
        .AppendLine(CultureInfo.InvariantCulture, $"# Reminder table benchmarks, {DateTimeOffset.UtcNow:yyyy-MM-dd}")
        .AppendLine()
        .AppendLine(CultureInfo.InvariantCulture, $"{Environment.ProcessorCount} logical processors, .NET {Environment.Version}, {Workloads.Writers} concurrent writers for {Workloads.Throughput.TotalSeconds:0} s. Latencies in milliseconds. Range reads cover a quarter of the hash ring over {Workloads.Operations} reminders.")
        .AppendLine()
        .AppendLine("| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Grain read p50 | Range read p50 | Remove p50 | Concurrent upserts/s | Concurrent p99 |")
        .AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    foreach (var engine in engines)
    {
        await engine.InstallInBoxSchemaAsync("Reminders");
        foreach (var provider in engine.Providers)
        {
            await using var cluster = engine.CreateCluster(provider);
            await cluster.DeployAsync();
            var table = cluster.Silos[0].ServiceProvider.GetRequiredService<IReminderTable>();
            Console.WriteLine($"{engine.Name} / {provider}...");
            var result = await Workloads.RunAsync(table);
            report.AppendLine(CultureInfo.InvariantCulture,
                $"| {engine.Name} | {provider} | {result.InsertP50:0.00} | {result.UpdateP50:0.00} | {result.UpdateP99:0.00} | {result.ReadP50:0.00} | {result.ReadP99:0.00} | {result.GrainReadP50:0.00} | {result.RangeReadP50:0.00} | {result.RemoveP50:0.00} | {result.ConcurrentPerSecond:0} | {result.ConcurrentP99:0.00} |");
            await table.TestOnlyClearTable();
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
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansRemindersModel();
}

internal sealed partial class Engine
{
    public InProcessTestCluster CreateCluster(string provider)
    {
        var builder = new InProcessTestClusterBuilder(1);
        builder.ConfigureHost(host => host.Logging.SetMinimumLevel(LogLevel.Warning));
        builder.ConfigureSilo((siloOptions, silo) =>
        {
            switch (provider)
            {
                case ScarletAdoNet:
                    silo.UseScarletAdoNetReminderService(options =>
                    {
                        options.DataSource = DataSource;
                        options.SchemaMode = Scarlet.Orleans.Reminders.AdoNet.SchemaMode.Apply;
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
                    // ADO.NET creates the shared Scarlet schema before this provider runs.
                    silo.UseScarletEntityFrameworkCoreReminderService<BenchDbContext>(options =>
                        options.SchemaMode = Scarlet.Orleans.Reminders.EntityFrameworkCore.SchemaMode.None);
                    break;
                default:
                    silo.UseAdoNetReminderService(options =>
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
