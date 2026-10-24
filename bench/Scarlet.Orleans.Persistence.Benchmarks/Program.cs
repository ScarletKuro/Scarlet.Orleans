using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Npgsql;
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

/// <summary>
/// A freshly created benchmark database on one engine, with the providers to compare on it.
/// </summary>
internal sealed class Engine : IAsyncDisposable
{
    public const string ScarletAdoNet = "Scarlet ADO.NET";
    public const string ScarletEntityFrameworkCore = "Scarlet EF Core";
    public const string InBox = "Orleans ADO.NET";

    private readonly Func<Task> drop;

    private Engine(string name, string connectionString, DbDataSource dataSource, Func<Task> drop)
    {
        Name = name;
        ConnectionString = connectionString;
        DataSource = dataSource;
        this.drop = drop;
    }

    public string Name { get; }

    public string ConnectionString { get; }

    public DbDataSource DataSource { get; }

    /// <summary>The in-box provider supports SQLite through System.Data.SQLite only, so it is compared on server engines.</summary>
    public IReadOnlyList<string> Providers => Name == "Sqlite" ? [ScarletAdoNet, ScarletEntityFrameworkCore] : [ScarletAdoNet, ScarletEntityFrameworkCore, InBox];

    public static Engine Sqlite()
    {
        var path = Path.Combine(Path.GetTempPath(), $"scarlet-bench-{Guid.NewGuid():N}.db");
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path }.ToString();
        return new Engine("Sqlite", connectionString, SqliteFactory.Instance.CreateDataSource(connectionString), () =>
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in new[] { path, path + "-wal", path + "-shm" })
            {
                File.Delete(file);
            }

            return Task.CompletedTask;
        });
    }

    public static async Task<Engine> CreateAsync(string name, string admin)
    {
        var database = $"scarlet_bench_{Guid.NewGuid():N}"[..24];
        switch (name)
        {
            case "PostgreSql":
            {
                await ExecuteAsync(NpgsqlDataSource.Create(admin), $"CREATE DATABASE \"{database}\"");
                var cs = new NpgsqlConnectionStringBuilder(admin) { Database = database }.ToString();
                return new Engine(name, cs, NpgsqlDataSource.Create(cs), () => ExecuteAsync(NpgsqlDataSource.Create(admin), $"DROP DATABASE IF EXISTS \"{database}\" WITH (FORCE)"));
            }

            case "SqlServer":
            {
                await ExecuteAsync(SqlClientFactory.Instance.CreateDataSource(admin), $"CREATE DATABASE [{database}]");
                var cs = new SqlConnectionStringBuilder(admin) { InitialCatalog = database }.ToString();
                return new Engine(name, cs, SqlClientFactory.Instance.CreateDataSource(cs), () =>
                {
                    SqlConnection.ClearAllPools();
                    return ExecuteAsync(SqlClientFactory.Instance.CreateDataSource(admin), $"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]");
                });
            }

            default:
            {
                await ExecuteAsync(new MySqlDataSource(admin), $"CREATE DATABASE `{database}`");
                var cs = new MySqlConnectionStringBuilder(admin) { Database = database }.ToString();
                return new Engine(name, cs, new MySqlDataSource(cs), () =>
                {
                    MySqlConnection.ClearAllPools();
                    return ExecuteAsync(new MySqlDataSource(admin), $"DROP DATABASE IF EXISTS `{database}`");
                });
            }
        }
    }

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

    /// <summary>
    /// Runs the in-box provider's scripts, splitting MySQL's at its <c>DELIMITER</c> directives as the <c>mysql</c> client does.
    /// </summary>
    public async Task InstallInBoxSchemaAsync()
    {
        if (!Providers.Contains(InBox))
        {
            return;
        }

        var prefix = Name switch { "PostgreSql" => "PostgreSQL", "SqlServer" => "SQLServer", _ => "MySQL" };
        foreach (var script in new[] { $"{prefix}-Main.sql", $"{prefix}-Persistence.sql" })
        {
            var text = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "InBoxScripts", script));
            foreach (var statement in Name == "MySql" ? SplitMySql(text) : [text])
            {
                await using var command = DataSource.CreateCommand(statement);
                await command.ExecuteNonQueryAsync();
            }
        }
    }

    /// <summary>The connections the database reports for this benchmark database, including the one counting.</summary>
    public async Task<int> CountConnectionsAsync()
    {
        var sql = Name switch
        {
            "PostgreSql" => "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()",
            "SqlServer" => "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE database_id = DB_ID() AND is_user_process = 1",
            "MySql" => "SELECT COUNT(*) FROM information_schema.processlist WHERE db = DATABASE()",
            _ => null,
        };
        if (sql is null)
        {
            return 0;
        }

        await using var command = DataSource.CreateCommand(sql);
        return Convert.ToInt32(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await drop();
    }

    private static IEnumerable<string> SplitMySql(string script)
    {
        var delimiter = ";";
        var current = new StringBuilder();
        foreach (var line in script.ReplaceLineEndings("\n").Split('\n'))
        {
            if (line.TrimStart().StartsWith("DELIMITER ", StringComparison.OrdinalIgnoreCase))
            {
                delimiter = line.Trim()["DELIMITER ".Length..].Trim();
                continue;
            }

            current.AppendLine(line);
            if (line.TrimEnd().EndsWith(delimiter, StringComparison.Ordinal))
            {
                var statement = current.ToString().Trim();
                current.Clear();
                statement = statement[..^delimiter.Length].Trim();
                if (statement.Length > 0)
                {
                    yield return statement;
                }
            }
        }

        if (current.ToString().Trim() is { Length: > 0 } rest)
        {
            yield return rest;
        }
    }

    private static async Task ExecuteAsync(DbDataSource dataSource, string sql)
    {
        await using (dataSource)
        {
            await using var command = dataSource.CreateCommand(sql);
            await command.ExecuteNonQueryAsync();
        }
    }
}
