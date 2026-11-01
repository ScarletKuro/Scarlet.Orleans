using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;

namespace Scarlet.Orleans.Tests;

/// <summary>
/// An empty database created for a test run and dropped afterwards.
/// </summary>
public sealed class TestDatabase(string engine, string driver, string connectionString, DbDataSource dataSource, Func<Task> drop) : IAsyncDisposable
{
    /// <summary>The engine name, as accepted by the <c>Engine</c> option.</summary>
    public string Engine { get; } = engine;

    /// <summary>The ADO.NET driver, where an engine has more than one.</summary>
    public string Driver { get; } = driver;

    public string ConnectionString { get; } = connectionString;

    public DbDataSource DataSource { get; } = dataSource;

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await drop();
    }
}

/// <summary>
/// Creates databases on one engine. SQLite always runs; server engines run when their environment variable holds an
/// administrative connection string, and their tests are skipped otherwise.
/// </summary>
public abstract class DatabaseFixture(string engine, string driver = TestDrivers.Default) : IAsyncLifetime
{
    private readonly List<TestDatabase> databases = [];
    private TestDatabase? database;

    public string Engine { get; } = engine;

    public string Driver { get; } = driver;

    /// <summary>The environment variable with the administrative connection string, or <see langword="null"/> for SQLite.</summary>
    public string? EnvironmentVariable => Engine == "Sqlite" ? null : $"SCARLET_TEST_{Engine.ToUpperInvariant()}";

    private string? AdminConnectionString => EnvironmentVariable is null ? null : Environment.GetEnvironmentVariable(EnvironmentVariable);

    /// <summary>
    /// Whether this engine runs: a server engine when its connection string is set, SQLite unless
    /// <c>SCARLET_TEST_SKIP_SQLITE</c> is <c>1</c>, which the server engines' CI jobs set, since the build job runs SQLite.
    /// </summary>
    public bool IsAvailable => EnvironmentVariable is null
        ? Environment.GetEnvironmentVariable("SCARLET_TEST_SKIP_SQLITE") != "1"
        : !string.IsNullOrWhiteSpace(AdminConnectionString);

    /// <summary>
    /// The database shared by the tests of a class. Tests isolate their rows with unique service IDs.
    /// </summary>
    public TestDatabase Database
    {
        get
        {
            Require();
            return database!;
        }
    }

    public async ValueTask InitializeAsync()
    {
        if (IsAvailable)
        {
            database = await CreateDatabaseAsync();
        }
    }

    /// <summary>
    /// Skips the calling test when this engine is not configured, unless <c>SCARLET_TEST_REQUIRE</c> lists the engine,
    /// in which case the test fails, so a CI job cannot pass by skipping its own engine.
    /// </summary>
    public void Require()
    {
        if (IsAvailable)
        {
            return;
        }

        var message = EnvironmentVariable is null
            ? "SCARLET_TEST_SKIP_SQLITE is set, so SQLite tests are skipped."
            : $"Set {EnvironmentVariable} to an administrative connection string to run {Engine} tests.";
        var required = (Environment.GetEnvironmentVariable("SCARLET_TEST_REQUIRE") ?? "").Split(',', StringSplitOptions.TrimEntries);
        if (required.Contains(Engine, StringComparer.OrdinalIgnoreCase))
        {
            Assert.Fail(message);
        }

        Assert.Skip(message);
    }

    /// <summary>
    /// Creates another empty database, dropped with the fixture.
    /// </summary>
    public async Task<TestDatabase> CreateDatabaseAsync()
    {
        Require();
        var name = $"scarlet_{Guid.NewGuid():N}"[..20];
        var created = Engine switch
        {
            "Sqlite" => CreateSqlite(name),
            "PostgreSql" => await CreatePostgreSqlAsync(name),
            "SqlServer" => await CreateSqlServerAsync(name),
#if !SCARLET_AOT_TESTS
            "MySql" or "MariaDb" when Driver == TestDrivers.MySqlData => await CreateMySqlDataAsync(name),
#endif
            "MySql" or "MariaDb" => await CreateMySqlAsync(name),
            _ => throw new NotSupportedException(Engine),
        };

        databases.Add(created);
        return created;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var created in databases)
        {
            await created.DisposeAsync();
        }

        GC.SuppressFinalize(this);
    }

    private TestDatabase CreateSqlite(string name)
    {
        var path = Path.Combine(Path.GetTempPath(), name + ".db");

        var connectionString = new SqliteConnectionStringBuilder { DataSource = path }.ToString();
        using (var connection = new SqliteConnection(connectionString))
        {
            connection.Open();
            using var wal = connection.CreateCommand();
            wal.CommandText = "PRAGMA journal_mode = WAL";
            wal.ExecuteNonQuery();
        }

        return new TestDatabase(Engine, Driver, connectionString, SqliteFactory.Instance.CreateDataSource(connectionString), () =>
        {
            SqliteConnection.ClearAllPools();
            foreach (var file in new[] { path, path + "-wal", path + "-shm" })
            {
                File.Delete(file);
            }

            return Task.CompletedTask;
        });
    }

    /// <summary>
    /// The connections each test database's pool may open. Tests run in parallel against one server, and PostgreSQL and
    /// MySQL accept 100 and 151 connections by default, which two pools at the drivers' default of 100 can exceed.
    /// </summary>
    private const int MaxPoolSize = 40;

    /// <summary>
    /// How long a pooled connection may sit idle. Drivers keep idle connections for minutes by default, so fixtures alive
    /// between their tests would hold connections the running tests need.
    /// </summary>
    private const int IdleSeconds = 2;

    /// <summary>
    /// Both MySQL drivers' tests share one server, whose default of 151 connections their pools exceed when a test project
    /// keeps several clusters alive. The test server is dedicated to the tests, so they raise its limit, never lower it.
    /// </summary>
    private const string RaiseMySqlConnectionLimit = "SET GLOBAL max_connections = CAST(GREATEST(@@GLOBAL.max_connections, 500) AS UNSIGNED);";

    private async Task<TestDatabase> CreatePostgreSqlAsync(string name)
    {
        await ExecuteAdminAsync(NpgsqlFactory.Instance, $"CREATE DATABASE \"{name}\"");
        var connectionString = new NpgsqlConnectionStringBuilder(AdminConnectionString) { Database = name, MaxPoolSize = MaxPoolSize, ConnectionIdleLifetime = IdleSeconds, ConnectionPruningInterval = 1 }.ToString();
        return new TestDatabase(Engine, Driver, connectionString, NpgsqlDataSource.Create(connectionString),
            () => ExecuteAdminAsync(NpgsqlFactory.Instance, $"DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"));
    }

    private async Task<TestDatabase> CreateSqlServerAsync(string name)
    {
        await ExecuteAdminAsync(SqlClientFactory.Instance, $"CREATE DATABASE [{name}]");
        var connectionString = new SqlConnectionStringBuilder(AdminConnectionString) { InitialCatalog = name }.ToString();
        return new TestDatabase(Engine, Driver, connectionString, SqlClientFactory.Instance.CreateDataSource(connectionString), () =>
        {
            SqlConnection.ClearAllPools();
            return ExecuteAdminAsync(SqlClientFactory.Instance, $"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{name}]");
        });
    }

    private async Task<TestDatabase> CreateMySqlAsync(string name)
    {
        await ExecuteAdminAsync(MySqlConnectorFactory.Instance, $"{RaiseMySqlConnectionLimit} CREATE DATABASE `{name}`");
        var connectionString = new MySqlConnectionStringBuilder(AdminConnectionString!) { Database = name, MaximumPoolSize = MaxPoolSize, ConnectionIdleTimeout = IdleSeconds }.ToString();
        return new TestDatabase(Engine, Driver, connectionString, new MySqlDataSource(connectionString), () =>
        {
            MySqlConnection.ClearAllPools();
            return ExecuteAdminAsync(MySqlConnectorFactory.Instance, $"DROP DATABASE IF EXISTS `{name}`");
        });
    }

#if !SCARLET_AOT_TESTS
    /// <remarks>
    /// MySql.Data 26.10 opens TLS connections through a dictionary shared without synchronisation, which concurrent
    /// opens corrupt, after which every open in the process fails. The tests connect without TLS, and administer the
    /// server through MySqlConnector, so that driver bug cannot fail them. They count changed rows instead of matched
    /// ones, so the providers are tested under both MySQL counting modes.
    /// </remarks>
    private async Task<TestDatabase> CreateMySqlDataAsync(string name)
    {
        await ExecuteAdminAsync(MySqlConnectorFactory.Instance, $"{RaiseMySqlConnectionLimit} CREATE DATABASE `{name}`");
        var connectionString = new MySql.Data.MySqlClient.MySqlConnectionStringBuilder(AdminConnectionString!)
        {
            Database = name,
            SslMode = MySql.Data.MySqlClient.MySqlSslMode.Disabled,
            AllowPublicKeyRetrieval = true,
            UseAffectedRows = true,
            MaximumPoolSize = MaxPoolSize,
        }.ToString();
        return new TestDatabase(Engine, Driver, connectionString, MySql.Data.MySqlClient.MySqlClientFactory.Instance.CreateDataSource(connectionString), () =>
        {
            MySql.Data.MySqlClient.MySqlConnection.ClearAllPools();
            return ExecuteAdminAsync(MySqlConnectorFactory.Instance, $"DROP DATABASE IF EXISTS `{name}`");
        });
    }
#endif


    private async Task ExecuteAdminAsync(DbProviderFactory factory, string sql)
    {
        await using var dataSource = factory.CreateDataSource(AdminConnectionString!);
        await using var command = dataSource.CreateCommand(sql);

        // PostgreSQL checkpoints before creating or dropping a database, which takes far longer than the default 30 seconds
        // on hosts with slow fsync, such as Docker Desktop.
        command.CommandTimeout = 300;
        await command.ExecuteNonQueryAsync();
    }
}

public sealed class SqliteDatabaseFixture() : DatabaseFixture("Sqlite");

public sealed class PostgreSqlDatabaseFixture() : DatabaseFixture("PostgreSql");

public sealed class SqlServerDatabaseFixture() : DatabaseFixture("SqlServer");

public sealed class MySqlDatabaseFixture() : DatabaseFixture("MySql");

public sealed class MariaDbDatabaseFixture() : DatabaseFixture("MariaDb");

#if !SCARLET_AOT_TESTS
// Oracle's MySql.Data relies on reflection, so the NativeAOT tests run MySQL through MySqlConnector only.
public sealed class MySqlDataDatabaseFixture() : DatabaseFixture("MySql", TestDrivers.MySqlData);
#endif

public static class TestDrivers
{
    /// <summary>The engine's usual driver: MySqlConnector for MySQL and MariaDB.</summary>
    public const string Default = "Default";

    /// <summary>Oracle's MySql.Data.</summary>
    public const string MySqlData = "MySql.Data";
}
