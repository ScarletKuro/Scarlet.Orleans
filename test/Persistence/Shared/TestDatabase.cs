using System.Data.Common;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;

namespace Scarlet.Orleans.Persistence.Tests;

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

    private bool IsAvailable => EnvironmentVariable is null || !string.IsNullOrWhiteSpace(AdminConnectionString);

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
    /// Skips the calling test when this engine is not configured.
    /// </summary>
    public void Require()
    {
        if (!IsAvailable)
        {
            Assert.Skip($"Set {EnvironmentVariable} to an administrative connection string to run {Engine} tests.");
        }
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
            "MySql" or "MariaDb" when Driver == TestDrivers.MySqlData => await CreateMySqlDataAsync(name),
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

        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString();
        return new TestDatabase(Engine, Driver, connectionString, SqliteFactory.Instance.CreateDataSource(connectionString), () =>
        {
            File.Delete(path);
            return Task.CompletedTask;
        });
    }

    private async Task<TestDatabase> CreatePostgreSqlAsync(string name)
    {
        await ExecuteAdminAsync(NpgsqlFactory.Instance, $"CREATE DATABASE \"{name}\"");
        var connectionString = new NpgsqlConnectionStringBuilder(AdminConnectionString) { Database = name }.ToString();
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
        await ExecuteAdminAsync(MySqlConnectorFactory.Instance, $"CREATE DATABASE `{name}`");
        var connectionString = new MySqlConnectionStringBuilder(AdminConnectionString!) { Database = name }.ToString();
        return new TestDatabase(Engine, Driver, connectionString, new MySqlDataSource(connectionString), () =>
        {
            MySqlConnection.ClearAllPools();
            return ExecuteAdminAsync(MySqlConnectorFactory.Instance, $"DROP DATABASE IF EXISTS `{name}`");
        });
    }

    private async Task<TestDatabase> CreateMySqlDataAsync(string name)
    {
        var factory = MySql.Data.MySqlClient.MySqlClientFactory.Instance;
        await ExecuteAdminAsync(factory, $"CREATE DATABASE `{name}`");
        var connectionString = new MySql.Data.MySqlClient.MySqlConnectionStringBuilder(AdminConnectionString!) { Database = name }.ToString();
        return new TestDatabase(Engine, Driver, connectionString, factory.CreateDataSource(connectionString), () =>
        {
            MySql.Data.MySqlClient.MySqlConnection.ClearAllPools();
            return ExecuteAdminAsync(factory, $"DROP DATABASE IF EXISTS `{name}`");
        });
    }


    private async Task ExecuteAdminAsync(DbProviderFactory factory, string sql)
    {
        await using var dataSource = factory.CreateDataSource(AdminConnectionString!);
        await using var command = dataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }
}

public sealed class SqliteDatabaseFixture() : DatabaseFixture("Sqlite");

public sealed class PostgreSqlDatabaseFixture() : DatabaseFixture("PostgreSql");

public sealed class SqlServerDatabaseFixture() : DatabaseFixture("SqlServer");

public sealed class MySqlDatabaseFixture() : DatabaseFixture("MySql");

public sealed class MariaDbDatabaseFixture() : DatabaseFixture("MariaDb");

public sealed class MySqlDataDatabaseFixture() : DatabaseFixture("MySql", TestDrivers.MySqlData);

public static class TestDrivers
{
    /// <summary>The engine's usual driver: MySqlConnector for MySQL and MariaDB.</summary>
    public const string Default = "Default";

    /// <summary>Oracle's MySql.Data.</summary>
    public const string MySqlData = "MySql.Data";
}
