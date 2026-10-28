using System.Data.Common;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;

/// <summary>
/// A freshly created benchmark database on one engine, with the providers to compare on it.
/// </summary>
internal sealed partial class Engine : IAsyncDisposable
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

    /// <summary>Compare the in-box providers on server engines; the SQLite comparison uses Microsoft.Data.Sqlite.</summary>
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

    /// <summary>
    /// Runs the in-box provider's scripts, splitting MySQL's at its <c>DELIMITER</c> directives as the <c>mysql</c> client does.
    /// </summary>
    public async Task InstallInBoxSchemaAsync(string feature = "Persistence")
    {
        if (!Providers.Contains(InBox))
        {
            return;
        }

        var prefix = Name switch { "PostgreSql" => "PostgreSQL", "SqlServer" => "SQLServer", _ => "MySQL" };
        foreach (var script in new[] { $"{prefix}-Main.sql", $"{prefix}-{feature}.sql" })
        {
            var text = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "InBoxScripts", script));
            // Only scripts defining routines need client-side DELIMITER handling. Execute ordinary scripts as a
            // batch so semicolons inside quoted OrleansQuery values remain part of those values.
            var statements = Name switch
            {
                "MySql" when text.Contains("DELIMITER ", StringComparison.OrdinalIgnoreCase) => SplitMySql(text),
                "SqlServer" => SplitSqlServer(text),
                _ => [text],
            };
            foreach (var statement in statements)
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

    // GO is a client-side batch separator. Grain directory scripts use it around CREATE PROCEDURE statements.
    private static IEnumerable<string> SplitSqlServer(string script)
    {
        var current = new StringBuilder();
        foreach (var line in script.ReplaceLineEndings("\n").Split('\n'))
        {
            if (line.Trim().Equals("GO", StringComparison.OrdinalIgnoreCase))
            {
                if (current.ToString().Trim() is { Length: > 0 } batch)
                {
                    yield return batch;
                }

                current.Clear();
            }
            else
            {
                current.AppendLine(line);
            }
        }

        if (current.ToString().Trim() is { Length: > 0 } rest)
        {
            yield return rest;
        }
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
