using System.Data.Common;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;

namespace Scarlet.Orleans.Tests;

/// <summary>
/// Real connection failures for failure tests: sessions killed by the database, and connections routed through a
/// <see cref="ChaosProxy"/>.
/// </summary>
public static class ConnectionFaults
{
    private static readonly TimeSpan KillInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// The database server's address, and a way to open the same database through another local port.
    /// </summary>
    public static (string Host, int Port, Func<int, (string ConnectionString, DbDataSource DataSource)> Connect) Route(TestDatabase database)
    {
        switch (database.Engine)
        {
            case "PostgreSql":
            {
                var builder = new NpgsqlConnectionStringBuilder(database.ConnectionString);
                return (builder.Host!, builder.Port, port => Open(new NpgsqlConnectionStringBuilder(database.ConnectionString) { Host = "127.0.0.1", Port = port }.ToString(), NpgsqlDataSource.Create));
            }

            case "SqlServer":
            {
                var server = new SqlConnectionStringBuilder(database.ConnectionString).DataSource.Split(',');
                return (server[0], server.Length > 1 ? int.Parse(server[1], System.Globalization.CultureInfo.InvariantCulture) : 1433,
                    // After a failed open, SqlClient fails every open for seconds by default, which would leave a
                    // lossy test with no successful write at all.
                    port => Open(new SqlConnectionStringBuilder(database.ConnectionString) { DataSource = $"127.0.0.1,{port}", PoolBlockingPeriod = PoolBlockingPeriod.NeverBlock }.ToString(), SqlClientFactory.Instance.CreateDataSource));
            }

#if !SCARLET_AOT_TESTS
            case "MySql" when database.Driver == TestDrivers.MySqlData:
            {
                var builder = new MySql.Data.MySqlClient.MySqlConnectionStringBuilder(database.ConnectionString);
                return (builder.Server, (int)builder.Port, port => Open(
                    new MySql.Data.MySqlClient.MySqlConnectionStringBuilder(database.ConnectionString) { Server = "127.0.0.1", Port = (uint)port }.ToString(),
                    MySql.Data.MySqlClient.MySqlClientFactory.Instance.CreateDataSource));
            }
#endif

            default:
            {
                var builder = new MySqlConnectionStringBuilder(database.ConnectionString);
                return (builder.Server, (int)builder.Port, port => Open(new MySqlConnectionStringBuilder(database.ConnectionString) { Server = "127.0.0.1", Port = (uint)port }.ToString(), cs => new MySqlDataSource(cs)));
            }
        }

        static (string, DbDataSource) Open(string connectionString, Func<string, DbDataSource> create) => (connectionString, create(connectionString));
    }

    /// <summary>
    /// Kills every other session connected to the test database, repeatedly, and returns how many were killed.
    /// </summary>
    public static async Task<int> KillSessionsAsync(TestDatabase database, CancellationToken stop)
    {
        var killed = 0;
        while (!stop.IsCancellationRequested)
        {
            try
            {
                killed += await KillOnceAsync(database);
            }
            catch (Exception) when (!stop.IsCancellationRequested)
            {
                // The killer's own connection may come from a pool another kill just broke.
            }

            await Task.Delay(KillInterval, CancellationToken.None);
        }

        return killed;
    }

    private static async Task<int> KillOnceAsync(TestDatabase database)
    {
        await using var connection = await database.DataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        switch (database.Engine)
        {
            case "PostgreSql":
                command.CommandText = "SELECT COUNT(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()";
                return Convert.ToInt32(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);

            case "SqlServer":
                command.CommandText = """
                    DECLARE @kill nvarchar(max) = N'', @count int = 0;
                    SELECT @kill += N'KILL ' + CAST(session_id AS nvarchar(10)) + N';', @count += 1
                    FROM sys.dm_exec_sessions
                    WHERE database_id = DB_ID() AND session_id <> @@SPID AND is_user_process = 1;
                    EXEC (@kill);
                    SELECT @count;
                    """;
                return Convert.ToInt32(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);

            case "MySql" or "MariaDb":
                command.CommandText = "SELECT id FROM information_schema.processlist WHERE db = DATABASE() AND id <> CONNECTION_ID()";
                var ids = new List<long>();
                await using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        ids.Add(Convert.ToInt64(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture));
                    }
                }

                foreach (var id in ids)
                {
                    await using var kill = connection.CreateCommand();
                    kill.CommandText = $"KILL {id}";
                    try
                    {
                        await kill.ExecuteNonQueryAsync();
                    }
                    catch (Exception)
                    {
                        // The session ended between listing and killing it.
                    }
                }

                return ids.Count;

            default:
                throw new NotSupportedException(database.Engine);
        }
    }
}
