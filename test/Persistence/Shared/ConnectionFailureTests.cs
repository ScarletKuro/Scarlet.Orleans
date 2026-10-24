using System.Data.Common;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Grain writes through real connection failures: sessions killed by the database, as in a failover, and responses lost on
/// the way back, as in a network partition after the server has committed.
/// </summary>
/// <remarks>
/// Each writer writes increasing values to its own grain. A write that fails with any error other than a conflict has an
/// unknown outcome; the writer then resynchronises from storage, as a grain does after reactivation.
/// </remarks>
public abstract class ConnectionFailureTests(DatabaseFixture fixture)
{
    private const string StateName = "state";
    private const int Writers = 8;
    private static readonly TimeSpan Duration = TimeSpan.FromSeconds(4);
    private static readonly TimeSpan KillInterval = TimeSpan.FromMilliseconds(100);

    [Fact]
    public async Task Writes_WhileSessionsAreKilled_StayDurableAndNeverConflictFalsely()
    {
        var database = fixture.Database;
        var storage = FlavourStorage.Create(database, $"service-{Guid.NewGuid():N}", deleteStateOnClear: false, TestSchemaMode.Apply);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        using var stop = new CancellationTokenSource(Duration);

        var killer = KillSessionsAsync(database, stop.Token);
        var writers = Enumerable.Range(0, Writers).Select(i => WriteAsync(storage, GrainId.Create("failure-grain", $"{Guid.NewGuid():N}-{i}"), stop.Token)).ToArray();
        var results = await Task.WhenAll(writers);
        var kills = await killer;

        Assert.True(kills > 0, "The test database had no sessions to kill.");
        Assert.True(results.Sum(r => r.Failures) > 0, $"Killing {kills} sessions failed no write, so this test exercised nothing.");
        await VerifyAsync(storage, results);
    }

    /// <summary>
    /// The server runs, and commits, every statement it receives, but some responses never reach the driver: the outcome
    /// of those writes is unknown, which is where a false conflict or a lost write would show.
    /// </summary>
    [Fact]
    public async Task Writes_WhoseResponsesAreLost_StayDurableAndNeverConflictFalsely()
    {
        var (host, port, connect) = Route(fixture.Database);
        await using var proxy = new ChaosProxy(host, port);
        var (connectionString, dataSource) = connect(proxy.Port);

        // The rewritten connection string is kept as built: a data source's own copy can omit the password (Npgsql does),
        // and the EF Core flavour connects from the string.
        await using var database = new TestDatabase(fixture.Database.Engine, fixture.Database.Driver, connectionString, dataSource, () => Task.CompletedTask);
        var storage = FlavourStorage.Create(database, $"service-{Guid.NewGuid():N}", deleteStateOnClear: false, TestSchemaMode.Apply);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        using var stop = new CancellationTokenSource(Duration);

        proxy.DropProbability = 0.05;
        var results = await Task.WhenAll(Enumerable.Range(0, Writers).Select(i => WriteAsync(storage, GrainId.Create("failure-grain", $"{Guid.NewGuid():N}-{i}"), stop.Token)));
        proxy.DropProbability = 0;

        Assert.True(proxy.Drops > 0 && results.Sum(r => r.Failures) > 0, $"{proxy.Drops} dropped responses failed no write, so this test exercised nothing.");
        await VerifyAsync(storage, results);
    }

    private static async Task VerifyAsync(RelationalGrainStorage storage, WriterResult[] results)
    {
        Assert.True(results.Sum(r => r.Acknowledged) > 0, "No write succeeded.");
        foreach (var result in results)
        {
            var (stored, _) = await ReadUntilAvailableAsync(storage, result.GrainId, CancellationToken.None);
            Assert.InRange(stored, result.LastAcknowledged, result.LastAttempted);

            var final = result.LastAttempted + 1;
            await WriteUntilAcknowledgedAsync(storage, result.GrainId, final);
            Assert.Equal(final, (await ReadUntilAvailableAsync(storage, result.GrainId, CancellationToken.None)).Value);
        }
    }

    /// <summary>
    /// The database server's address, and a way to open the same database through another local port.
    /// </summary>
    private static (string Host, int Port, Func<int, (string ConnectionString, DbDataSource DataSource)> Connect) Route(TestDatabase database)
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
                    port => Open(new SqlConnectionStringBuilder(database.ConnectionString) { DataSource = $"127.0.0.1,{port}" }.ToString(), SqlClientFactory.Instance.CreateDataSource));
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

    private static async Task<WriterResult> WriteAsync(RelationalGrainStorage storage, GrainId grainId, CancellationToken stop)
    {
        var result = new WriterResult(grainId);
        string? etag = null;
        var uncertain = false;
        while (!stop.IsCancellationRequested)
        {
            var value = result.LastAttempted + 1;
            var state = new GrainState<TestState> { State = new TestState { Value = value }, ETag = etag, RecordExists = etag is not null };
            result.LastAttempted = value;
            try
            {
                await storage.WriteStateAsync(StateName, grainId, state);
                etag = state.ETag;
                result.LastAcknowledged = value;
                result.Acknowledged++;
                uncertain = false;
            }
            catch (InconsistentStateException) when (uncertain)
            {
                // An earlier write of unknown outcome committed after all; storage moved on, as a grain learns on reactivation.
                (_, etag) = await ReadUntilAvailableAsync(storage, grainId, CancellationToken.None);
                uncertain = false;
            }
            catch (Exception exception) when (exception is not InconsistentStateException and not GrainStateKeyCollisionException)
            {
                result.Failures++;
                uncertain = true;
            }
        }

        return result;
    }

    /// <summary>
    /// Writes as a reactivated grain would once the database is reachable again: from the stored ETag, retrying while
    /// pooled connections broken by the last kills are discarded.
    /// </summary>
    private static async Task WriteUntilAcknowledgedAsync(RelationalGrainStorage storage, GrainId grainId, int value)
    {
        var deadline = Stopwatch.StartNew();
        while (true)
        {
            var (_, etag) = await ReadUntilAvailableAsync(storage, grainId, CancellationToken.None);
            try
            {
                await storage.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState { Value = value }, ETag = etag, RecordExists = etag is not null });
                return;
            }
            catch (Exception exception) when (exception is not GrainStateKeyCollisionException && deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                await Task.Delay(50);
            }
        }
    }

    private static async Task<(int Value, string? ETag)> ReadUntilAvailableAsync(RelationalGrainStorage storage, GrainId grainId, CancellationToken cancellationToken)
    {
        var deadline = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                var read = new GrainState<TestState>();
                await storage.ReadStateAsync(StateName, grainId, read, cancellationToken);
                return (read.State!.Value, read.ETag);
            }
            catch (Exception) when (deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                await Task.Delay(50, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Kills every other session connected to the test database, repeatedly, and returns how many were killed.
    /// </summary>
    private static async Task<int> KillSessionsAsync(TestDatabase database, CancellationToken stop)
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

    private sealed class WriterResult(GrainId grainId)
    {
        public GrainId GrainId { get; } = grainId;

        public int LastAttempted { get; set; }

        public int LastAcknowledged { get; set; }

        public int Acknowledged { get; set; }

        public int Failures { get; set; }
    }
}
