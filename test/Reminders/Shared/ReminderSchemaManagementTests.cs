using System.Data;
using System.Globalization;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// Startup schema checks and migrations through the reminder table. Every test uses a fresh database.
/// </summary>
public abstract class ReminderSchemaManagementTests(DatabaseFixture fixture)
{
    private static RelationalReminderTable Table(TestDatabase database, TestSchemaMode mode) =>
        FlavourTable.Create(database, "schema-tests", mode);

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Validate_OnEmptyDatabase_FailsNamingTheScript()
    {
        var database = await fixture.CreateDatabaseAsync();

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Table(database, TestSchemaMode.Validate).StartAsync(CancellationToken));

        var scriptEngine = database.Engine == "MariaDb" ? "MySql" : database.Engine;
        Assert.Contains($"{scriptEngine}-0001-reminders.sql", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task None_OnEmptyDatabase_SkipsTheCheck()
    {
        var database = await fixture.CreateDatabaseAsync();

        await Table(database, TestSchemaMode.None).StartAsync(CancellationToken);
    }

    [Fact]
    public async Task Apply_CreatesTheSchema_ThenValidatePasses()
    {
        var database = await fixture.CreateDatabaseAsync();

        await Table(database, TestSchemaMode.Apply).StartAsync(CancellationToken);
        await Table(database, TestSchemaMode.Apply).StartAsync(CancellationToken);
        await Table(database, TestSchemaMode.Validate).StartAsync(CancellationToken);

        Assert.Equal([1], await ReadLedgerVersionsAsync(database));
        Assert.Equal(SchemaLedger.ScriptSource, await ScalarAsync(database, $"SELECT source FROM {SchemaLedger.TableName}"));
    }

    [Fact]
    public async Task Validate_AfterTheScriptRanByHand_Passes()
    {
        var database = await fixture.CreateDatabaseAsync();
        var script = Assert.Single(SchemaScript.Load(typeof(RemindersSchema).Assembly, RelationalEngines.Parse(database.Engine)));
        await ExecuteAsync(database, script.Text);

        await Table(database, TestSchemaMode.Validate).StartAsync(CancellationToken);
        await Table(database, TestSchemaMode.Apply).StartAsync(CancellationToken);

        Assert.Equal([1], await ReadLedgerVersionsAsync(database));
        Assert.Equal(SchemaLedger.ManualSource, await ScalarAsync(database, $"SELECT source FROM {SchemaLedger.TableName}"));
    }

    [Fact]
    public async Task Apply_FromManySilosAtOnce_RecordsTheVersionOnce()
    {
        var database = await fixture.CreateDatabaseAsync();

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Table(database, TestSchemaMode.Apply).StartAsync(CancellationToken)));

        Assert.Equal([1], await ReadLedgerVersionsAsync(database));
    }

    [Fact]
    public async Task Validate_WhenAnAppliedScriptChanged_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await Table(database, TestSchemaMode.Apply).StartAsync(CancellationToken);
        await ExecuteAsync(database, $"UPDATE {SchemaLedger.TableName} SET checksum = 'edited'");

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Table(database, TestSchemaMode.Validate).StartAsync(CancellationToken));

        Assert.Contains("must never change", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_WhenTheDatabaseIsNewer_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await Table(database, TestSchemaMode.Apply).StartAsync(CancellationToken);
        await InsertLedgerRowAsync(database, version: 2);

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Table(database, TestSchemaMode.Validate).StartAsync(CancellationToken));

        Assert.Contains("newer", exception.Message, StringComparison.Ordinal);
    }

    private static async Task<List<int>> ReadLedgerVersionsAsync(TestDatabase database)
    {
        await using var command = database.DataSource.CreateCommand(SchemaLedger.SelectApplied);
        command.AddParameter("component", RemindersSchema.Component, DbType.String);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken);
        var versions = new List<int>();
        while (await reader.ReadAsync(CancellationToken))
        {
            versions.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        }

        return versions;
    }

    private static async Task InsertLedgerRowAsync(TestDatabase database, int version)
    {
        await using var command = database.DataSource.CreateCommand(SchemaLedger.Insert);
        command.AddParameter("component", RemindersSchema.Component, DbType.String);
        command.AddParameter("version", version, DbType.Int32);
        command.AddParameter("script", "future.sql", DbType.String);
        command.AddParameter("checksum", "future", DbType.String);
        command.AddParameter("source", SchemaLedger.ScriptSource, DbType.String);
        await command.ExecuteNonQueryAsync(CancellationToken);
    }

    private static async Task<object?> ScalarAsync(TestDatabase database, string sql)
    {
        await using var command = database.DataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync(CancellationToken);
    }

    private static async Task ExecuteAsync(TestDatabase database, string sql)
    {
        await using var command = database.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(CancellationToken);
    }
}
