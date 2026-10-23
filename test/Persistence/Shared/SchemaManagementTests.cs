using System.Data;
using System.Globalization;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Startup schema checks and migrations through the storage provider. Every test uses a fresh database.
/// </summary>
public abstract class SchemaManagementTests(DatabaseFixture fixture)
{
    private static RelationalGrainStorage Storage(TestDatabase database, TestSchemaMode mode) =>
        FlavourStorage.Create(database, "schema-tests", deleteStateOnClear: false, mode);

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Validate_OnEmptyDatabase_FailsNamingTheScript()
    {
        var database = await fixture.CreateDatabaseAsync();

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Storage(database, TestSchemaMode.Validate).InitializeAsync(CancellationToken));

        var scriptEngine = database.Engine == "MariaDb" ? "MySql" : database.Engine;
        Assert.Contains($"{scriptEngine}-0001-grain-state.sql", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task None_OnEmptyDatabase_SkipsTheCheck()
    {
        var database = await fixture.CreateDatabaseAsync();

        await Storage(database, TestSchemaMode.None).InitializeAsync(CancellationToken);
    }

    [Fact]
    public async Task Apply_CreatesTheSchema_ThenValidatePasses()
    {
        var database = await fixture.CreateDatabaseAsync();

        await Storage(database, TestSchemaMode.Apply).InitializeAsync(CancellationToken);
        await Storage(database, TestSchemaMode.Apply).InitializeAsync(CancellationToken);
        await Storage(database, TestSchemaMode.Validate).InitializeAsync(CancellationToken);

        Assert.Equal([1], await ReadLedgerVersionsAsync(database));
    }

    [Fact]
    public async Task Apply_FromManySilosAtOnce_RecordsTheVersionOnce()
    {
        var database = await fixture.CreateDatabaseAsync();

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Storage(database, TestSchemaMode.Apply).InitializeAsync(CancellationToken)));

        Assert.Equal([1], await ReadLedgerVersionsAsync(database));
    }

    [Fact]
    public async Task Validate_WhenAnAppliedScriptChanged_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await Storage(database, TestSchemaMode.Apply).InitializeAsync(CancellationToken);
        await ExecuteAsync(database, $"UPDATE {SchemaLedger.TableName} SET checksum = 'edited'");

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Storage(database, TestSchemaMode.Validate).InitializeAsync(CancellationToken));

        Assert.Contains("must never change", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_WhenTheDatabaseIsNewer_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await Storage(database, TestSchemaMode.Apply).InitializeAsync(CancellationToken);
        await InsertLedgerRowAsync(database, version: 2);

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Storage(database, TestSchemaMode.Validate).InitializeAsync(CancellationToken));

        Assert.Contains("newer", exception.Message, StringComparison.Ordinal);
    }

    private static async Task<List<int>> ReadLedgerVersionsAsync(TestDatabase database)
    {
        await using var command = database.DataSource.CreateCommand(SchemaLedger.SelectApplied);
        command.AddParameter("component", PersistenceSchema.Component, DbType.String);
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
        command.AddParameter("component", PersistenceSchema.Component, DbType.String);
        command.AddParameter("version", version, DbType.Int32);
        command.AddParameter("script", "future.sql", DbType.String);
        command.AddParameter("checksum", "future", DbType.String);
        command.AddParameter("source", SchemaLedger.ScriptSource, DbType.String);
        await command.ExecuteNonQueryAsync(CancellationToken);
    }

    private static async Task ExecuteAsync(TestDatabase database, string sql)
    {
        await using var command = database.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(CancellationToken);
    }
}
