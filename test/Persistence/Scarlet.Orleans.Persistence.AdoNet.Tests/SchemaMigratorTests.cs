using System.Data;
using System.Globalization;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// The migration runner over synthetic scripts, which exercise more than one schema version.
/// </summary>
public abstract class SchemaMigratorTests(DatabaseFixture fixture)
{
    private readonly string component = $"test-{Guid.NewGuid():N}";
    private readonly string suffix = Guid.NewGuid().ToString("N")[..8];

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    private RelationalEngine Engine => RelationalEngines.Parse(fixture.Engine);

    private SchemaScript Script(int version, string text) => new(version, $"{fixture.Engine}-{version:0000}-test.sql", text);

    private SchemaScript CreateTable(int version) => Script(version, $"CREATE TABLE m{version}_{suffix} (id int)");

    private async Task EnsureAsync(bool apply, int minimumVersion, params SchemaScript[] scripts)
    {
        await using var connection = await fixture.Database.DataSource.OpenConnectionAsync(CancellationToken);
        await new SchemaMigrator(component, Engine, scripts, minimumVersion).EnsureAsync(connection, apply, CancellationToken);
    }

    [Fact]
    public async Task Apply_RunsPendingScriptsInOrder()
    {
        await EnsureAsync(apply: true, minimumVersion: 1, CreateTable(1));
        await EnsureAsync(apply: true, minimumVersion: 2, CreateTable(1), CreateTable(2));

        Assert.Equal([1, 2], await ReadLedgerAsync());
        await ExecuteAsync($"SELECT COUNT(*) FROM m2_{suffix}");
    }

    [Fact]
    public async Task Validate_AtOrAboveMinimumVersion_AcceptsPendingScripts()
    {
        await EnsureAsync(apply: true, minimumVersion: 1, CreateTable(1));

        await EnsureAsync(apply: false, minimumVersion: 1, CreateTable(1), CreateTable(2));
        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => EnsureAsync(apply: false, minimumVersion: 2, CreateTable(1), CreateTable(2)));

        Assert.Contains(CreateTable(2).Name, exception.Message, StringComparison.Ordinal);
        Assert.Equal([1], await ReadLedgerAsync());
    }

    [Fact]
    public async Task Apply_WhenManagedByEntityFrameworkMigrations_Refuses()
    {
        await ExecuteAsync(SchemaLedger.CreateTable(Engine));
        await InsertLedgerRowAsync(1, SchemaLedger.EntityFrameworkSource);

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => EnsureAsync(apply: true, minimumVersion: 2, CreateTable(1), CreateTable(2)));

        Assert.Contains("EF Core", exception.Message, StringComparison.Ordinal);
        Assert.Equal([1], await ReadLedgerAsync());
    }

    [Fact]
    public async Task Validate_EntityFrameworkRows_SkipTheChecksum()
    {
        await ExecuteAsync(SchemaLedger.CreateTable(Engine));
        await InsertLedgerRowAsync(1, SchemaLedger.EntityFrameworkSource);

        await EnsureAsync(apply: false, minimumVersion: 1, CreateTable(1));
    }

    [Fact]
    public async Task Apply_WhenAScriptFails_RecordsNothing()
    {
        await ExecuteAsync(SchemaLedger.CreateTable(Engine));
        var failing = Script(1, $"CREATE TABLE m1_{suffix} (id int); SELECT * FROM missing_{suffix}");

        await Assert.ThrowsAnyAsync<Exception>(() => EnsureAsync(apply: true, minimumVersion: 1, failing));

        Assert.Empty(await ReadLedgerAsync());
    }

    private async Task<List<int>> ReadLedgerAsync()
    {
        await using var command = fixture.Database.DataSource.CreateCommand(SchemaLedger.SelectApplied);
        command.AddParameter("component", component, DbType.String);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken);
        var versions = new List<int>();
        while (await reader.ReadAsync(CancellationToken))
        {
            versions.Add(Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        }

        return versions;
    }

    private async Task InsertLedgerRowAsync(int version, string source)
    {
        await using var command = fixture.Database.DataSource.CreateCommand(SchemaLedger.Insert);
        command.AddParameter("component", component, DbType.String);
        command.AddParameter("version", version, DbType.Int32);
        command.AddParameter("script", "ef", DbType.String);
        command.AddParameter("checksum", null, DbType.String);
        command.AddParameter("source", source, DbType.String);
        await command.ExecuteNonQueryAsync(CancellationToken);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var command = fixture.Database.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(CancellationToken);
    }
}
