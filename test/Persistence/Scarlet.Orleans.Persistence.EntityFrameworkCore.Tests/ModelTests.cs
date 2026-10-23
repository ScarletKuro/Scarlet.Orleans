using System.Data;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans;
using Orleans.Runtime;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Persistence.Tests;

public sealed class BareDbContext(DbContextOptions<BareDbContext> options) : DbContext(options);

/// <summary>
/// The EF Core model against the ADO.NET flavour's SQL scripts, and the schema history that EF Core migrations record.
/// </summary>
public abstract class ModelTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    private static RelationalGrainStorage Validating(TestDatabase database) =>
        FlavourStorage.Create(database, "model-tests", deleteStateOnClear: false, TestSchemaMode.Validate);

    [Theory]
    [InlineData(PersistenceSchema.TableName)]
    [InlineData(SchemaLedger.TableName)]
    public async Task ModelSchema_MatchesTheScripts(string table)
    {
        var fromScripts = await fixture.CreateDatabaseAsync();
        var fromModel = await fixture.CreateDatabaseAsync();
        await ApplyScriptsAsync(fromScripts);
        await using (var context = await FlavourStorage.ContextFactory(fromModel).CreateDbContextAsync(CancellationToken))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
        }

        var expected = await DescribeTableAsync(fromScripts, table);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, await DescribeTableAsync(fromModel, table));
    }

    [Fact]
    public async Task MigrationStep_RecordsTheVersion_ThenValidatePasses()
    {
        var database = await fixture.CreateDatabaseAsync();
        await FlavourStorage.EnsureSchemaAsync(database, CancellationToken);

        await Validating(database).InitializeAsync(CancellationToken);

        Assert.Equal(SchemaLedger.EntityFrameworkSource, await ScalarAsync(database, $"SELECT source FROM {SchemaLedger.TableName}"));
    }

    [Fact]
    public async Task Validate_OnASchemaCreatedByTheAdoNetScripts_PassesAndStoresState()
    {
        var database = await fixture.CreateDatabaseAsync();
        await ApplyScriptsAsync(database);
        await InsertLedgerRowAsync(database, version: 1, SchemaLedger.ScriptSource);
        var storage = Validating(database);
        await storage.InitializeAsync(CancellationToken);
        var grainId = GrainId.Create("model-grain", "key");

        await storage.WriteStateAsync("state", grainId, new GrainState<TestState> { State = new TestState { Value = 3 } });

        var read = new GrainState<TestState>();
        await storage.ReadStateAsync("state", grainId, read);
        Assert.Equal(3, read.State!.Value);
    }

    [Fact]
    public async Task Validate_WithoutTheMigrationStep_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await using (var context = await FlavourStorage.ContextFactory(database).CreateDbContextAsync(CancellationToken))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
        }

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Validating(database).InitializeAsync(CancellationToken));

        Assert.Contains(nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceSchema), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_OnAnEmptyDatabase_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Validating(database).InitializeAsync(CancellationToken));

        Assert.NotNull(exception.InnerException);
    }

    [Fact]
    public async Task Validate_WhenTheDatabaseIsNewer_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await FlavourStorage.EnsureSchemaAsync(database, CancellationToken);
        await InsertLedgerRowAsync(database, version: 2, SchemaLedger.EntityFrameworkSource);

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Validating(database).InitializeAsync(CancellationToken));

        Assert.Contains("newer", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(PersistenceSchema.LatestVersion + 1)]
    public void MigrationStep_ForAnUnknownVersion_Throws(int version) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new MigrationBuilder(activeProvider: null).ApplyOrleansPersistenceSchema(version));

    [Fact]
    public async Task Context_WithoutTheOrleansModel_FailsAtStartup()
    {
        var options = FlavourStorage.Configure(new DbContextOptionsBuilder<BareDbContext>(), fixture.Database).Options;
        var store = new EntityFrameworkCoreGrainStateStore<BareDbContext>(new PooledDbContextFactory<BareDbContext>(options), SchemaMode.Validate);
        var storage = new RelationalGrainStorage("test", "model-tests", store, new JsonTestSerializer(), new TestActivatorProvider(), false, 0, TimeProvider.System, NullLogger<RelationalGrainStorage>.Instance);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => storage.InitializeAsync(CancellationToken));

        Assert.Contains(nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceModel), exception.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Creates the schema the way the ADO.NET flavour's runner does, from its ledger DDL and SQL scripts.
    /// </summary>
    private static async Task ApplyScriptsAsync(TestDatabase database)
    {
        var engine = RelationalEngines.Parse(database.Engine);
        var scriptEngine = engine == RelationalEngine.MariaDb ? RelationalEngine.MySql : engine;
        var script = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Schema", $"{scriptEngine}-0001-grain-state.sql"), CancellationToken);
        foreach (var sql in new[] { SchemaLedger.CreateTable(engine), script })
        {
            await using var command = database.DataSource.CreateCommand(sql);
            await command.ExecuteNonQueryAsync(CancellationToken);
        }
    }

    private static async Task InsertLedgerRowAsync(TestDatabase database, int version, string source)
    {
        await using var command = database.DataSource.CreateCommand(SchemaLedger.Insert);
        command.AddParameter("component", PersistenceSchema.Component, DbType.String);
        command.AddParameter("version", version, DbType.Int32);
        command.AddParameter("script", $"{PersistenceSchema.Component}-{version:0000}", DbType.String);
        command.AddParameter("checksum", null, DbType.String);
        command.AddParameter("source", source, DbType.String);
        await command.ExecuteNonQueryAsync(CancellationToken);
    }

    private static async Task<List<string>> DescribeTableAsync(TestDatabase database, string table)
    {
        var sql = database.Engine switch
        {
            "Sqlite" => $"SELECT name, type, \"notnull\", pk FROM pragma_table_info('{table}') ORDER BY name",
            "PostgreSql" => $"""
                SELECT c.column_name, c.data_type, c.is_nullable, c.character_maximum_length, c.datetime_precision,
                    (SELECT tc.constraint_name FROM information_schema.key_column_usage k
                        JOIN information_schema.table_constraints tc ON tc.constraint_name = k.constraint_name AND tc.table_schema = k.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY' AND k.table_name = c.table_name AND k.column_name = c.column_name AND k.table_schema = c.table_schema)
                FROM information_schema.columns c
                WHERE c.table_schema = current_schema() AND c.table_name = '{table}'
                ORDER BY c.column_name
                """,
            "SqlServer" => $"""
                SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.CHARACTER_MAXIMUM_LENGTH, c.DATETIME_PRECISION,
                    (SELECT tc.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND k.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = '{table}'
                ORDER BY c.COLUMN_NAME
                """,
            "MySql" or "MariaDb" => $"""
                SELECT column_name, column_type, is_nullable, column_key, character_set_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = '{table}'
                ORDER BY column_name
                """,
            _ => throw new NotSupportedException(database.Engine),
        };

        await using var command = database.DataSource.CreateCommand(sql);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken);
        var columns = new List<string>();
        while (await reader.ReadAsync(CancellationToken))
        {
            columns.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
        }

        return columns;
    }

    private static async Task<object?> ScalarAsync(TestDatabase database, string sql)
    {
        await using var command = database.DataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync(CancellationToken);
    }
}
