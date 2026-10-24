using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans;
using Orleans.Runtime;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;
using Scarlet.Orleans.Reminders.EntityFrameworkCore;

namespace Scarlet.Orleans.Reminders.Tests;

public sealed class BareDbContext(DbContextOptions<BareDbContext> options) : DbContext(options);

/// <summary>
/// The EF Core model against the ADO.NET flavour's SQL scripts, and the schema history that EF Core migrations record.
/// </summary>
public abstract class ReminderModelTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    private static RelationalReminderTable Validating(TestDatabase database) =>
        FlavourTable.Create(database, "model-tests", TestSchemaMode.Validate);

    [Theory]
    [InlineData(RemindersSchema.TableName)]
    [InlineData(SchemaLedger.TableName)]
    public async Task ModelSchema_MatchesTheScripts(string table)
    {
        var fromScripts = await fixture.CreateDatabaseAsync();
        var fromModel = await fixture.CreateDatabaseAsync();
        await ApplyScriptsAsync(fromScripts);
        await using (var context = await FlavourTable.ContextFactory(fromModel).CreateDbContextAsync(CancellationToken))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
        }

        var expectedColumns = await DescribeAsync(fromScripts, ColumnsSql(fromScripts.Engine, table));
        var expectedIndexes = await DescribeAsync(fromScripts, IndexesSql(fromScripts.Engine, table));

        Assert.NotEmpty(expectedColumns);
        Assert.Equal(expectedColumns, await DescribeAsync(fromModel, ColumnsSql(fromModel.Engine, table)));
        Assert.Equal(expectedIndexes, await DescribeAsync(fromModel, IndexesSql(fromModel.Engine, table)));
    }

    [Fact]
    public async Task MigrationStep_RecordsTheVersion_ThenValidatePasses()
    {
        var database = await fixture.CreateDatabaseAsync();
        await FlavourTable.EnsureSchemaAsync(database, CancellationToken);

        await Validating(database).StartAsync(CancellationToken);

        Assert.Equal(SchemaLedger.EntityFrameworkSource, await ScalarAsync(database, $"SELECT source FROM {SchemaLedger.TableName}"));
    }

    [Fact]
    public async Task Validate_OnASchemaCreatedByTheAdoNetScripts_PassesAndStoresReminders()
    {
        var database = await fixture.CreateDatabaseAsync();
        await ApplyScriptsAsync(database);
        var table = Validating(database);
        await table.StartAsync(CancellationToken);
        var grainId = GrainId.Create("model-grain", "key");

        var etag = await table.UpsertRow(new ReminderEntry { GrainId = grainId, ReminderName = "r", StartAt = DateTime.UtcNow, Period = TimeSpan.FromMinutes(1) }, CancellationToken);

        Assert.Equal(etag, (await table.ReadRow(grainId, "r", CancellationToken))?.ETag);
    }

    [Fact]
    public async Task Validate_WithoutTheMigrationStep_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await using (var context = await FlavourTable.ContextFactory(database).CreateDbContextAsync(CancellationToken))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
        }

        var exception = await Assert.ThrowsAsync<SchemaVersionException>(() => Validating(database).StartAsync(CancellationToken));

        Assert.Contains(nameof(ScarletRemindersModelBuilderExtensions.ApplyOrleansRemindersSchema), exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(RemindersSchema.LatestVersion + 1)]
    public void MigrationStep_ForAnUnknownVersion_Throws(int version) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new MigrationBuilder(activeProvider: null).ApplyOrleansRemindersSchema(version));

    [Fact]
    public async Task Context_WithoutTheOrleansModel_FailsAtStartup()
    {
        var options = FlavourTable.Configure(new DbContextOptionsBuilder<BareDbContext>(), fixture.Database).Options;
        var store = new EntityFrameworkCoreReminderStore<BareDbContext>(new PooledDbContextFactory<BareDbContext>(options), SchemaMode.Validate, TimeProvider.System, NullLogger.Instance);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => FlavourTable.Create(store, "model-tests").StartAsync(CancellationToken));

        Assert.Contains(nameof(ScarletRemindersModelBuilderExtensions.ApplyOrleansRemindersModel), exception.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Creates the schema the way the ADO.NET flavour's runner does, from its ledger DDL and SQL scripts.
    /// </summary>
    private static async Task ApplyScriptsAsync(TestDatabase database)
    {
        var engine = RelationalEngines.Parse(database.Engine);
        var scriptEngine = engine == RelationalEngine.MariaDb ? RelationalEngine.MySql : engine;
        var script = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Schema", $"{scriptEngine}-0001-reminders.sql"), CancellationToken);
        foreach (var sql in new[] { SchemaLedgerSql.CreateTable(engine), script })
        {
            await using var command = database.DataSource.CreateCommand(sql);
            await command.ExecuteNonQueryAsync(CancellationToken);
        }
    }

    private static string ColumnsSql(string engine, string table) => engine switch
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
        _ => throw new NotSupportedException(engine),
    };

    /// <summary>
    /// The secondary indexes of a table, with their columns in order and uniqueness.
    /// </summary>
    private static string IndexesSql(string engine, string table) => engine switch
    {
        "Sqlite" => $"""
            SELECT il.name, il."unique", ii.seqno, ii.name FROM pragma_index_list('{table}') il
            JOIN pragma_index_info(il.name) ii WHERE il.origin = 'c'
            ORDER BY il.name, ii.seqno
            """,
        "PostgreSql" => $"""
            SELECT i.relname, ix.indisunique, k.ordinality, a.attname
            FROM pg_class t
            JOIN pg_index ix ON ix.indrelid = t.oid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON TRUE
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE t.relname = '{table}' AND t.relnamespace = current_schema()::regnamespace AND NOT ix.indisprimary
            ORDER BY i.relname, k.ordinality
            """,
        "SqlServer" => $"""
            SELECT i.name, i.is_unique, ic.key_ordinal, c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = OBJECT_ID(N'{table}') AND i.is_primary_key = 0
            ORDER BY i.name, ic.key_ordinal
            """,
        "MySql" or "MariaDb" => $"""
            SELECT index_name, non_unique, seq_in_index, column_name
            FROM information_schema.statistics
            WHERE table_schema = DATABASE() AND table_name = '{table}' AND index_name <> 'PRIMARY'
            ORDER BY index_name, seq_in_index
            """,
        _ => throw new NotSupportedException(engine),
    };

    private static async Task<List<string>> DescribeAsync(TestDatabase database, string sql)
    {
        await using var command = database.DataSource.CreateCommand(sql);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken);
        var rows = new List<string>();
        while (await reader.ReadAsync(CancellationToken))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
        }

        return rows;
    }

    private static async Task<object?> ScalarAsync(TestDatabase database, string sql)
    {
        await using var command = database.DataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync(CancellationToken);
    }
}
