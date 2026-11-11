using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Streaming.Tests;

public sealed class StreamingDbContext(DbContextOptions<StreamingDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansStreamingModel();
}

/// <summary>
/// The EF Core model of Scarlet.Orleans.Streaming.EntityFrameworkCore against the ADO.NET package's SQL scripts, and the
/// ADO.NET provider running on a schema that EF Core created and recorded, as an application's migration would.
/// </summary>
public abstract class StreamingModelTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(StreamingSchema.QueueTableName)]
    [InlineData(StreamingSchema.MessageTableName)]
    [InlineData(StreamingSchema.DeadLetterTableName)]
    [InlineData(SchemaLedger.TableName)]
    public async Task ModelSchema_MatchesTheScripts(string table)
    {
        var fromScripts = await fixture.CreateDatabaseAsync();
        var fromModel = await fixture.CreateDatabaseAsync();
        await FlavourStreaming.CreateStore(fromScripts, TestSchemaMode.Apply).InitializeAsync(CancellationToken);
        await using (var context = CreateContext(fromModel))
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
    public async Task AdoNetStore_OnTheModelsSchema_ValidatesAndQueues()
    {
        var database = await fixture.CreateDatabaseAsync();
        await using (var context = CreateContext(database))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
            var migration = new MigrationBuilder(context.Database.ProviderName);
            migration.ApplyOrleansStreamingSchema(1);
            var commands = context.GetService<IMigrationsSqlGenerator>().Generate(migration.Operations, context.Model);
            await context.GetService<IMigrationCommandExecutor>().ExecuteNonQueryAsync(commands, context.GetService<IRelationalConnection>(), CancellationToken);
        }

        var store = FlavourStreaming.CreateStore(database, TestSchemaMode.Validate);
        await store.InitializeAsync(CancellationToken);
        var queue = StreamQueueKey.Create("service", "provider", "queue");
        var now = DateTimeOffset.UtcNow;
        await store.EnqueueAsync(queue, Guid.NewGuid(), [1, 2, 3], now, now.AddMinutes(1), CancellationToken);
        await store.EnqueueAsync(queue, Guid.NewGuid(), [4], now, now.AddMinutes(1), CancellationToken);

        var messages = await store.DequeueAsync(queue, 10, 3, now, now.AddMinutes(1), CancellationToken);

        Assert.Equal([[1, 2, 3], [4]], messages.Select(m => m.Payload));
        Assert.True(messages[0].MessageId < messages[1].MessageId);
        Assert.Equal(SchemaLedger.EntityFrameworkSource, await ScalarAsync(database, $"SELECT source FROM {SchemaLedger.TableName}"));
    }

    private static StreamingDbContext CreateContext(TestDatabase database)
    {
        var builder = new DbContextOptionsBuilder<StreamingDbContext>();
        _ = database.Engine switch
        {
            "Sqlite" => builder.UseSqlite(database.ConnectionString),
            "PostgreSql" => builder.UseNpgsql(database.ConnectionString),
            "SqlServer" => builder.UseSqlServer(database.ConnectionString),
            "MySql" when database.Driver == TestDrivers.MySqlData => builder.UseMySQL(database.ConnectionString),
            "MySql" or "MariaDb" => builder.UseMySql(database.ConnectionString, ServerVersion.AutoDetect(database.ConnectionString)),
            _ => throw new NotSupportedException(database.Engine),
        };
        return new StreamingDbContext(builder.Options);
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
