using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.Extensions.Logging.Abstractions;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Persistence.Tests;

public sealed class BareDbContext(DbContextOptions<BareDbContext> options) : DbContext(options);

/// <summary>
/// The EF Core model against the SQL schema scripts, and the EF migration step that records schema versions.
/// </summary>
public abstract class ModelTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ModelSchema_MatchesTheScript()
    {
        var fromScript = await fixture.CreateDatabaseAsync();
        var fromModel = await fixture.CreateDatabaseAsync();
        await FlavourStorage.Create(fromScript, "model-tests", deleteStateOnClear: false, TestSchemaMode.Apply).InitializeAsync(CancellationToken);
        await using (var context = await FlavourStorage.ContextFactory(fromModel).CreateDbContextAsync(CancellationToken))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
        }

        var expected = await DescribeTableAsync(fromScript);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, await DescribeTableAsync(fromModel));
    }

    [Fact]
    public async Task MigrationStep_RecordsTheVersion_ThenValidatePasses()
    {
        var database = await fixture.CreateDatabaseAsync();
        await using (var context = await FlavourStorage.ContextFactory(database).CreateDbContextAsync(CancellationToken))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
            var migration = new MigrationBuilder(context.Database.ProviderName);
            migration.ApplyOrleansPersistenceSchema(1);
            foreach (var operation in migration.Operations.Cast<SqlOperation>())
            {
                await context.Database.ExecuteSqlRawAsync(operation.Sql, CancellationToken);
            }
        }

        var storage = FlavourStorage.Create(database, "model-tests", deleteStateOnClear: false, TestSchemaMode.Validate);
        await storage.InitializeAsync(CancellationToken);

        Assert.Equal(SchemaLedger.EntityFrameworkSource, await ScalarAsync(database, $"SELECT source FROM {SchemaLedger.TableName}"));
    }

    [Fact]
    public async Task Validate_WithoutTheMigrationStep_Fails()
    {
        var database = await fixture.CreateDatabaseAsync();
        await using (var context = await FlavourStorage.ContextFactory(database).CreateDbContextAsync(CancellationToken))
        {
            await context.Database.EnsureCreatedAsync(CancellationToken);
        }

        await Assert.ThrowsAsync<SchemaVersionException>(
            () => FlavourStorage.Create(database, "model-tests", deleteStateOnClear: false, TestSchemaMode.Validate).InitializeAsync(CancellationToken));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void MigrationStep_ForAnUnknownVersion_Throws(int version)
    {
        fixture.Require();
        using var context = FlavourStorage.ContextFactory(fixture.Database).CreateDbContext();

        Assert.Throws<ArgumentOutOfRangeException>(() => new MigrationBuilder(context.Database.ProviderName).ApplyOrleansPersistenceSchema(version));
    }

    [Fact]
    public async Task Context_WithoutTheOrleansModel_FailsAtStartup()
    {
        var options = FlavourStorage.Configure(new DbContextOptionsBuilder<BareDbContext>(), fixture.Database).Options;
        var store = new EntityFrameworkCoreGrainStateStore<BareDbContext>(new PooledDbContextFactory<BareDbContext>(options), SchemaMode.Validate, TimeProvider.System);
        var storage = new RelationalGrainStorage("test", "model-tests", store, new JsonTestSerializer(), new TestActivatorProvider(), false, 0, TimeProvider.System, NullLogger<RelationalGrainStorage>.Instance);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => storage.InitializeAsync(CancellationToken));

        Assert.Contains(nameof(ScarletPersistenceModelBuilderExtensions.ApplyOrleansPersistenceModel), exception.Message, StringComparison.Ordinal);
    }

    private static async Task<List<string>> DescribeTableAsync(TestDatabase database)
    {
        var sql = database.Engine switch
        {
            "Sqlite" => $"SELECT name, type, \"notnull\", pk FROM pragma_table_info('{PersistenceSchema.TableName}') ORDER BY name",
            "PostgreSql" => $"""
                SELECT c.column_name, c.data_type, c.is_nullable, c.character_maximum_length, c.datetime_precision,
                    (SELECT tc.constraint_name FROM information_schema.key_column_usage k
                        JOIN information_schema.table_constraints tc ON tc.constraint_name = k.constraint_name AND tc.table_schema = k.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY' AND k.table_name = c.table_name AND k.column_name = c.column_name AND k.table_schema = c.table_schema)
                FROM information_schema.columns c
                WHERE c.table_schema = current_schema() AND c.table_name = '{PersistenceSchema.TableName}'
                ORDER BY c.column_name
                """,
            "SqlServer" => $"""
                SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.CHARACTER_MAXIMUM_LENGTH, c.DATETIME_PRECISION,
                    (SELECT tc.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                        WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND k.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.TABLE_NAME = '{PersistenceSchema.TableName}'
                ORDER BY c.COLUMN_NAME
                """,
            "MySql" or "MariaDb" => $"""
                SELECT column_name, column_type, is_nullable, column_key, character_set_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = '{PersistenceSchema.TableName}'
                ORDER BY column_name
                """,
            _ => throw new NotSupportedException(database.Engine),
        };

        await using var command = database.DataSource.CreateCommand(sql);
        await using var reader = await command.ExecuteReaderAsync(CancellationToken);
        var columns = new List<string>();
        while (await reader.ReadAsync(CancellationToken))
        {
            columns.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
        }

        return columns;
    }

    private static async Task<object?> ScalarAsync(TestDatabase database, string sql)
    {
        await using DbCommand command = database.DataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync(CancellationToken);
    }
}
