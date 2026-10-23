using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Writes under the application's retrying execution strategy, when a write's acknowledgement is lost after it committed.
/// </summary>
public sealed class ExecutionStrategyTests(SqliteDatabaseFixture fixture) : IClassFixture<SqliteDatabaseFixture>
{
    private const string StateName = "state";

    [Fact]
    public async Task Update_WithALostAcknowledgement_RunsOnceAndReportsTheFailure()
    {
        var (interceptor, storage) = await ArrangeAsync(deleteStateOnClear: false);
        var grainId = NewGrainId();
        var state = new GrainState<TestState> { State = new TestState { Value = 1 } };
        await storage.WriteStateAsync(StateName, grainId, state);

        interceptor.Arm("UPDATE");
        var attempted = new GrainState<TestState> { State = new TestState { Value = 2 }, ETag = state.ETag };
        var exception = await Record.ExceptionAsync(() => storage.WriteStateAsync(StateName, grainId, attempted));

        Assert.IsType<LostAcknowledgementException>(exception);
        Assert.Equal(1, interceptor.Executions);
        Assert.Equal(2, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Fact]
    public async Task Delete_WithALostAcknowledgement_RunsOnceAndReportsTheFailure()
    {
        var (interceptor, storage) = await ArrangeAsync(deleteStateOnClear: true);
        var grainId = NewGrainId();
        var state = new GrainState<TestState> { State = new TestState { Value = 1 } };
        await storage.WriteStateAsync(StateName, grainId, state);

        interceptor.Arm("DELETE");
        var exception = await Record.ExceptionAsync(() => storage.ClearStateAsync(StateName, grainId, state));

        Assert.IsType<LostAcknowledgementException>(exception);
        Assert.Equal(1, interceptor.Executions);
        Assert.False((await ReadAsync(storage, grainId)).RecordExists);
    }

    /// <summary>
    /// The lost acknowledgement comes from no known driver, so the insert falls back to checking for the row, which
    /// holds the ETag this write generated.
    /// </summary>
    [Fact]
    public async Task Insert_WithALostAcknowledgement_RunsOnce()
    {
        var (interceptor, storage) = await ArrangeAsync(deleteStateOnClear: false);
        var grainId = NewGrainId();
        var state = new GrainState<TestState> { State = new TestState { Value = 2 } };

        interceptor.Arm("INSERT");
        await storage.WriteStateAsync(StateName, grainId, state);

        Assert.Equal(1, interceptor.Executions);
        var read = await ReadAsync(storage, grainId);
        Assert.Equal(state.ETag, read.ETag);
        Assert.Equal(2, read.State!.Value);
    }

    [Fact]
    public async Task DuplicateInsert_WhenTheExistingRowIsDeletedBeforeClassification_ReportsAConflict()
    {
        await FlavourStorage.EnsureSchemaAsync(fixture.Database, TestContext.Current.CancellationToken);
        var serviceId = $"service-{Guid.NewGuid():N}";
        var grainId = NewGrainId();
        var plain = FlavourStorage.Create(fixture.Database, serviceId, deleteStateOnClear: false, TestSchemaMode.Validate);
        await plain.InitializeAsync(TestContext.Current.CancellationToken);
        await plain.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState { Value = 1 } });
        var key = GrainStateKey.Create(serviceId, StateName, grainId);
        var deleteOnFailure = new DeleteOnFailedInsertInterceptor(fixture.Database, key.Hash);
        var options = new DbContextOptionsBuilder<TestDbContext>().UseSqlite(fixture.Database.ConnectionString).AddInterceptors(deleteOnFailure).Options;
        var storage = FlavourStorage.Create(
            new EntityFrameworkCoreGrainStateStore<TestDbContext>(new PooledDbContextFactory<TestDbContext>(options), SchemaMode.Validate, TimeProvider.System),
            serviceId,
            deleteStateOnClear: false);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);

        var exception = await Record.ExceptionAsync(() => storage.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState { Value = 2 } }));

        Assert.True(deleteOnFailure.Deleted);
        Assert.IsType<InconsistentStateException>(exception);
    }

    private static GrainId NewGrainId() => GrainId.Create("test-grain", Guid.NewGuid().ToString("N"));

    private static async Task<GrainState<TestState>> ReadAsync(RelationalGrainStorage storage, GrainId grainId)
    {
        var read = new GrainState<TestState>();
        await storage.ReadStateAsync(StateName, grainId, read);
        return read;
    }

    private async Task<(LoseAcknowledgementInterceptor Interceptor, RelationalGrainStorage Storage)> ArrangeAsync(bool deleteStateOnClear)
    {
        await FlavourStorage.EnsureSchemaAsync(fixture.Database, TestContext.Current.CancellationToken);
        var interceptor = new LoseAcknowledgementInterceptor();
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(fixture.Database.ConnectionString, sqlite => sqlite.ExecutionStrategy(dependencies => new RetryingStrategy(dependencies)))
            .AddInterceptors(interceptor)
            .Options;
        var store = new EntityFrameworkCoreGrainStateStore<TestDbContext>(new PooledDbContextFactory<TestDbContext>(options), SchemaMode.Validate, TimeProvider.System);
        var storage = FlavourStorage.Create(store, $"service-{Guid.NewGuid():N}", deleteStateOnClear);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        return (interceptor, storage);
    }

    private sealed class LostAcknowledgementException() : DbException("The connection was lost after the statement committed.");

    private sealed class RetryingStrategy(ExecutionStrategyDependencies dependencies) : ExecutionStrategy(dependencies, maxRetryCount: 3, maxRetryDelay: TimeSpan.Zero)
    {
        protected override bool ShouldRetryOn(Exception exception) => exception is LostAcknowledgementException;
    }

    /// <summary>
    /// Lets the armed statement execute and commit, then fails its first execution as if the acknowledgement never arrived.
    /// </summary>
    private sealed class LoseAcknowledgementInterceptor : DbCommandInterceptor
    {
        private string? statement;

        public int Executions { get; private set; }

        public void Arm(string armed) => statement = armed;

        public override async ValueTask<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
        {
            result = await base.NonQueryExecutedAsync(command, eventData, result, cancellationToken);
            Observe(command);
            return result;
        }

        public override async ValueTask<DbDataReader> ReaderExecutedAsync(DbCommand command, CommandExecutedEventData eventData, DbDataReader result, CancellationToken cancellationToken = default)
        {
            result = await base.ReaderExecutedAsync(command, eventData, result, cancellationToken);
            if (Matches(command))
            {
                // Consume the results, so the statement has completed before its acknowledgement is lost.
                while (await result.NextResultAsync(cancellationToken))
                {
                }

                await result.DisposeAsync();
            }

            Observe(command);
            return result;
        }

        private bool Matches(DbCommand command) =>
            statement is not null && command.CommandText.TrimStart().StartsWith(statement, StringComparison.OrdinalIgnoreCase);

        private void Observe(DbCommand command)
        {
            if (Matches(command))
            {
                Executions++;
                if (Executions == 1)
                {
                    throw new LostAcknowledgementException();
                }
            }
        }
    }

    /// <summary>
    /// Deletes the existing row as soon as an insert fails, before the store can classify the failure.
    /// </summary>
    private sealed class DeleteOnFailedInsertInterceptor(TestDatabase database, byte[] keyHash) : DbCommandInterceptor
    {
        public bool Deleted { get; private set; }

        public override async Task CommandFailedAsync(DbCommand command, CommandErrorEventData eventData, CancellationToken cancellationToken = default)
        {
            if (command.CommandText.TrimStart().StartsWith("INSERT", StringComparison.OrdinalIgnoreCase))
            {
                await using var delete = database.DataSource.CreateCommand($"DELETE FROM {PersistenceSchema.TableName} WHERE key_hash = @key_hash");
                delete.AddParameter("key_hash", keyHash, System.Data.DbType.Binary);
                Deleted = await delete.ExecuteNonQueryAsync(cancellationToken) == 1;
            }

            await base.CommandFailedAsync(command, eventData, cancellationToken);
        }
    }
}
