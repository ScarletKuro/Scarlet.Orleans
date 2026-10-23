using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Orleans;
using Orleans.Runtime;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// The application's retrying execution strategy, repeating a write whose first attempt committed but whose
/// acknowledgement was lost.
/// </summary>
public sealed class ExecutionStrategyTests(SqliteDatabaseFixture fixture) : IClassFixture<SqliteDatabaseFixture>
{
    private const string StateName = "state";

    [Fact]
    public async Task Update_RepeatedAfterALostAcknowledgement_Succeeds()
    {
        var (interceptor, storage) = await ArrangeAsync(deleteStateOnClear: false);
        var grainId = NewGrainId();
        var state = new GrainState<TestState> { State = new TestState { Value = 1 } };
        await storage.WriteStateAsync(StateName, grainId, state);

        interceptor.Arm("UPDATE");
        state.State.Value = 2;
        await storage.WriteStateAsync(StateName, grainId, state);

        Assert.Equal(2, interceptor.Executions);
        await AssertStoredAsync(storage, grainId, state);
    }

    /// <summary>
    /// EF Core reports the failed <c>SaveChanges</c> rather than repeating the insert; the store finds the committed row
    /// and the storage settles the write from its ETag, so the outcome is the same as for a repeat.
    /// </summary>
    [Fact]
    public async Task Insert_AfterALostAcknowledgement_Succeeds()
    {
        var (interceptor, storage) = await ArrangeAsync(deleteStateOnClear: false);
        var grainId = NewGrainId();
        var state = new GrainState<TestState> { State = new TestState { Value = 2 } };

        interceptor.Arm("INSERT");
        await storage.WriteStateAsync(StateName, grainId, state);

        Assert.True(interceptor.Executions >= 1);
        await AssertStoredAsync(storage, grainId, state);
    }

    private static async Task AssertStoredAsync(RelationalGrainStorage storage, GrainId grainId, GrainState<TestState> written)
    {
        var read = new GrainState<TestState>();
        await storage.ReadStateAsync(StateName, grainId, read);
        Assert.Equal(written.ETag, read.ETag);
        Assert.Equal(written.State!.Value, read.State!.Value);
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
        var read = new GrainState<TestState>();
        await storage.ReadStateAsync(StateName, grainId, read);
        Assert.False(read.RecordExists);
    }

    private static GrainId NewGrainId() => GrainId.Create("test-grain", Guid.NewGuid().ToString("N"));

    private async Task<(LoseAcknowledgementInterceptor Interceptor, RelationalGrainStorage Storage)> ArrangeAsync(bool deleteStateOnClear)
    {
        await FlavourStorage.EnsureSchemaAsync(fixture.Database, TestContext.Current.CancellationToken);
        var interceptor = new LoseAcknowledgementInterceptor();
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(fixture.Database.ConnectionString, sqlite => sqlite.ExecutionStrategy(dependencies => new RetryingStrategy(dependencies)))
            .AddInterceptors(interceptor)
            .Options;
        var store = new EntityFrameworkCoreGrainStateStore<TestDbContext>(new PooledDbContextFactory<TestDbContext>(options), SchemaMode.Validate);
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
}
