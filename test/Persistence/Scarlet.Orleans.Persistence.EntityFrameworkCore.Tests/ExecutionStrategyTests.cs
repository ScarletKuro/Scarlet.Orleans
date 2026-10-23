using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// The application's retrying execution strategy never repeats a write whose acknowledgement was lost after commit.
/// </summary>
public sealed class ExecutionStrategyTests(SqliteDatabaseFixture fixture) : IClassFixture<SqliteDatabaseFixture>
{
    private const string StateName = "state";

    [Theory]
    [InlineData("INSERT", false)]
    [InlineData("UPDATE", false)]
    [InlineData("DELETE", true)]
    public async Task Write_WhenTheAcknowledgementIsLostAfterCommit_IsNotRetriedIntoAConflict(string statement, bool deleteStateOnClear)
    {
        var interceptor = new LoseAcknowledgementInterceptor();
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite(fixture.Database.ConnectionString, sqlite => sqlite.ExecutionStrategy(dependencies => new RetryingStrategy(dependencies)))
            .AddInterceptors(interceptor)
            .Options;
        var store = new EntityFrameworkCoreGrainStateStore<TestDbContext>(new PooledDbContextFactory<TestDbContext>(options), SchemaMode.Apply, TimeProvider.System);
        var storage = FlavourStorage.Create(store, $"service-{Guid.NewGuid():N}", deleteStateOnClear);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        var grainId = GrainId.Create("test-grain", Guid.NewGuid().ToString("N"));
        var state = new GrainState<TestState> { State = new TestState { Value = 1 } };
        if (statement != "INSERT")
        {
            await storage.WriteStateAsync(StateName, grainId, state);
        }

        interceptor.Arm(statement);
        var exception = await Record.ExceptionAsync(() => statement == "DELETE"
            ? storage.ClearStateAsync(StateName, grainId, state)
            : storage.WriteStateAsync(StateName, grainId, state));

        Assert.IsType<LostAcknowledgementException>(exception);
        Assert.Equal(1, interceptor.Executions);
        var read = new GrainState<TestState>();
        await storage.ReadStateAsync(StateName, grainId, read);
        Assert.Equal(statement != "DELETE", read.RecordExists);
    }

    private sealed class LostAcknowledgementException() : DbException("The connection was lost after the statement committed.");

    private sealed class RetryingStrategy(ExecutionStrategyDependencies dependencies) : ExecutionStrategy(dependencies, maxRetryCount: 3, maxRetryDelay: TimeSpan.Zero)
    {
        protected override bool ShouldRetryOn(Exception exception) => exception is LostAcknowledgementException;
    }

    /// <summary>
    /// Lets the armed statement execute and commit, then fails as if its acknowledgement never arrived.
    /// </summary>
    private sealed class LoseAcknowledgementInterceptor : DbCommandInterceptor
    {
        private string? statement;

        public int Executions { get; private set; }

        public void Arm(string armed) => statement = armed;

        public override async ValueTask<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
        {
            result = await base.NonQueryExecutedAsync(command, eventData, result, cancellationToken);
            if (statement is not null && command.CommandText.TrimStart().StartsWith(statement, StringComparison.OrdinalIgnoreCase))
            {
                Executions++;
                if (Executions == 1)
                {
                    throw new LostAcknowledgementException();
                }
            }

            return result;
        }
    }
}
