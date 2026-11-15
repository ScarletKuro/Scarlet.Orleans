using System.Data;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Scarlet.Orleans.Streaming.Tests;

/// <summary>A database lock, rather than a scheduling delay, reproduces an older message being overtaken.</summary>
public abstract class DequeueOrderingTests(DatabaseFixture fixture, bool snapshot = false)
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Dequeue_PreservesOrder_WithoutBlockingOtherQueues(bool sameQueue)
    {
        var ct = TestContext.Current.CancellationToken;
        var database = await fixture.CreateDatabaseAsync();
        if (snapshot)
        {
            await using var configure = database.DataSource.CreateCommand("DECLARE @sql nvarchar(max) = N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + N' SET READ_COMMITTED_SNAPSHOT ON'; EXEC (@sql)");
            await configure.ExecuteNonQueryAsync(ct);
        }

        var store = FlavourStreaming.CreateStore(database, TestSchemaMode.Apply);
        await store.InitializeAsync(ct);
        var queue = StreamQueueKey.Create(Guid.NewGuid().ToString("N"), "order", "locked");
        var other = StreamQueueKey.Create(Guid.NewGuid().ToString("N"), "order", "other");
        var now = DateTimeOffset.UtcNow;
        foreach (var key in new[] { queue, other })
        {
            await store.RegisterQueueAsync(key, now, now.AddHours(1), ct);
            for (var i = 0; i < 3; i++)
                await store.EnqueueAsync(key, Guid.NewGuid(), BitConverter.GetBytes(i), now, now.AddHours(1), ct);
        }

        await using var connection = await database.DataSource.OpenConnectionAsync(ct);
        await using var held = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct);
        await using var block = connection.CreateCommand();
        block.Transaction = held;
        block.CommandText = "SELECT MIN(message_id) FROM orleans_stream_message WHERE queue_hash = @queue_hash";
        StreamingSql.AddQueue(block, queue);
        var id = Convert.ToInt64(await block.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
        block.Parameters.Clear();
        block.CommandText = "UPDATE orleans_stream_message" + (database.Engine == "SqlServer" ? " WITH (ROWLOCK)" : "") + " SET expires_on = expires_on + 1 WHERE message_id = @id";
        block.AddParameter("id", id, DbType.Int64);
        await block.ExecuteNonQueryAsync(ct);
        block.Parameters.Clear();
        block.CommandText = database.Engine switch
        {
            "SqlServer" => "SELECT @@SPID",
            "PostgreSql" => "SELECT pg_backend_pid()",
            _ => "SELECT CONNECTION_ID()",
        };
        var blocker = Convert.ToInt32(await block.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);

        var dequeue = store.DequeueAsync(sameQueue ? queue : other, 3, 5, now, now.AddMinutes(1), ct);
        var waited = false;
        try
        {
            // The old skip-locked read completes while the lock is held. A FIFO read waits for this transaction.
            // Observe the database's lock graph so neither test result depends on how quickly the task is scheduled.
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
            deadline.CancelAfter(TimeSpan.FromSeconds(30));
            while (!dequeue.IsCompleted)
            {
                await using var waiting = database.DataSource.CreateCommand(database.Engine switch
                {
                    "SqlServer" => "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE database_id = DB_ID() AND blocking_session_id = @blocker",
                    "PostgreSql" => "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND @blocker = ANY(pg_blocking_pids(pid))",
                    "MySql" => "SELECT COUNT(*) FROM performance_schema.data_lock_waits w JOIN information_schema.INNODB_TRX t ON t.trx_id = w.BLOCKING_ENGINE_TRANSACTION_ID WHERE t.trx_mysql_thread_id = @blocker",
                    _ => "SELECT COUNT(*) FROM information_schema.INNODB_LOCK_WAITS w JOIN information_schema.INNODB_TRX t ON t.trx_id = w.blocking_trx_id WHERE t.trx_mysql_thread_id = @blocker",
                });
                waiting.AddParameter("blocker", blocker, DbType.Int32);
                if (Convert.ToInt32(await waiting.ExecuteScalarAsync(deadline.Token), System.Globalization.CultureInfo.InvariantCulture) > 0)
                {
                    waited = true;
                    break;
                }
                await Task.Delay(20, deadline.Token);
            }
        }
        finally { await held.RollbackAsync(CancellationToken.None); }

        var batch = await dequeue;
        Assert.Equal([0, 1, 2], batch.Select(m => BitConverter.ToInt32(m.Payload)));
        if (!sameQueue) Assert.False(waited, "A lock in another queue blocked this dequeue.");
    }
}
