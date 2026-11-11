using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// The queue operations as plain SQL (<see cref="StreamingSql"/>), each one statement or one transaction.
/// </summary>
/// <param name="dataSource">Opens the connection of each operation.</param>
/// <param name="engineName">The configured engine, or <see langword="null"/> to infer it from the driver.</param>
/// <param name="schemaMode">What to do with the schema in <see cref="InitializeAsync"/>.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
internal sealed class AdoNetStreamQueueStore(DbDataSource dataSource, string? engineName, SchemaMode schemaMode, TimeProvider timeProvider, ILogger logger) : IStreamQueueStore
{
    /// <summary>The most messages one eviction moves, each a parameter of its statements.</summary>
    public const int MaxEvictionBatch = 2 * StreamingSql.ReceiptsPerStatement;

    private RelationalEngine engine;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        engine = engineName is null ? RelationalEngines.FromConnection(connection) : RelationalEngines.Parse(engineName);
        if (engine == RelationalEngine.Sqlite && schemaMode != SchemaMode.None)
        {
            await SqliteJournal.EnsureWriteAheadLogAsync(connection, enable: schemaMode == SchemaMode.Apply, logger, cancellationToken).ConfigureAwait(false);
        }

        if (schemaMode != SchemaMode.None)
        {
            var migrator = new SchemaMigrator(
                StreamingSchema.Component,
                engine,
                SchemaScript.Load(typeof(AdoNetStreamQueueStore).Assembly, engine),
                StreamingSchema.MinimumVersion);
            await migrator.EnsureAsync(connection, apply: schemaMode == SchemaMode.Apply, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task RegisterQueueAsync(StreamQueueKey queue, DateTimeOffset now, DateTimeOffset evictOn, CancellationToken cancellationToken)
    {
        var stored = await TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using (var insert = dataSource.CreateCommand(StreamingSql.InsertQueue(engine)))
            {
                StreamingSql.AddQueue(insert, queue);
                insert.AddParameter("queue_identity", queue.Identity, DbType.Binary);
                insert.AddParameter("service_id", queue.ServiceIdText, DbType.String);
                insert.AddParameter("provider_name", queue.ProviderNameText, DbType.String);
                insert.AddParameter("queue_id", queue.QueueIdText, DbType.String);
                insert.AddParameter("evict_on", evictOn.UtcTicks, DbType.Int64);
                insert.AddParameter("created_on", now, DbType.DateTimeOffset);
                try
                {
                    await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                catch (DbException exception) when (!StreamingSql.SkipsDuplicates(engine) && DbErrors.IsDuplicateKey(exception))
                {
                }
            }

            await using var select = dataSource.CreateCommand(StreamingSql.SelectQueueIdentity);
            StreamingSql.AddQueue(select, queue);
            return await select.ExecuteScalarAsync(ct).ConfigureAwait(false) as byte[];
        }, cancellationToken).ConfigureAwait(false);

        if (stored is null || !stored.AsSpan().SequenceEqual(queue.Identity))
        {
            throw new InvalidOperationException($"Another stream queue is recorded under the hash of {queue}. Its messages cannot be told apart, so the queue is not used.");
        }
    }

    public Task EnqueueAsync(StreamQueueKey queue, Guid messageKey, byte[] payload, DateTimeOffset now, DateTimeOffset expiresOn, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var insert = dataSource.CreateCommand(StreamingSql.InsertMessage(engine));
            StreamingSql.AddQueue(insert, queue);
            insert.AddParameter("message_key", messageKey.ToByteArray(), DbType.Binary);
            insert.AddParameter("now", now.UtcTicks, DbType.Int64);
            insert.AddParameter("expires_on", expiresOn.UtcTicks, DbType.Int64);
            insert.AddParameter("created_on", now, DbType.DateTimeOffset);
            insert.AddParameter("payload", payload, DbType.Binary);
            try
            {
                await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
            catch (DbException exception) when (!StreamingSql.SkipsDuplicates(engine) && DbErrors.IsDuplicateKey(exception))
            {
                // An earlier attempt of this enqueue committed.
            }

            return true;
        }, cancellationToken);

    public async Task<IReadOnlyList<DequeuedMessage>> DequeueAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset visibleOn, CancellationToken cancellationToken)
    {
        var messages = await TransientRetry.ExecuteAsync(timeProvider, ct => engine is RelationalEngine.MySql or RelationalEngine.MariaDb
            ? DequeueMySqlAsync(queue, maxCount, maxAttempts, now, visibleOn, ct)
            : DequeueInOneStatementAsync(queue, maxCount, maxAttempts, now, visibleOn, ct), cancellationToken).ConfigureAwait(false);
        messages.Sort(static (a, b) => a.MessageId.CompareTo(b.MessageId));
        return messages;
    }

    public Task<int> ConfirmAsync(StreamQueueKey queue, IReadOnlyList<MessageReceipt> receipts, CancellationToken cancellationToken) =>
        ForEachChunkAsync(queue, receipts, StreamingSql.ConfirmMessages, now: null, cancellationToken);

    public Task<int> ReleaseAsync(StreamQueueKey queue, IReadOnlyList<MessageReceipt> receipts, DateTimeOffset now, CancellationToken cancellationToken) =>
        ForEachChunkAsync(queue, receipts, StreamingSql.ReleaseMessages, now, cancellationToken);

    public async Task<FailOutcome> FailAsync(StreamQueueKey queue, MessageReceipt receipt, int maxAttempts, DateTimeOffset now, DateTimeOffset removeOn, CancellationToken cancellationToken)
    {
        // A matching receipt is the message's dequeue count, which decides between another attempt and the dead letters.
        if (receipt.Receipt < maxAttempts)
        {
            var retried = await TransientRetry.ExecuteAsync(timeProvider, async ct =>
            {
                await using var update = dataSource.CreateCommand(StreamingSql.RetryMessage);
                StreamingSql.AddQueue(update, queue);
                StreamingSql.AddReceipts(update, [receipt]);
                update.AddParameter("now", now.UtcTicks, DbType.Int64);
                return await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
            return retried == 1 ? FailOutcome.Retried : FailOutcome.Stale;
        }

        // The copy and the delete each match the receipt; if a new attempt took the message between them, their counts
        // differ, and the transaction rolls back.
        return await InTransactionAsync(async (connection, transaction, ct) =>
        {
            await using var copy = Command(connection, transaction, StreamingSql.DeadLetterMessage);
            StreamingSql.AddQueue(copy, queue);
            StreamingSql.AddReceipts(copy, [receipt]);
            copy.AddParameter("dead_on", now, DbType.DateTimeOffset);
            copy.AddParameter("remove_on", removeOn.UtcTicks, DbType.Int64);
            var copied = await copy.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            await using var delete = Command(connection, transaction, StreamingSql.DeleteMessage);
            StreamingSql.AddQueue(delete, queue);
            StreamingSql.AddReceipts(delete, [receipt]);
            var deleted = await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return copied == 1 && deleted == 1 ? (FailOutcome.DeadLettered, true) : (FailOutcome.Stale, false);
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryClaimEvictionAsync(StreamQueueKey queue, DateTimeOffset now, DateTimeOffset next, CancellationToken cancellationToken) =>
        await TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var update = dataSource.CreateCommand(StreamingSql.ClaimEviction);
            StreamingSql.AddQueue(update, queue);
            update.AddParameter("now", now.UtcTicks, DbType.Int64);
            update.AddParameter("next", next.UtcTicks, DbType.Int64);
            return await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false) == 1;

    public Task<int> EvictAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset removeOn, CancellationToken cancellationToken)
    {
        return InTransactionAsync(async (connection, transaction, ct) =>
        {
            var ids = new List<long>();
            await using (var select = Command(connection, transaction, StreamingSql.SelectEvictable(engine)))
            {
                AddEvictable(select);
                select.AddParameter("max_count", Math.Min(maxCount, MaxEvictionBatch), DbType.Int32);
                await using var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    ids.Add(reader.GetInt64(0));
                }
            }

            if (ids.Count == 0)
            {
                return (0, true);
            }

            await using var copy = Command(connection, transaction, StreamingSql.DeadLetterEvictable(ids.Count));
            AddEvictable(copy);
            StreamingSql.AddIds(copy, ids);
            copy.AddParameter("dead_on", now, DbType.DateTimeOffset);
            copy.AddParameter("remove_on", removeOn.UtcTicks, DbType.Int64);
            var copied = await copy.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            await using var delete = Command(connection, transaction, StreamingSql.DeleteEvictable(ids.Count));
            AddEvictable(delete);
            StreamingSql.AddIds(delete, ids);
            var deleted = await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

            // A message dequeued between the copy and the delete stays a message; the next eviction tries again.
            return copied == deleted ? (deleted, true) : (0, false);
        }, cancellationToken);

        void AddEvictable(DbCommand command)
        {
            StreamingSql.AddQueue(command, queue);
            command.AddParameter("now", now.UtcTicks, DbType.Int64);
            command.AddParameter("max_attempts", maxAttempts, DbType.Int32);
        }
    }

    public Task<int> EvictDeadLettersAsync(StreamQueueKey queue, int maxCount, DateTimeOffset now, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var delete = dataSource.CreateCommand(StreamingSql.EvictDeadLetters(engine));
            StreamingSql.AddQueue(delete, queue);
            delete.AddParameter("now", now.UtcTicks, DbType.Int64);
            delete.AddParameter("max_count", maxCount, DbType.Int32);
            return await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);

    private async Task<List<DequeuedMessage>> DequeueInOneStatementAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset visibleOn, CancellationToken cancellationToken)
    {
        await using var update = dataSource.CreateCommand(StreamingSql.Dequeue(engine));
        AddDequeueable(update, queue, maxCount, maxAttempts, now);
        update.AddParameter("visible_on", visibleOn.UtcTicks, DbType.Int64);
        var messages = new List<DequeuedMessage>();
        await using var reader = await update.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (await SqlTransactions.NextResultWithColumnsAsync(reader, first: true, cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                messages.Add(StreamingSql.ReadMessage(reader));
            }
        }

        return messages;
    }

    /// <summary>A dequeue in two commands, which lock the batch, then update it and commit (<see cref="MySqlStreamingBatch"/>).</summary>
    private async Task<List<DequeuedMessage>> DequeueMySqlAsync(StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now, DateTimeOffset visibleOn, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var committed = false;
        try
        {
            var ids = new List<long>();
            await using (var select = connection.CreateCommand())
            {
                select.CommandText = MySqlStreamingBatch.Lock;
                AddDequeueable(select, queue, maxCount, maxAttempts, now);
                await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (await SqlTransactions.NextResultWithColumnsAsync(reader, first: true, cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        ids.Add(reader.GetInt64(0));
                    }
                }
            }

            var messages = new List<DequeuedMessage>(ids.Count);
            if (ids.Count == 0)
            {
                return messages;
            }

            await using (var update = MySqlStreamingBatch.CreateUpdate(connection, queue, ids, visibleOn))
            await using (var reader = await update.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (await SqlTransactions.NextResultWithColumnsAsync(reader, first: true, cancellationToken).ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        messages.Add(StreamingSql.ReadMessage(reader));
                    }
                }

                while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                {
                }
            }

            committed = true;
            return messages;
        }
        finally
        {
            if (!committed)
            {
                await SqlTransactions.RollbackAsync(connection).ConfigureAwait(false);
            }
        }
    }

    private static void AddDequeueable(DbCommand command, StreamQueueKey queue, int maxCount, int maxAttempts, DateTimeOffset now)
    {
        StreamingSql.AddQueue(command, queue);
        command.AddParameter("now", now.UtcTicks, DbType.Int64);
        command.AddParameter("max_attempts", maxAttempts, DbType.Int32);
        command.AddParameter("max_count", maxCount, DbType.Int32);
    }

    /// <summary>
    /// Runs <paramref name="sql"/> for each chunk of receipts, each its own statement, and sums the rows it changed.
    /// Each chunk is idempotent, so a repeated one changes nothing.
    /// </summary>
    private async Task<int> ForEachChunkAsync(StreamQueueKey queue, IReadOnlyList<MessageReceipt> receipts, Func<int, string> sql, DateTimeOffset? now, CancellationToken cancellationToken)
    {
        var changed = 0;
        foreach (var chunk in receipts.Chunk(StreamingSql.ReceiptsPerStatement))
        {
            changed += await TransientRetry.ExecuteAsync(timeProvider, async ct =>
            {
                await using var command = dataSource.CreateCommand(sql(chunk.Length));
                StreamingSql.AddQueue(command, queue);
                StreamingSql.AddReceipts(command, chunk);
                if (now is { } value)
                {
                    command.AddParameter("now", value.UtcTicks, DbType.Int64);
                }

                return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
        }

        return changed;
    }

    /// <summary>
    /// Runs <paramref name="body"/> in a read-committed transaction on its own connection, and commits it if the body
    /// says so, or rolls it back. A transient error rolls the transaction back, as it is disposed, before the whole
    /// operation is repeated (design section 4.7).
    /// </summary>
    private Task<T> InTransactionAsync<T>(Func<DbConnection, DbTransaction, CancellationToken, Task<(T Result, bool Commit)>> body, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var connection = await dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct).ConfigureAwait(false);
            var (result, commit) = await body(connection, transaction, ct).ConfigureAwait(false);
            if (commit)
            {
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            else
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
            }

            return result;
        }, cancellationToken);

    private static DbCommand Command(DbConnection connection, DbTransaction transaction, string text)
    {
        var command = connection.CreateCommand();
        command.CommandText = text;
        command.Transaction = transaction;
        return command;
    }
}
