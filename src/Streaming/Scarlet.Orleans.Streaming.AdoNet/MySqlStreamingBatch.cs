using System.Data;
using System.Data.Common;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// MySQL and MariaDB dequeue in two commands: the first starts a read-committed transaction and locks the batch with a
/// skip-locked read, and the second updates those rows by ID, reads them back and commits.
/// </summary>
/// <remarks>
/// MySQL rejects an update whose subquery reads the target table, and <c>LIMIT</c> in an <c>IN</c> subquery, and has no
/// <c>RETURNING</c>, so it cannot dequeue in one statement. The transaction is started in SQL; a dequeue that does not
/// commit rolls back explicitly (<see cref="SqlTransactions.RollbackAsync"/>). Read committed takes no gap locks, which
/// would block the neighbouring queues.
/// </remarks>
internal static class MySqlStreamingBatch
{
    private const string Messages = StreamingSchema.MessageTableName;

    public const string Lock =
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION; " +
        $"SELECT message_id FROM {Messages} WHERE queue_hash = @queue_hash AND visible_on <= @now AND expires_on > @now AND dequeued < @max_attempts " +
        "ORDER BY message_id LIMIT @max_count FOR UPDATE SKIP LOCKED";

    public static DbCommand CreateUpdate(DbConnection connection, StreamQueueKey queue, IReadOnlyList<long> ids, DateTimeOffset visibleOn)
    {
        var command = connection.CreateCommand();
        var list = StreamingSql.IdList(ids.Count);
        StreamingSql.AddQueue(command, queue);
        StreamingSql.AddIds(command, ids);
        command.AddParameter("visible_on", visibleOn.UtcTicks, DbType.Int64);
        command.CommandText =
            $"UPDATE {Messages} SET dequeued = dequeued + 1, visible_on = @visible_on WHERE queue_hash = @queue_hash AND message_id IN ({list}); " +
            $"SELECT message_id, message_key, dequeued, expires_on, payload FROM {Messages} WHERE queue_hash = @queue_hash AND message_id IN ({list}); " +
            "COMMIT";
        return command;
    }
}
