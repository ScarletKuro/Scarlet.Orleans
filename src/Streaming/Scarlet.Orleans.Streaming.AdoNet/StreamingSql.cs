using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// The streaming SQL. Statements are standard and shared by every engine, except the dequeue, which needs each engine's
/// own locking read (design section 5.6), and the bounds on eviction batches.
/// </summary>
internal static class StreamingSql
{
    /// <summary>
    /// The most receipts or message IDs one statement matches, two parameters each, under SQL Server's 2,100.
    /// </summary>
    public const int ReceiptsPerStatement = 500;

    private const string Queues = StreamingSchema.QueueTableName;

    private const string Messages = StreamingSchema.MessageTableName;

    private const string DeadLetters = StreamingSchema.DeadLetterTableName;

    private const string Columns = "message_id, message_key, dequeued, expires_on, payload";

    private const string Dequeueable =
        "queue_hash = @queue_hash AND visible_on <= @now AND expires_on > @now AND dequeued < @max_attempts";

    /// <summary>Messages to evict: expired, or out of attempts with the last attempt's visibility over.</summary>
    private const string Evictable =
        "queue_hash = @queue_hash AND (expires_on <= @now OR (dequeued >= @max_attempts AND visible_on <= @now))";

    private const string InsertQueueStatement =
        $"INSERT INTO {Queues} (queue_hash, queue_identity, service_id, provider_name, queue_id, evict_on, created_on) " +
        "VALUES (@queue_hash, @queue_identity, @service_id, @provider_name, @queue_id, @evict_on, @created_on)";

    private const string InsertMessageStatement =
        $"INSERT INTO {Messages} (queue_hash, message_key, dequeued, visible_on, expires_on, created_on, payload) " +
        "VALUES (@queue_hash, @message_key, 0, @now, @expires_on, @created_on, @payload)";

    public const string SelectQueueIdentity = $"SELECT queue_identity FROM {Queues} WHERE queue_hash = @queue_hash";

    public const string ClaimEviction = $"UPDATE {Queues} SET evict_on = @next WHERE queue_hash = @queue_hash AND evict_on <= @now";

    public const string RetryMessage =
        $"UPDATE {Messages} SET visible_on = @now WHERE queue_hash = @queue_hash AND message_id = @s0 AND dequeued = @r0";

    public const string DeadLetterMessage =
        $"INSERT INTO {DeadLetters} (queue_hash, message_id, message_key, dequeued, expires_on, created_on, payload, dead_on, remove_on) " +
        $"SELECT queue_hash, message_id, message_key, dequeued, expires_on, created_on, payload, @dead_on, @remove_on FROM {Messages} " +
        "WHERE queue_hash = @queue_hash AND message_id = @s0 AND dequeued = @r0";

    public const string DeleteMessage = $"DELETE FROM {Messages} WHERE queue_hash = @queue_hash AND message_id = @s0 AND dequeued = @r0";

    /// <summary>
    /// Inserts the queue row unless one exists. PostgreSQL and SQLite skip an existing row; the other engines report it
    /// as a duplicate key, which the store ignores, since the identity check that follows settles it.
    /// </summary>
    public static string InsertQueue(RelationalEngine engine) =>
        SkipsDuplicates(engine) ? InsertQueueStatement + " ON CONFLICT (queue_hash) DO NOTHING" : InsertQueueStatement;

    /// <summary>
    /// Inserts a message. A duplicate <c>message_key</c> is an earlier attempt of the same enqueue: PostgreSQL and SQLite
    /// skip it, and the other engines report it as a duplicate key, which the store treats as success.
    /// </summary>
    public static string InsertMessage(RelationalEngine engine) =>
        SkipsDuplicates(engine) ? InsertMessageStatement + " ON CONFLICT (message_key) DO NOTHING" : InsertMessageStatement;

    public static bool SkipsDuplicates(RelationalEngine engine) => engine is RelationalEngine.PostgreSql or RelationalEngine.Sqlite;

    /// <summary>
    /// The one-statement dequeue of PostgreSQL, SQL Server and SQLite. Results are sorted by the caller, since no engine
    /// promises <c>RETURNING</c> or <c>OUTPUT</c> order.
    /// </summary>
    public static string Dequeue(RelationalEngine engine) => engine switch
    {
        RelationalEngine.PostgreSql =>
            $"UPDATE {Messages} m SET dequeued = m.dequeued + 1, visible_on = @visible_on " +
            $"FROM (SELECT message_id FROM {Messages} WHERE {Dequeueable} ORDER BY message_id LIMIT @max_count FOR UPDATE SKIP LOCKED) s " +
            $"WHERE m.queue_hash = @queue_hash AND m.message_id = s.message_id RETURNING m.message_id, m.message_key, m.dequeued, m.expires_on, m.payload",

        // READPAST is valid only at READ COMMITTED or REPEATABLE READ, and under READ_COMMITTED_SNAPSHOT only with
        // READCOMMITTEDLOCK, so the session's level is set and the hint added; with RCSI off, the hint is the default.
        RelationalEngine.SqlServer =>
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; " +
            $"WITH batch AS (SELECT TOP (@max_count) * FROM {Messages} WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK) WHERE {Dequeueable} ORDER BY message_id) " +
            $"UPDATE batch SET dequeued = dequeued + 1, visible_on = @visible_on OUTPUT inserted.message_id, inserted.message_key, inserted.dequeued, inserted.expires_on, inserted.payload",

        // A single writer, so there is nothing to skip.
        RelationalEngine.Sqlite =>
            $"UPDATE {Messages} SET dequeued = dequeued + 1, visible_on = @visible_on " +
            $"WHERE message_id IN (SELECT message_id FROM {Messages} WHERE {Dequeueable} ORDER BY message_id LIMIT @max_count) " +
            $"RETURNING {Columns}",

        _ => throw new NotSupportedException($"{engine} dequeues in two commands ({nameof(MySqlStreamingBatch)})."),
    };

    /// <summary>The IDs of up to <c>@max_count</c> messages to evict, in ID order.</summary>
    public static string SelectEvictable(RelationalEngine engine) => engine == RelationalEngine.SqlServer
        ? $"SELECT TOP (@max_count) message_id FROM {Messages} WHERE {Evictable} ORDER BY message_id"
        : $"SELECT message_id FROM {Messages} WHERE {Evictable} ORDER BY message_id LIMIT @max_count";

    /// <summary>Copies the listed messages, if still evictable, to the dead letters.</summary>
    public static string DeadLetterEvictable(int count) =>
        $"INSERT INTO {DeadLetters} (queue_hash, message_id, message_key, dequeued, expires_on, created_on, payload, dead_on, remove_on) " +
        $"SELECT queue_hash, message_id, message_key, dequeued, expires_on, created_on, payload, @dead_on, @remove_on FROM {Messages} " +
        $"WHERE {Evictable} AND message_id IN ({IdList(count)})";

    /// <summary>Deletes the listed messages, if still evictable.</summary>
    public static string DeleteEvictable(int count) => $"DELETE FROM {Messages} WHERE {Evictable} AND message_id IN ({IdList(count)})";

    /// <summary>Deletes up to <c>@max_count</c> dead letters due for removal.</summary>
    public static string EvictDeadLetters(RelationalEngine engine) => engine switch
    {
        RelationalEngine.SqlServer => $"DELETE TOP (@max_count) FROM {DeadLetters} WHERE queue_hash = @queue_hash AND remove_on <= @now",
        RelationalEngine.MySql or RelationalEngine.MariaDb => $"DELETE FROM {DeadLetters} WHERE queue_hash = @queue_hash AND remove_on <= @now ORDER BY remove_on LIMIT @max_count",
        _ => $"DELETE FROM {DeadLetters} WHERE queue_hash = @queue_hash AND message_id IN " +
            $"(SELECT message_id FROM {DeadLetters} WHERE queue_hash = @queue_hash AND remove_on <= @now ORDER BY remove_on LIMIT @max_count)",
    };

    /// <summary>Deletes the messages whose receipts match.</summary>
    public static string ConfirmMessages(int count) => $"DELETE FROM {Messages} WHERE queue_hash = @queue_hash AND ({ReceiptList(count)})";

    /// <summary>Makes the messages whose receipts match visible now.</summary>
    public static string ReleaseMessages(int count) => $"UPDATE {Messages} SET visible_on = @now WHERE queue_hash = @queue_hash AND ({ReceiptList(count)})";

    public static void AddQueue(DbCommand command, StreamQueueKey queue) => command.AddParameter("queue_hash", queue.Hash, DbType.Binary);

    /// <summary>Adds the receipts as <c>@s{i}</c> (message ID) and <c>@r{i}</c> (receipt).</summary>
    public static void AddReceipts(DbCommand command, ReadOnlySpan<MessageReceipt> receipts)
    {
        for (var i = 0; i < receipts.Length; i++)
        {
            command.AddParameter($"s{i}", receipts[i].MessageId, DbType.Int64);
            command.AddParameter($"r{i}", receipts[i].Receipt, DbType.Int32);
        }
    }

    /// <summary>Adds the message IDs as <c>@s{i}</c>.</summary>
    public static void AddIds(DbCommand command, IReadOnlyList<long> ids)
    {
        for (var i = 0; i < ids.Count; i++)
        {
            command.AddParameter($"s{i}", ids[i], DbType.Int64);
        }
    }

    public static DequeuedMessage ReadMessage(DbDataReader reader) =>
        new(
            reader.GetInt64(0),
            new Guid(reader.GetFieldValue<byte[]>(1)),
            reader.GetInt32(2),
            new DateTimeOffset(reader.GetInt64(3), TimeSpan.Zero),
            reader.GetFieldValue<byte[]>(4));

    /// <summary>A sequence of parameters, <c>@s0, @s1, …</c>, so the text depends only on their number.</summary>
    public static string IdList(int count)
    {
        var list = new StringBuilder();
        for (var i = 0; i < count; i++)
        {
            list.Append(CultureInfo.InvariantCulture, $"{(i == 0 ? "" : ", ")}@s{i}");
        }

        return list.ToString();
    }

    private static string ReceiptList(int count)
    {
        var list = new StringBuilder();
        for (var i = 0; i < count; i++)
        {
            list.Append(CultureInfo.InvariantCulture, $"{(i == 0 ? "" : " OR ")}(message_id = @s{i} AND dequeued = @r{i})");
        }

        return list.ToString();
    }
}
