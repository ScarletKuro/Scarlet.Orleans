using System.Data;
using System.Xml.Linq;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Scarlet.Orleans.Streaming.Tests;

public sealed class SqlServerReceiptPlanTests(SqlServerDatabaseFixture fixture) : IClassFixture<SqlServerDatabaseFixture>
{
    [Theory]
    [InlineData(32, false)]
    [InlineData(64, false)]
    [InlineData(500, false)]
    [InlineData(32, true)]
    [InlineData(64, true)]
    [InlineData(500, true)]
    public async Task ReceiptWrites_ReadThroughTheMessageIdIndex(int count, bool release)
    {
        fixture.Require();
        var database = await fixture.CreateDatabaseAsync();
        var ct = TestContext.Current.CancellationToken;
        var store = FlavourStreaming.CreateStore(database, TestSchemaMode.Apply);
        await store.InitializeAsync(ct);
        var queue = StreamQueueKey.Create(Guid.NewGuid().ToString("N"), "plan", "queue");
        var now = DateTimeOffset.UtcNow;
        await store.RegisterQueueAsync(queue, now, now.AddMinutes(1), ct);
        await using (var seed = database.DataSource.CreateCommand("""
            INSERT INTO orleans_stream_message (queue_hash, message_key, dequeued, visible_on, expires_on, created_on, payload)
            SELECT TOP (@seed_count) @queue_hash, CONVERT(binary(16), NEWID()), 0, @now, @expires, SYSUTCDATETIME(), CONVERT(varbinary(max), REPLICATE('x', 256))
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
            """))
        {
            StreamingSql.AddQueue(seed, queue);
            seed.AddParameter("seed_count", count, DbType.Int32);
            seed.AddParameter("now", now.UtcTicks, DbType.Int64);
            seed.AddParameter("expires", now.AddHours(1).UtcTicks, DbType.Int64);
            await seed.ExecuteNonQueryAsync(ct);
        }

        var messages = await store.DequeueAsync(queue, count, 3, now, now.AddMinutes(1), ct);
        Assert.Equal(count, messages.Count);
        var receipts = messages.Select(m => new MessageReceipt(m.MessageId, m.Receipt)).ToArray();
        await using var connection = await database.DataSource.OpenConnectionAsync(ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "SET STATISTICS XML ON; " + (release ? StreamingSql.ReleaseMessages(RelationalEngine.SqlServer, count) : StreamingSql.ConfirmMessages(RelationalEngine.SqlServer, count)) + "; SET STATISTICS XML OFF";
        StreamingSql.AddQueue(command, queue);
        StreamingSql.AddReceipts(command, receipts);
        if (release) command.AddParameter("now", now.UtcTicks, DbType.Int64);
        await using var reader = await command.ExecuteReaderAsync(ct);
        string? xml = null;
        do
        {
            if (reader.FieldCount == 1 && await reader.ReadAsync(ct)) xml = reader.GetString(0);
        } while (await reader.NextResultAsync(ct));
        var plan = XDocument.Parse(Assert.IsType<string>(xml));

        XNamespace ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";
        var reads = plan.Descendants(ns + "RelOp")
            .Where(op => ((string?)op.Attribute("LogicalOp")) is "Index Scan" or "Clustered Index Scan" or "Index Seek" or "Clustered Index Seek")
            .SelectMany(op => op.Descendants(ns + "Object"))
            .Select(o => (string?)o.Attribute("Index"))
            .ToArray();
        // An unrelated queue's delete can own a key in this global index. Scanning it while confirming another
        // queue produced U/X deadlocks and several-second pauses in the SQL Server streaming benchmark.
        Assert.NotEmpty(reads);
        Assert.All(reads, index => Assert.Equal("[pk_orleans_stream_message]", index));
    }
}
