using System.Data;
using System.Transactions;
using Orleans;
using Orleans.Runtime;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// Guarantees of this provider beyond the Orleans TestKit: identity verification, unbounded identities, start time
/// normalisation and transaction isolation.
/// </summary>
public abstract class ReminderTableConformanceTests(DatabaseFixture fixture) : IAsyncLifetime
{
    private static readonly DateTime StartAt = new(2026, 3, 1, 12, 30, 15, DateTimeKind.Utc);

    private readonly string serviceId = $"service-{Guid.NewGuid():N}";
    private RelationalReminderTable? table;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    private RelationalReminderTable Table => table ??= FlavourTable.Create(fixture.Database, serviceId, TestSchemaMode.Apply);

    public async ValueTask InitializeAsync()
    {
        if (fixture.IsAvailable)
        {
            await Table.StartAsync(CancellationToken);
        }
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task LongIdentities_RoundTripThroughEveryRead()
    {
        var grainId = GrainId.Create(new string('t', 300), new string('k', 10_000));
        var name = new string('n', 5_000);
        var etag = await Table.UpsertRow(Entry(grainId, name), CancellationToken);

        var read = await Table.ReadRow(grainId, name, CancellationToken);
        var grain = await Table.ReadRows(grainId, CancellationToken);
        var range = await Table.ReadRows(0, 0, CancellationToken);

        Assert.Equal(etag, read?.ETag);
        Assert.Equal(grainId, Assert.Single(grain.Reminders).GrainId);
        Assert.Equal(name, Assert.Single(range.Reminders).ReminderName);
    }

    [Fact]
    public async Task Identities_WithNulAndNonUtf8GrainKeys_RoundTrip()
    {
        var grainId = GrainId.Create(new GrainType([0xFF, 0x00, 0xC3]), new IdSpan([0x00, 0xFE]));
        var name = "name\0with\0nul";
        await Table.UpsertRow(Entry(grainId, name), CancellationToken);

        var entry = Assert.Single((await Table.ReadRows(grainId, CancellationToken)).Reminders);

        Assert.Equal(grainId, entry.GrainId);
        Assert.Equal(name, entry.ReminderName);
    }

    [Fact]
    public async Task Services_DoNotSeeEachOthersReminders()
    {
        var other = FlavourTable.Create(fixture.Database, $"other-{Guid.NewGuid():N}", TestSchemaMode.None);
        var grainId = GrainId.Create("isolation", "key");
        await Table.UpsertRow(Entry(grainId, "mine"), CancellationToken);
        await other.UpsertRow(Entry(grainId, "theirs"), CancellationToken);

        await other.TestOnlyClearTable(CancellationToken);

        Assert.Equal("mine", Assert.Single((await Table.ReadRows(0, 0, CancellationToken)).Reminders).ReminderName);
        Assert.Equal("mine", Assert.Single((await Table.ReadRows(grainId, CancellationToken)).Reminders).ReminderName);
        Assert.Null(await other.ReadRow(grainId, "mine", CancellationToken));
    }

    [Theory]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Unspecified)]
    [InlineData(DateTimeKind.Local)]
    public async Task StartAt_IsStoredAsUtc(DateTimeKind kind)
    {
        var grainId = GrainId.Create("start-at", kind.ToString());
        var startAt = kind switch
        {
            DateTimeKind.Local => StartAt.ToLocalTime(),
            DateTimeKind.Unspecified => DateTime.SpecifyKind(StartAt, DateTimeKind.Unspecified),
            _ => StartAt,
        };
        await Table.UpsertRow(Entry(grainId, "r", startAt), CancellationToken);

        var read = await Table.ReadRow(grainId, "r", CancellationToken);

        Assert.Equal(DateTimeKind.Utc, read!.StartAt.Kind);
        Assert.Equal(StartAt, read.StartAt);
    }

    [Fact]
    public async Task Schedule_KeepsSubSecondTicks()
    {
        var grainId = GrainId.Create("ticks", "key");
        var entry = Entry(grainId, "r", StartAt.AddTicks(1_234_567));
        entry.Period = TimeSpan.FromTicks(98_765_432_101);
        await Table.UpsertRow(entry, CancellationToken);

        var read = await Table.ReadRow(grainId, "r", CancellationToken);

        Assert.Equal(entry.StartAt, read!.StartAt);
        Assert.Equal(entry.Period, read.Period);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-an-etag")]
    public async Task RemoveRow_WithAnETagNeverIssued_ReturnsFalseAndKeepsTheRow(string etag)
    {
        var grainId = GrainId.Create("foreign-etag", "key");
        await Table.UpsertRow(Entry(grainId, "r"), CancellationToken);

        Assert.False(await Table.RemoveRow(grainId, "r", etag, CancellationToken));
        Assert.NotNull(await Table.ReadRow(grainId, "r", CancellationToken));
    }

    [Fact]
    public async Task Writes_InsideAnAmbientTransactionThatRollsBack_StayCommitted()
    {
        var grainId = GrainId.Create("ambient", "key");
        string? etag;
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            etag = await Table.UpsertRow(Entry(grainId, "r"), CancellationToken);
        }

        Assert.Equal(etag, (await Table.ReadRow(grainId, "r", CancellationToken))?.ETag);
    }

    /// <summary>
    /// A row under one reminder's key holding another reminder's identity is what a 128-bit hash collision would leave.
    /// Every operation on the colliding reminder fails, and the row it collides with is left as it was.
    /// </summary>
    [Fact]
    public async Task HashCollision_FailsEveryOperationWithoutTouchingTheOtherReminder()
    {
        var victim = GrainId.Create("collision", "victim");
        var intruder = GrainId.Create("collision", "intruder");
        var etag = await Table.UpsertRow(Entry(victim, "r"), CancellationToken);
        await ExecuteAsync(
            $"UPDATE {RemindersSchema.TableName} SET key_hash = @intruder WHERE key_hash = @victim",
            ("intruder", ReminderKey.Create(serviceId, intruder, "r").Hash),
            ("victim", ReminderKey.Create(serviceId, victim, "r").Hash));

        await Assert.ThrowsAsync<ReminderKeyCollisionException>(() => Table.ReadRow(intruder, "r", CancellationToken));
        await Assert.ThrowsAsync<ReminderKeyCollisionException>(() => Table.UpsertRow(Entry(intruder, "r", StartAt.AddDays(1)), CancellationToken));
        Assert.False(await Table.RemoveRow(intruder, "r", etag!, CancellationToken));

        var stored = Assert.Single((await Table.ReadRows(0, 0, CancellationToken)).Reminders);
        Assert.Equal(victim, stored.GrainId);
        Assert.Equal(StartAt, stored.StartAt);
        Assert.Equal(etag, stored.ETag);
    }

    [Fact]
    public async Task GrainKeyCollision_FailsTheGrainRead()
    {
        var victim = GrainId.Create("collision", "victim");
        var intruder = GrainId.Create("collision", "intruder");
        await Table.UpsertRow(Entry(victim, "r"), CancellationToken);
        await ExecuteAsync(
            $"UPDATE {RemindersSchema.TableName} SET grain_key_hash = @intruder",
            ("intruder", ReminderKey.Create(serviceId, intruder, "r").GrainKeyHash));

        await Assert.ThrowsAsync<ReminderKeyCollisionException>(() => Table.ReadRows(intruder, CancellationToken));
    }

    private static ReminderEntry Entry(GrainId grainId, string name, DateTime? startAt = null) => new()
    {
        GrainId = grainId,
        ReminderName = name,
        StartAt = startAt ?? StartAt,
        Period = TimeSpan.FromMinutes(5),
    };

    private async Task ExecuteAsync(string sql, params (string Name, byte[] Value)[] parameters)
    {
        await using var command = fixture.Database.DataSource.CreateCommand(sql);
        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.DbType = DbType.Binary;
            command.Parameters.Add(parameter);
        }

        await command.ExecuteNonQueryAsync(CancellationToken);
    }
}
