using Orleans;
using Orleans.Runtime;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// Cancellation of reminder writes whose statement may already have committed, as when a token fires while the server
/// is answering.
/// </summary>
public abstract class ReminderCancellationTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Upsert_CanceledAfterItCommitted_ReportsTheETagItWrote(bool existing)
    {
        var store = new CancelingStore(FlavourTable.CreateStore(fixture.Database, TestSchemaMode.Apply));
        var table = FlavourTable.Create(store, $"service-{Guid.NewGuid():N}");
        await table.StartAsync(CancellationToken);
        var grainId = GrainId.Create("cancel", "key");
        if (existing)
        {
            await table.UpsertRow(Entry(grainId, minutes: 1), CancellationToken);
        }

        store.CommitThenCancel = true;
        var etag = await table.UpsertRow(Entry(grainId, minutes: 2), CancellationToken);
        store.CommitThenCancel = false;

        var read = await table.ReadRow(grainId, "r", CancellationToken);
        Assert.Equal(etag, read?.ETag);
        Assert.Equal(TimeSpan.FromMinutes(2), read!.Period);
    }

    [Fact]
    public async Task Upsert_CanceledBeforeItCommitted_IsCanceledAndChangesNothing()
    {
        var store = new CancelingStore(FlavourTable.CreateStore(fixture.Database, TestSchemaMode.Apply));
        var table = FlavourTable.Create(store, $"service-{Guid.NewGuid():N}");
        await table.StartAsync(CancellationToken);
        var grainId = GrainId.Create("cancel", "key");
        var etag = await table.UpsertRow(Entry(grainId, minutes: 1), CancellationToken);

        store.CancelBeforeCommit = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.UpsertRow(Entry(grainId, minutes: 2), CancellationToken));
        store.CancelBeforeCommit = false;

        Assert.Equal(etag, (await table.ReadRow(grainId, "r", CancellationToken))?.ETag);
    }

    [Fact]
    public async Task Operations_CanceledBeforeTheyStart_DoNothing()
    {
        var table = FlavourTable.Create(fixture.Database, $"service-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await table.StartAsync(CancellationToken);
        var grainId = GrainId.Create("cancel", "key");
        var etag = await table.UpsertRow(Entry(grainId, minutes: 1), CancellationToken);
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.UpsertRow(Entry(grainId, minutes: 2), canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.RemoveRow(grainId, "r", etag!, canceled.Token));

        Assert.Equal(etag, (await table.ReadRow(grainId, "r", CancellationToken))?.ETag);
    }

    private static ReminderEntry Entry(GrainId grainId, int minutes) => new()
    {
        GrainId = grainId,
        ReminderName = "r",
        StartAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        Period = TimeSpan.FromMinutes(minutes),
    };

    /// <summary>
    /// Fails writes as a driver does when the token fires while the statement runs: after the server committed it, or
    /// before the server ran it.
    /// </summary>
    private sealed class CancelingStore(IReminderStore inner) : IReminderStore
    {
        public bool CommitThenCancel { get; set; }

        public bool CancelBeforeCommit { get; set; }

        public Task InitializeAsync(CancellationToken cancellationToken) => inner.InitializeAsync(cancellationToken);

        public Task<StoredReminder?> ReadAsync(ReminderKey key, CancellationToken cancellationToken) => inner.ReadAsync(key, cancellationToken);

        public Task<IReadOnlyList<StoredReminder>> ReadGrainAsync(byte[] grainKeyHash, CancellationToken cancellationToken) =>
            inner.ReadGrainAsync(grainKeyHash, cancellationToken);

        public Task<IReadOnlyList<StoredReminder>> ReadRangeAsync(byte[] serviceHash, uint begin, uint end, CancellationToken cancellationToken) =>
            inner.ReadRangeAsync(serviceHash, begin, end, cancellationToken);

        public Task<bool> TryInsertAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
            WriteAsync(() => inner.TryInsertAsync(key, startAt, period, etag, CancellationToken.None));

        public Task<bool> TryUpdateAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
            WriteAsync(() => inner.TryUpdateAsync(key, startAt, period, etag, CancellationToken.None));

        public Task<bool> TryDeleteAsync(ReminderKey key, byte[] expectedETag, CancellationToken cancellationToken) =>
            inner.TryDeleteAsync(key, expectedETag, cancellationToken);

        public Task ClearAsync(byte[] serviceHash, CancellationToken cancellationToken) => inner.ClearAsync(serviceHash, cancellationToken);

        private async Task<bool> WriteAsync(Func<Task<bool>> write)
        {
            if (CancelBeforeCommit)
            {
                throw new OperationCanceledException();
            }

            var written = await write();
            if (CommitThenCancel && written)
            {
                throw new OperationCanceledException();
            }

            return written;
        }
    }
}
