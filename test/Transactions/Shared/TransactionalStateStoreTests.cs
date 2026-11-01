using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions.Tests;

/// <summary>
/// The store contract (<see cref="ITransactionalStateStore"/>), below the Orleans types, so it also runs under NativeAOT:
/// each store is all or nothing, and abort, prepare and commit follow Orleans' grain storage wrapper from the committed
/// sequence number before the store.
/// </summary>
public abstract class TransactionalStateStoreTests(DatabaseFixture fixture) : IAsyncLifetime
{
    private ITransactionalStateStore store = null!;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        fixture.Require();
        store = FlavourTransactions.CreateStore(fixture.Database, TestSchemaMode.Apply);
        await store.InitializeAsync(CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task FirstStore_Inserts_AndASecondInsertConflicts()
    {
        var key = NewKey();

        var etag = await StoreAsync(key, null, [Entry(1)]);

        await AssertConflictAsync(key, null, []);
        var loaded = await LoadAsync(key);
        Assert.Equal(etag, loaded.ETag);
        Assert.Equal(0, loaded.CommittedSequenceId);
        Assert.Equal([1L], Sequences(loaded));
    }

    [Fact]
    public async Task Store_WithAStaleOrUnknownETag_ConflictsAndChangesNothing()
    {
        var key = NewKey();
        var first = await StoreAsync(key, null, [Entry(1)]);
        await StoreAsync(key, first, [Entry(2)]);

        await AssertConflictAsync(key, first, [Entry(3)], commitUpTo: 1);
        await AssertConflictAsync(NewKey(), first, []);

        Assert.Equal(new long[] { 1, 2 }, Sequences(await LoadAsync(key)));
    }

    [Fact]
    public async Task Commit_KeepsTheCommittedEntry_AndDeletesOlderOnes()
    {
        var key = NewKey();
        var etag = await StoreAsync(key, null, [Entry(1), Entry(2), Entry(3)]);

        await StoreAsync(key, etag, [], commitUpTo: 2);

        var loaded = await LoadAsync(key);
        Assert.Equal(2, loaded.CommittedSequenceId);
        Assert.Equal([2L, 3L], Sequences(loaded));
    }

    [Fact]
    public async Task PrepareAndCommit_InOneStore_OnAFreshState()
    {
        var key = NewKey();

        await StoreAsync(key, null, [Entry(1)], commitUpTo: 1);

        var loaded = await LoadAsync(key);
        Assert.Equal(1, loaded.CommittedSequenceId);
        Assert.Equal([1L], Sequences(loaded));
    }

    [Fact]
    public async Task Abort_RemovesOnlyPendingEntries_NotTheCommittedOne()
    {
        var key = NewKey();
        var etag = await StoreAsync(key, null, [Entry(1), Entry(2), Entry(3)], commitUpTo: 1);

        etag = await StoreAsync(key, etag, [], abortAfter: 0);

        var loaded = await LoadAsync(key);
        Assert.Equal(1, loaded.CommittedSequenceId);
        Assert.Equal([1L], Sequences(loaded));
        Assert.Equal(Payload(1), loaded.Entries[0].Payload);

        await StoreAsync(key, etag, [Entry(2)], abortAfter: 5);
        Assert.Equal(new long[] { 1, 2 }, Sequences(await LoadAsync(key)));
    }

    [Fact]
    public async Task Prepare_AtOrBelowTheCommittedVersion_LeavesTheCommittedEntry()
    {
        var key = NewKey();
        var etag = await StoreAsync(key, null, [Entry(1), Entry(2)], commitUpTo: 2);

        await StoreAsync(key, etag, [Entry(1, payload: 99), Entry(2, payload: 99), Entry(3)]);

        var loaded = await LoadAsync(key);
        Assert.Equal(2, loaded.CommittedSequenceId);
        Assert.Equal([2L, 3L], Sequences(loaded));
        Assert.Equal(Payload(2), loaded.Entries[0].Payload);
    }

    [Fact]
    public async Task Prepare_ReplacesAPendingEntry()
    {
        var key = NewKey();
        var etag = await StoreAsync(key, null, [Entry(1), Entry(2)]);

        await StoreAsync(key, etag, [Entry(2, payload: 99)]);

        var loaded = await LoadAsync(key);
        Assert.Equal(Payload(99), loaded.Entries[1].Payload);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    public async Task Commit_ThatDoesNotAdvance_ChangesNothingButTheRow(long commitUpTo)
    {
        var key = NewKey();
        var etag = await StoreAsync(key, null, commitUpTo == 0 ? [] : [Entry(1), Entry(2)], commitUpTo: commitUpTo == 0 ? 0 : 2);
        var before = await LoadAsync(key);

        var stored = await StoreAsync(key, etag, [], commitUpTo: commitUpTo);

        var loaded = await LoadAsync(key);
        Assert.Equal(stored, loaded.ETag);
        Assert.Equal(before.CommittedSequenceId, loaded.CommittedSequenceId);
        Assert.Equal(Sequences(before), Sequences(loaded));
    }

    [Fact]
    public async Task Commit_WithoutItsEntry_ReportsIt_AndChangesNothing()
    {
        var key = NewKey();
        var etag = await StoreAsync(key, null, [Entry(1)]);

        var result = await store.StoreAsync(key, Request(etag, [Entry(2)], commitUpTo: 3), CancellationToken);

        Assert.Equal(StoreStatus.MissingCommitRecord, result.Status);
        var loaded = await LoadAsync(key);
        Assert.Equal(etag, loaded.ETag);
        Assert.Equal([1L], Sequences(loaded));
    }

    [Fact]
    public async Task Conflict_ReportsTheIdentityHeldUnderTheKey()
    {
        var key = NewKey();
        await StoreAsync(key, null, []);

        var result = await store.StoreAsync(key, Request(null, []), CancellationToken);

        Assert.Equal(StoreStatus.Conflict, result.Status);
        Assert.Equal(key.Identity, result.StoredIdentity);
    }

    [Fact]
    public async Task ManyEntries_AreWrittenAndCommittedInOneStore()
    {
        var key = NewKey();
        var entries = Enumerable.Range(1, 450).Select(i => Entry(i)).ToArray();

        var etag = await StoreAsync(key, null, entries);
        await StoreAsync(key, etag, entries, commitUpTo: 300);

        var loaded = await LoadAsync(key);
        Assert.Equal(300, loaded.CommittedSequenceId);
        Assert.Equal(151, loaded.Entries.Count);
    }

    /// <summary>
    /// More prepares than one statement can take as parameters: PostgreSQL allows 65,535, five per entry.
    /// </summary>
    [Fact]
    public async Task PreparesBeyondOneStatementsParameterLimit_AreStoredInOneStore()
    {
        var key = NewKey();
        var entries = Enumerable.Range(1, 14_000).Select(i => Entry(i)).ToArray();

        var etag = await StoreAsync(key, null, entries);
        await StoreAsync(key, etag, [], commitUpTo: 13_999);

        var loaded = await LoadAsync(key);
        Assert.Equal(13_999, loaded.CommittedSequenceId);
        Assert.Equal(new long[] { 13_999, 14_000 }, Sequences(loaded));
    }

    private async Task<byte[]> StoreAsync(TransactionalStateKey key, byte[]? expectedETag, StoredEntry[] prepared, long? commitUpTo = null, long? abortAfter = null)
    {
        var request = Request(expectedETag, prepared, commitUpTo, abortAfter);
        Assert.Equal(StoreStatus.Stored, (await store.StoreAsync(key, request, CancellationToken)).Status);
        return request.ETag;
    }

    private async Task AssertConflictAsync(TransactionalStateKey key, byte[]? expectedETag, StoredEntry[] prepared, long? commitUpTo = null) =>
        Assert.Equal(StoreStatus.Conflict, (await store.StoreAsync(key, Request(expectedETag, prepared, commitUpTo), CancellationToken)).Status);

    private async Task<StoredTransactionalState> LoadAsync(TransactionalStateKey key) =>
        await store.LoadAsync(key, CancellationToken) ?? throw new InvalidOperationException("No state row.");

    private static StoreRequest Request(byte[]? expectedETag, StoredEntry[] prepared, long? commitUpTo = null, long? abortAfter = null) =>
        new(expectedETag, ETags.New(TimeProvider.System), [7], prepared, commitUpTo, abortAfter, DateTimeOffset.UtcNow);

    private static StoredEntry Entry(long sequenceId, int? payload = null) =>
        new(sequenceId, $"transaction-{sequenceId}", sequenceId * 10, [1], Payload(payload ?? (int)sequenceId));

    private static byte[] Payload(int value) => BitConverter.GetBytes(value);

    private static long[] Sequences(StoredTransactionalState state) => [.. state.Entries.Select(e => e.SequenceId)];

    private static TransactionalStateKey NewKey() => TransactionalStateKey.Create("service", "state", GrainId.Create("store", Guid.NewGuid().ToString("N")));
}
