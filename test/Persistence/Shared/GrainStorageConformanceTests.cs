using System.Data;
using System.Transactions;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// The <see cref="IGrainStorage"/> contract, run against every flavour and engine.
/// </summary>
/// <remarks>
/// Tests share one database per class and isolate their rows with a unique service ID.
/// </remarks>
public abstract class GrainStorageConformanceTests(DatabaseFixture fixture)
{
    private const string StateName = "state";
    private readonly string serviceId = $"service-{Guid.NewGuid():N}";

    protected DatabaseFixture Fixture { get; } = fixture;

    private protected async Task<RelationalGrainStorage> CreateInitializedStorageAsync(bool deleteStateOnClear = false, string? serviceId = null)
    {
        var storage = FlavourStorage.Create(Fixture.Database, serviceId ?? this.serviceId, deleteStateOnClear, TestSchemaMode.Apply);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        return storage;
    }

    private static GrainId NewGrainId(string type = "test-grain") => GrainId.Create(type, Guid.NewGuid().ToString("N"));

    private static GrainState<TestState> NewState(int value, string? etag = null) =>
        new() { State = new TestState { Value = value, Text = $"value-{value}" }, ETag = etag };

    private static async Task<GrainState<TestState>> ReadAsync(RelationalGrainStorage storage, GrainId grainId, string stateName = StateName)
    {
        var state = new GrainState<TestState>();
        await storage.ReadStateAsync(stateName, grainId, state);
        return state;
    }

    [Fact]
    public async Task Read_WhenNothingWritten_ReturnsNoRecord()
    {
        var storage = await CreateInitializedStorageAsync();

        var state = await ReadAsync(storage, NewGrainId());

        Assert.False(state.RecordExists);
        Assert.Null(state.ETag);
        Assert.Equal(0, state.State!.Value);
    }

    [Fact]
    public async Task Write_ThenRead_RoundTripsStateAndETag()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var written = NewState(1);

        await storage.WriteStateAsync(StateName, grainId, written);
        var read = await ReadAsync(storage, grainId);

        Assert.True(written.RecordExists);
        Assert.NotNull(written.ETag);
        Assert.True(read.RecordExists);
        Assert.Equal(written.ETag, read.ETag);
        Assert.Equal(1, read.State!.Value);
        Assert.Equal("value-1", read.State!.Text);
    }

    [Fact]
    public async Task Write_WithCurrentETag_ReplacesStateAndETag()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var state = NewState(1);
        await storage.WriteStateAsync(StateName, grainId, state);
        var firstETag = state.ETag;

        state.State!.Value = 2;
        await storage.WriteStateAsync(StateName, grainId, state);
        var read = await ReadAsync(storage, grainId);

        Assert.NotEqual(firstETag, state.ETag);
        Assert.Equal(state.ETag, read.ETag);
        Assert.Equal(2, read.State!.Value);
    }

    [Fact]
    public async Task Write_WithStaleETag_ThrowsAndKeepsStoredState()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var state = NewState(1);
        await storage.WriteStateAsync(StateName, grainId, state);
        var stale = NewState(9, state.ETag);
        await storage.WriteStateAsync(StateName, grainId, state);

        var exception = await Assert.ThrowsAsync<InconsistentStateException>(() => storage.WriteStateAsync(StateName, grainId, stale));
        var read = await ReadAsync(storage, grainId);

        Assert.Equal(state.ETag, exception.StoredEtag);
        Assert.Equal(1, read.State!.Value);
        Assert.Equal(state.ETag, read.ETag);
    }

    [Theory]
    [InlineData("12345")]
    [InlineData("not-an-etag")]
    [InlineData("")]
    public async Task Write_WithETagNotIssuedByProvider_Throws(string etag)
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        await storage.WriteStateAsync(StateName, grainId, NewState(1));

        await Assert.ThrowsAsync<InconsistentStateException>(() => storage.WriteStateAsync(StateName, grainId, NewState(2, etag)));

        Assert.Equal(1, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Fact]
    public async Task Write_WithoutETag_WhenStateExists_Throws()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        await storage.WriteStateAsync(StateName, grainId, NewState(1));

        await Assert.ThrowsAsync<InconsistentStateException>(() => storage.WriteStateAsync(StateName, grainId, NewState(2)));

        Assert.Equal(1, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Fact]
    public async Task Clear_KeepsRowWithNewETag()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var state = NewState(1);
        await storage.WriteStateAsync(StateName, grainId, state);
        var writtenETag = state.ETag;

        await storage.ClearStateAsync(StateName, grainId, state);
        var read = await ReadAsync(storage, grainId);

        Assert.False(state.RecordExists);
        Assert.NotNull(state.ETag);
        Assert.NotEqual(writtenETag, state.ETag);
        Assert.Equal(0, state.State!.Value);
        Assert.False(read.RecordExists);
        Assert.Equal(state.ETag, read.ETag);

        state.State!.Value = 3;
        await storage.WriteStateAsync(StateName, grainId, state);
        Assert.Equal(3, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Fact]
    public async Task Clear_WithDeleteStateOnClear_RemovesRow()
    {
        var storage = await CreateInitializedStorageAsync(deleteStateOnClear: true);
        var grainId = NewGrainId();
        var state = NewState(1);
        await storage.WriteStateAsync(StateName, grainId, state);

        await storage.ClearStateAsync(StateName, grainId, state);
        var read = await ReadAsync(storage, grainId);

        Assert.False(state.RecordExists);
        Assert.Null(state.ETag);
        Assert.False(read.RecordExists);
        Assert.Null(read.ETag);

        state.State!.Value = 3;
        await storage.WriteStateAsync(StateName, grainId, state);
        Assert.Equal(3, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Clear_WithStaleETag_ThrowsAndKeepsStoredState(bool deleteStateOnClear)
    {
        var storage = await CreateInitializedStorageAsync(deleteStateOnClear);
        var grainId = NewGrainId();
        var state = NewState(1);
        await storage.WriteStateAsync(StateName, grainId, state);
        var stale = NewState(1, state.ETag);
        await storage.WriteStateAsync(StateName, grainId, state);

        await Assert.ThrowsAsync<InconsistentStateException>(() => storage.ClearStateAsync(StateName, grainId, stale));

        var read = await ReadAsync(storage, grainId);
        Assert.True(read.RecordExists);
        Assert.Equal(state.ETag, read.ETag);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Clear_WhenNothingWritten_ResetsState(bool deleteStateOnClear)
    {
        var storage = await CreateInitializedStorageAsync(deleteStateOnClear);
        var grainId = NewGrainId();
        var state = NewState(5);

        await storage.ClearStateAsync(StateName, grainId, state);

        Assert.False(state.RecordExists);
        Assert.Null(state.ETag);
        Assert.Equal(0, state.State!.Value);
        Assert.False((await ReadAsync(storage, grainId)).RecordExists);
    }

    [Fact]
    public async Task Clear_WithoutETag_WhenStateExists_Throws()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        await storage.WriteStateAsync(StateName, grainId, NewState(1));

        await Assert.ThrowsAsync<InconsistentStateException>(() => storage.ClearStateAsync(StateName, grainId, NewState(1)));

        Assert.True((await ReadAsync(storage, grainId)).RecordExists);
    }

    [Fact]
    public async Task States_AreIsolatedByServiceStateNameGrainTypeAndKey()
    {
        var storage = await CreateInitializedStorageAsync();
        var otherService = await CreateInitializedStorageAsync(serviceId: $"other-{Guid.NewGuid():N}");
        var key = Guid.NewGuid().ToString("N");
        var grainId = GrainId.Create("grain-a", key);
        await storage.WriteStateAsync(StateName, grainId, NewState(1));

        Assert.False((await ReadAsync(otherService, grainId)).RecordExists);
        Assert.False((await ReadAsync(storage, grainId, "other-state")).RecordExists);
        Assert.False((await ReadAsync(storage, GrainId.Create("grain-b", key))).RecordExists);
        Assert.False((await ReadAsync(storage, GrainId.Create("grain-a", key + "x"))).RecordExists);
        Assert.Equal(1, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Fact]
    public async Task Identities_ThatAreNotValidUtf8_StayDistinct()
    {
        var storage = await CreateInitializedStorageAsync();
        var type = new GrainType([0xFF, 0x01]);

        var first = GrainId.Create(type, new IdSpan([0xFF]));
        var second = GrainId.Create(type, new IdSpan([0xFE]));
        await storage.WriteStateAsync(StateName, first, NewState(1));
        await storage.WriteStateAsync(StateName, second, NewState(2));

        Assert.Equal(1, (await ReadAsync(storage, first)).State!.Value);
        Assert.Equal(2, (await ReadAsync(storage, second)).State!.Value);
    }

    [Fact]
    public async Task LongIdentity_RoundTrips()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = GrainId.Create(new string('t', 2_000), new string('k', 10_000));
        var stateName = new string('s', 2_000);

        await storage.WriteStateAsync(stateName, grainId, NewState(7));

        Assert.Equal(7, (await ReadAsync(storage, grainId, stateName)).State!.Value);
    }

    [Fact]
    public async Task LargePayload_RoundTrips()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var text = new string('x', 1024 * 1024);

        await storage.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState { Text = text } });

        Assert.Equal(text, (await ReadAsync(storage, grainId)).State!.Text);
    }

    [Fact]
    public async Task Write_LargerThanMaxPayloadSize_FailsAndWritesNothing()
    {
        await CreateInitializedStorageAsync();
        var storage = FlavourStorage.Create(FlavourStorage.CreateStore(Fixture.Database, TestSchemaMode.Validate), serviceId, deleteStateOnClear: false, maxPayloadSize: 64);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        var grainId = NewGrainId();

        var exception = await Assert.ThrowsAsync<GrainStatePayloadTooLargeException>(
            () => storage.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState { Text = new string('x', 100) } }));

        Assert.Contains(grainId.ToString(), exception.Message, StringComparison.Ordinal);
        Assert.False((await ReadAsync(storage, grainId)).RecordExists);
        await storage.WriteStateAsync(StateName, grainId, NewState(1));
        Assert.Equal(1, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Fact]
    public async Task ConcurrentWrites_WithTheSameETag_ExactlyOneSucceeds()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var state = NewState(0);
        await storage.WriteStateAsync(StateName, grainId, state);

        var writers = Enumerable.Range(1, 8).Select(i => NewState(i, state.ETag)).ToList();
        var results = await Task.WhenAll(writers.Select(w => Record.ExceptionAsync(() => storage.WriteStateAsync(StateName, grainId, w)).AsTask()));

        var winner = Assert.Single(writers.Where((_, i) => results[i] is null));
        Assert.All(results.Where(r => r is not null), r => Assert.IsType<InconsistentStateException>(r));
        var read = await ReadAsync(storage, grainId);
        Assert.Equal(winner.ETag, read.ETag);
        Assert.Equal(winner.State!.Value, read.State!.Value);
    }

    [Fact]
    public async Task ConcurrentInserts_ExactlyOneSucceeds()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();

        var writers = Enumerable.Range(1, 8).Select(i => NewState(i)).ToList();
        var results = await Task.WhenAll(writers.Select(w => Record.ExceptionAsync(() => storage.WriteStateAsync(StateName, grainId, w)).AsTask()));

        var winner = Assert.Single(writers.Where((_, i) => results[i] is null));
        Assert.All(results.Where(r => r is not null), r => Assert.IsType<InconsistentStateException>(r));
        Assert.Equal(winner.State!.Value, (await ReadAsync(storage, grainId)).State!.Value);
    }

    [Fact]
    public async Task HashCollision_FailsWithoutTouchingEitherState()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var key = GrainStateKey.Create(serviceId, StateName, grainId);
        await InsertForeignRowAsync(key.Hash);

        await Assert.ThrowsAsync<GrainStateKeyCollisionException>(() => ReadAsync(storage, grainId));
        await Assert.ThrowsAsync<GrainStateKeyCollisionException>(() => storage.WriteStateAsync(StateName, grainId, NewState(1)));
        await Assert.ThrowsAsync<GrainStateKeyCollisionException>(() => storage.ClearStateAsync(StateName, grainId, NewState(1)));

        Assert.Equal(ForeignPayload, await ReadPayloadAsync(key.Hash));
    }

    [Fact]
    public async Task Write_InsideAnAmbientTransactionThatRollsBack_StaysCommitted()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = NewGrainId();
        var state = NewState(1);

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await storage.WriteStateAsync(StateName, grainId, state);
        }

        var read = await ReadAsync(storage, grainId);
        Assert.True(read.RecordExists);
        Assert.Equal(state.ETag, read.ETag);
    }

    [Fact]
    public async Task Identities_WithNulCharacters_RoundTrip()
    {
        var storage = await CreateInitializedStorageAsync();
        var grainId = GrainId.Create(new GrainType([0x61, 0x00, 0x62]), new IdSpan([0x00, 0x01]));
        const string stateName = "state\0name";

        await storage.WriteStateAsync(stateName, grainId, NewState(4));

        Assert.Equal(4, (await ReadAsync(storage, grainId, stateName)).State!.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Write_WhenARepeatFindsItsOwnCommittedWrite_Succeeds(bool insert)
    {
        await CreateInitializedStorageAsync();
        var store = new LostAcknowledgementStore(FlavourStorage.CreateStore(Fixture.Database, TestSchemaMode.Validate));
        var storage = FlavourStorage.Create(store, serviceId, deleteStateOnClear: false);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        var grainId = NewGrainId();
        var state = NewState(1);
        if (!insert)
        {
            await storage.WriteStateAsync(StateName, grainId, state);
        }

        store.LoseNextAcknowledgement = true;
        state.State!.Value = 2;
        await storage.WriteStateAsync(StateName, grainId, state);

        var read = await ReadAsync(storage, grainId);
        Assert.Equal(state.ETag, read.ETag);
        Assert.Equal(2, read.State!.Value);
    }

    [Fact]
    public async Task Clear_WhenARepeatFindsItsOwnCommittedClear_Succeeds()
    {
        await CreateInitializedStorageAsync();
        var store = new LostAcknowledgementStore(FlavourStorage.CreateStore(Fixture.Database, TestSchemaMode.Validate));
        var storage = FlavourStorage.Create(store, serviceId, deleteStateOnClear: false);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        var grainId = NewGrainId();
        var state = NewState(1);
        await storage.WriteStateAsync(StateName, grainId, state);

        store.LoseNextAcknowledgement = true;
        await storage.ClearStateAsync(StateName, grainId, state);

        var read = await ReadAsync(storage, grainId);
        Assert.False(read.RecordExists);
        Assert.Equal(state.ETag, read.ETag);
    }

    /// <summary>
    /// Commits a write, then reports that it matched no row, as a repeat of a committed statement does.
    /// </summary>
    private sealed class LostAcknowledgementStore(IGrainStateStore inner) : IGrainStateStore
    {
        public bool LoseNextAcknowledgement { get; set; }

        public Task InitializeAsync(CancellationToken cancellationToken) => inner.InitializeAsync(cancellationToken);

        public Task<StoredGrainState?> ReadAsync(GrainStateKey key, CancellationToken cancellationToken) => inner.ReadAsync(key, cancellationToken);

        public async Task<bool> TryInsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
            Acknowledge(await inner.TryInsertAsync(key, payload, etag, modifiedOn, cancellationToken));

        public async Task<bool> TryUpdateAsync(GrainStateKey key, byte[] expectedETag, byte[]? payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken) =>
            Acknowledge(await inner.TryUpdateAsync(key, expectedETag, payload, etag, modifiedOn, cancellationToken));

        public Task<bool> TryDeleteAsync(GrainStateKey key, byte[] expectedETag, CancellationToken cancellationToken) => inner.TryDeleteAsync(key, expectedETag, cancellationToken);

        private bool Acknowledge(bool written)
        {
            Assert.True(written);
            var acknowledged = !LoseNextAcknowledgement;
            LoseNextAcknowledgement = false;
            return acknowledged;
        }
    }

    private static readonly byte[] ForeignPayload = [1, 2, 3];

    /// <summary>
    /// Stores a row of another identity under <paramref name="keyHash"/>, as a hash collision would.
    /// </summary>
    private async Task InsertForeignRowAsync(byte[] keyHash)
    {
        await using var command = Fixture.Database.DataSource.CreateCommand(
            $"INSERT INTO {PersistenceSchema.TableName} (key_hash, grain_identity, service_id, state_name, grain_id, payload, etag, modified_on) " +
            "VALUES (@key_hash, @grain_identity, 'other', 'other', 'other', @payload, @etag, @modified_on)");
        command.AddParameter("key_hash", keyHash, DbType.Binary);
        command.AddParameter("grain_identity", new byte[] { 42 }, DbType.Binary);
        command.AddParameter("payload", ForeignPayload, DbType.Binary);
        command.AddParameter("etag", Guid.NewGuid().ToByteArray(), DbType.Binary);
        command.AddParameter("modified_on", DateTimeOffset.UtcNow, DbType.DateTimeOffset);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<byte[]> ReadPayloadAsync(byte[] keyHash)
    {
        await using var command = Fixture.Database.DataSource.CreateCommand($"SELECT payload FROM {PersistenceSchema.TableName} WHERE key_hash = @key_hash");
        command.AddParameter("key_hash", keyHash, DbType.Binary);
        return (byte[])(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }
}
