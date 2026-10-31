using System.Data;
using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.Transactions;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions.TestKit.xUnit;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions.Tests;

/// <summary>
/// Orleans' storage runner (<see cref="TransactionalStateStorageTestRunnerxUnit{TState}"/>), and the provider's own
/// guarantees: strict ETags, commits that move a number rather than copy a payload, identities and round trips.
/// </summary>
public abstract class TransactionalStateStorageTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : TransactionalStateStorageTestRunnerxUnit<TestState>(() => CreateStorageAsync(fixture, NewGrainId()), seed => new TestState { State = seed }, fixture.GrainFactory, output)
{
    private static readonly DateTime Start = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task StoreWithNoChanges_StillRotatesTheETag_AndFailsTheStaleWriter()
    {
        var (writer, staleWriter, writerLoad, staleLoad) = await CreateWritersAsync();

        var updated = await writer.Store(writerLoad.ETag, writerLoad.Metadata, [], null, null);

        Assert.NotEqual(writerLoad.ETag, updated);
        await Assert.ThrowsAsync<InconsistentStateException>(() => staleWriter.Store(staleLoad.ETag, staleLoad.Metadata, [], null, null));
    }

    [Fact]
    public async Task StaleWriter_ConflictsUntilItLoads_ThenConverges()
    {
        var (writer, staleWriter, writerLoad, staleLoad) = await CreateWritersAsync();
        await writer.Store(writerLoad.ETag, new TransactionalStateMetaData { TimeStamp = Start }, [], null, null);

        await Assert.ThrowsAsync<InconsistentStateException>(() => staleWriter.Store(staleLoad.ETag, staleLoad.Metadata, [], null, null));
        await Assert.ThrowsAsync<InconsistentStateException>(() => staleWriter.Store(staleLoad.ETag, staleLoad.Metadata, [], null, null));

        var refreshed = await staleWriter.Load();
        Assert.Equal(Start, refreshed.Metadata.TimeStamp);
        var etag = await staleWriter.Store(refreshed.ETag, new TransactionalStateMetaData { TimeStamp = Start.AddSeconds(1) }, [], null, null);
        var converged = await writer.Load();
        Assert.Equal(etag, converged.ETag);
        Assert.Equal(Start.AddSeconds(1), converged.Metadata.TimeStamp);
    }

    [Fact]
    public async Task Commit_KeepsOnlyTheCommittedAndLaterEntries()
    {
        var grainId = NewGrainId();
        var storage = await CreateStorageAsync(fixture, grainId);
        var etag = await storage.Store(null, new TransactionalStateMetaData(), [Pending(1), Pending(2), Pending(3)], null, null);

        await storage.Store(etag, new TransactionalStateMetaData(), [], commitUpTo: 2, abortAfter: null);

        var loaded = await storage.Load();
        Assert.Equal(2, loaded.CommittedSequenceId);
        Assert.Equal(new TestState { State = 2, Payload = "2" }, loaded.CommittedState);
        Assert.Equal(3, Assert.Single(loaded.PendingStates).SequenceId);
        Assert.Equal(new long[] { 2, 3 }, await StoredSequencesAsync(grainId));
    }

    [Fact]
    public async Task CommitBelowTheCommittedVersion_ChangesNothing()
    {
        var storage = await CreateStorageAsync(fixture, NewGrainId());
        var etag = await storage.Store(null, new TransactionalStateMetaData(), [Pending(1), Pending(2)], commitUpTo: 2, abortAfter: null);

        etag = await storage.Store(etag, new TransactionalStateMetaData(), [], commitUpTo: 1, abortAfter: null);

        var loaded = await storage.Load();
        Assert.Equal(etag, loaded.ETag);
        Assert.Equal(2, loaded.CommittedSequenceId);
        Assert.Equal(new TestState { State = 2, Payload = "2" }, loaded.CommittedState);
    }

    [Fact]
    public async Task CommitWithoutItsPrepareRecord_FailsAndChangesNothing()
    {
        var storage = await CreateStorageAsync(fixture, NewGrainId());
        var etag = await storage.Store(null, new TransactionalStateMetaData(), [Pending(1)], null, null);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => storage.Store(etag, new TransactionalStateMetaData(), [], commitUpTo: 2, abortAfter: null));

        Assert.Contains("Missing prepare record (SequenceId=2)", exception.Message, StringComparison.Ordinal);
        var loaded = await storage.Load();
        Assert.Equal(etag, loaded.ETag);
        Assert.Equal(0, loaded.CommittedSequenceId);
        Assert.Equal(1, Assert.Single(loaded.PendingStates).SequenceId);
    }

    [Fact]
    public async Task PendingStates_RoundTrip_WithTheirTransactionManagers()
    {
        var storage = await CreateStorageAsync(fixture, NewGrainId());
        var manager = new ParticipantId("manager", (GrainReference)fixture.GrainFactory.GetGrain(GrainId.Create("manager", "1")), ParticipantId.Role.Resource | ParticipantId.Role.Manager);
        var pending = Pending(1);
        pending.TransactionManager = manager;
        pending.TimeStamp = new DateTime(Start.Ticks + 1234567, DateTimeKind.Utc);
        var metadata = new TransactionalStateMetaData { TimeStamp = Start.AddTicks(7) };
        metadata.CommitRecords[Guid.NewGuid()] = new CommitRecord { Timestamp = Start, WriteParticipants = [manager] };

        await storage.Store(null, metadata, [pending, Pending(2)], null, null);
        var loaded = await storage.Load();

        Assert.Equal(metadata.TimeStamp, loaded.Metadata.TimeStamp);
        Assert.Equal(manager, Assert.Single(Assert.Single(loaded.Metadata.CommitRecords).Value.WriteParticipants));
        Assert.Equal(manager, loaded.PendingStates[0].TransactionManager);
        Assert.Equal(pending.TimeStamp.Ticks, loaded.PendingStates[0].TimeStamp.Ticks);
        Assert.Equal(pending.TransactionId, loaded.PendingStates[0].TransactionId);
        Assert.Equal(default, loaded.PendingStates[1].TransactionManager);
    }

    [Fact]
    public async Task States_AreIsolated_ByName_Grain_AndService()
    {
        var grainId = NewGrainId();
        var store = await CreateStoreAsync(fixture);
        var states = new[]
        {
            CreateStorage(fixture, store, grainId, "state", "service"),
            CreateStorage(fixture, store, grainId, "other-state", "service"),
            CreateStorage(fixture, store, NewGrainId(), "state", "service"),
            CreateStorage(fixture, store, grainId, "state", "other-service"),
        };

        for (var i = 0; i < states.Length; i++)
        {
            await states[i].Store(null, new TransactionalStateMetaData(), [Pending(i + 1)], commitUpTo: i + 1, abortAfter: null);
        }

        for (var i = 0; i < states.Length; i++)
        {
            var loaded = await states[i].Load();
            Assert.Equal(i + 1, loaded.CommittedSequenceId);
            Assert.Equal(i + 1, loaded.CommittedState.State);
        }
    }

    [Fact]
    public async Task Collision_OfAStateKey_FailsLoadsAndStores_AndChangesNothing()
    {
        var grainId = NewGrainId();
        var storage = await CreateStorageAsync(fixture, grainId);
        var etag = await storage.Store(null, new TransactionalStateMetaData(), [Pending(1)], null, null);
        await ExecuteAsync(
            $"UPDATE {TransactionsSchema.StateTableName} SET state_identity = @identity WHERE key_hash = @hash",
            ("identity", StorageIdentity.StrictUtf8.GetBytes("another state")),
            ("hash", Key(grainId).Hash));

        await Assert.ThrowsAsync<TransactionalStateKeyCollisionException>(storage.Load);
        await Assert.ThrowsAsync<TransactionalStateKeyCollisionException>(() => storage.Store(etag, new TransactionalStateMetaData(), [Pending(2)], 1, null));
        await Assert.ThrowsAsync<TransactionalStateKeyCollisionException>(() => storage.Store(null, new TransactionalStateMetaData(), [], null, null));

        Assert.Equal(new long[] { 1 }, await StoredSequencesAsync(grainId));
    }

    [Fact]
    public async Task AmbientTransaction_IsNotJoined()
    {
        var storage = await CreateStorageAsync(fixture, NewGrainId());

        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            await storage.Store(null, new TransactionalStateMetaData(), [Pending(1)], null, null);
        }

        Assert.Single((await storage.Load()).PendingStates);
    }

    private static async Task<ITransactionalStateStorage<TestState>> CreateStorageAsync(TransactionsClusterFixture fixture, GrainId grainId) =>
        CreateStorage(fixture, await CreateStoreAsync(fixture), grainId);

    private static async Task<ITransactionalStateStore> CreateStoreAsync(TransactionsClusterFixture fixture)
    {
        // The cluster applied the schema; initializing resolves the engine.
        var store = FlavourTransactions.CreateStore(fixture.Database.Database, TestSchemaMode.None);
        await store.InitializeAsync(TestContext.Current.CancellationToken);
        return store;
    }

    private static RelationalTransactionalStateStorage<TestState> CreateStorage(TransactionsClusterFixture fixture, ITransactionalStateStore store, GrainId grainId, string stateName = "state", string serviceId = "service") =>
        new RelationalTransactionalStateStorage<TestState>(
            TransactionalStateKey.Create(serviceId, stateName, grainId),
            store,
            fixture.SiloServices.GetRequiredService<IGrainStorageSerializer>(),
            fixture.SiloServices.GetRequiredService<Serializer>(),
            new TransactionalStateObserver("test", TimeProvider.System, NullLogger<TransactionalStateObserver>.Instance),
            TimeProvider.System);

    private async Task<(ITransactionalStateStorage<TestState> Writer, ITransactionalStateStorage<TestState> StaleWriter, TransactionalStorageLoadResponse<TestState> WriterLoad, TransactionalStorageLoadResponse<TestState> StaleLoad)> CreateWritersAsync()
    {
        var grainId = NewGrainId();
        var initializer = await CreateStorageAsync(fixture, grainId);
        await initializer.Store(null, new TransactionalStateMetaData(), [], null, null);
        var writer = await CreateStorageAsync(fixture, grainId);
        var staleWriter = await CreateStorageAsync(fixture, grainId);
        return (writer, staleWriter, await writer.Load(), await staleWriter.Load());
    }

    private static PendingTransactionState<TestState> Pending(long sequenceId) => new()
    {
        SequenceId = sequenceId,
        TransactionId = Guid.NewGuid().ToString(),
        TimeStamp = Start.AddSeconds(sequenceId),
        State = new TestState { State = (int)sequenceId, Payload = sequenceId.ToString(System.Globalization.CultureInfo.InvariantCulture) },
    };

    private static GrainId NewGrainId() => GrainId.Create("transactional", Guid.NewGuid().ToString("N"));

    private static TransactionalStateKey Key(GrainId grainId) => TransactionalStateKey.Create("service", "state", grainId);

    private async Task<long[]> StoredSequencesAsync(GrainId grainId)
    {
        await using var command = fixture.Database.Database.DataSource.CreateCommand(
            $"SELECT sequence_id FROM {TransactionsSchema.EntryTableName} WHERE key_hash = @hash ORDER BY sequence_id");
        command.AddParameter("hash", Key(grainId).Hash, DbType.Binary);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var sequences = new List<long>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            sequences.Add(reader.GetInt64(0));
        }

        return [.. sequences];
    }

    private async Task ExecuteAsync(string sql, params (string Name, byte[] Value)[] parameters)
    {
        await using var command = fixture.Database.Database.DataSource.CreateCommand(sql);
        foreach (var (name, value) in parameters)
        {
            command.AddParameter(name, value, DbType.Binary);
        }

        Assert.Equal(1, await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken));
    }
}
