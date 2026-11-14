using Orleans.Transactions.Abstractions;

namespace Scarlet.Orleans.Transactions.Tests;

public sealed class StorageFaultInjectorTests
{
    [Fact]
    public async Task InFlightStore_CannotConsumeAFaultArmedWhileItWasAwaitingStorage()
    {
        var inner = new GatedStorage();
        var injector = new FaultInjectionFixture.StorageFaultInjector();
        var storage = new FaultInjectionFixture.FaultInjectingStorage<State>(inner, injector);
        var earlier = StoreAsync(storage);
        Assert.Equal(1, inner.Calls);

        injector.InjectAfterStore = true;
        inner.Complete();
        Assert.Equal("etag", await earlier);
        Assert.True(injector.InjectAfterStore);
        await Assert.ThrowsAsync<InjectedStorageException>(() => StoreAsync(storage));
        Assert.Equal("etag", await StoreAsync(storage));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UnrelatedStore_CannotConsumeTheTargetTransactionsFault(bool before)
    {
        var target = Guid.NewGuid();
        var injector = new FaultInjectionFixture.StorageFaultInjector
        {
            TargetTransactionId = target,
            InjectBeforeStore = before,
            InjectAfterStore = !before,
        };
        var inner = new GatedStorage();
        inner.Complete();
        var storage = new FaultInjectionFixture.FaultInjectingStorage<State>(inner, injector);

        Assert.Equal("etag", await StoreAsync(storage));
        Assert.Equal("etag", await StoreAsync(storage, Guid.NewGuid()));
        await Assert.ThrowsAsync<InjectedStorageException>(() => StoreAsync(storage, target));
        Assert.Equal(before ? 2 : 3, inner.Calls);
        Assert.Equal("etag", await StoreAsync(storage, target));
    }

    [Fact]
    public async Task TargetCommitRecord_ConsumesTheFault()
    {
        var target = Guid.NewGuid();
        var injector = new FaultInjectionFixture.StorageFaultInjector { TargetTransactionId = target, InjectBeforeStore = true };
        var inner = new GatedStorage();
        var storage = new FaultInjectionFixture.FaultInjectingStorage<State>(inner, injector);
        var metadata = new TransactionalStateMetaData();
        metadata.CommitRecords.Add(target, new CommitRecord());

        await Assert.ThrowsAsync<InjectedStorageException>(() => storage.Store(null, metadata, null, null, null));
        Assert.Equal(0, inner.Calls);
    }

    [Fact]
    public async Task GenericAfterStore_ThrowsAGenericExceptionAfterStorageCompletes()
    {
        var injector = new FaultInjectionFixture.StorageFaultInjector { InjectGenericAfterStore = true };
        var inner = new GatedStorage();
        var storage = new FaultInjectionFixture.FaultInjectingStorage<State>(inner, injector);
        var store = StoreAsync(storage);
        Assert.False(store.IsCompleted);

        inner.Complete();
        await Assert.ThrowsAsync<InvalidOperationException>(() => store);
        Assert.Equal("etag", await StoreAsync(storage));
    }

    private static Task<string> StoreAsync(FaultInjectionFixture.FaultInjectingStorage<State> storage, Guid? transaction = null) =>
        storage.Store(null, new TransactionalStateMetaData(), transaction is { } id
            ? [new PendingTransactionState<State> { TransactionId = id.ToString(), State = new State() }]
            : null, null, null);

    public sealed class State;

    private sealed class GatedStorage : ITransactionalStateStorage<State>
    {
        private readonly TaskCompletionSource<string> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Calls { get; private set; }

        public void Complete() => completion.TrySetResult("etag");

        public Task<TransactionalStorageLoadResponse<State>> Load() => throw new NotSupportedException();

        public Task<string> Store(string? expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<State>>? statesToPrepare, long? commitUpTo, long? abortAfter)
        {
            Calls++;
            return completion.Task;
        }
    }
}
