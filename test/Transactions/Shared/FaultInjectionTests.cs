using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions.TestKit;
using Orleans.Transactions.TestKit.xUnit;

namespace Scarlet.Orleans.Transactions.Tests;

/// <summary>
/// Orleans' controlled fault injection runner: storage failures before and after a store, and deactivations at each
/// transaction phase, with the storage under test underneath. Upstream builds its injectors on the Azure and DynamoDB
/// providers; this one wraps any <see cref="ITransactionalStateStorageFactory"/>.
/// </summary>
public abstract class FaultInjectionTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : ControlledFaultInjectionTransactionTestRunnerxUnit(fixture.GrainFactory, output);

/// <summary>
/// A cluster whose transaction store fails on demand: the storage under test, registered under another name, behind a
/// factory that consults the grain's <see cref="IControlledTransactionFaultInjector"/> around each store.
/// </summary>
public abstract class FaultInjectionFixture(DatabaseFixture database) : TransactionsClusterFixture(database)
{
    private const string InnerName = "FaultInjectionInner";

    protected override void ConfigureStorage(ISiloBuilder silo, string name)
    {
        base.ConfigureStorage(silo, InnerName);
        silo.Services.AddKeyedSingleton<ITransactionalStateStorageFactory>(name, (services, _) =>
            new FaultInjectingStorageFactory(services.GetRequiredKeyedService<ITransactionalStateStorageFactory>(InnerName)));
        silo.UseControlledFaultInjectionTransactionState();
        silo.Services.AddScoped<IControlledTransactionFaultInjector, StorageFaultInjector>();
        silo.Services.AddScoped<ITransactionFaultInjector>(services => services.GetRequiredService<IControlledTransactionFaultInjector>());
    }

    private sealed class FaultInjectingStorageFactory(ITransactionalStateStorageFactory inner) : ITransactionalStateStorageFactory
    {
        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainContext context)
            where TState : class, new() =>
            new FaultInjectingStorage<TState>(inner.Create<TState>(stateName, context), context.ActivationServices.GetRequiredService<ITransactionFaultInjector>());
    }

    private sealed class FaultInjectingStorage<TState>(ITransactionalStateStorage<TState> inner, ITransactionFaultInjector injector) : ITransactionalStateStorage<TState>
        where TState : class, new()
    {
        public Task<TransactionalStorageLoadResponse<TState>> Load() => inner.Load();

        public async Task<string> Store(string? expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>>? statesToPrepare, long? commitUpTo, long? abortAfter)
        {
            injector.BeforeStore();
            var etag = await inner.Store(expectedETag, metadata, statesToPrepare, commitUpTo, abortAfter);
            injector.AfterStore();
            return etag;
        }
    }

    /// <summary>
    /// Fails the next store once when armed: before it reaches the database, or after it committed, so the grain does
    /// not learn that it did.
    /// </summary>
    private sealed class StorageFaultInjector : IControlledTransactionFaultInjector
    {
        public bool InjectBeforeStore { get; set; }

        public bool InjectAfterStore { get; set; }

        public bool InjectGenericAfterStore { get; set; }

        public void BeforeStore()
        {
            if (InjectBeforeStore)
            {
                InjectBeforeStore = false;
                throw new InjectedStorageException("Storage failure injected before the store.");
            }
        }

        public void AfterStore()
        {
            if (InjectAfterStore || InjectGenericAfterStore)
            {
                InjectAfterStore = InjectGenericAfterStore = false;
                throw new InjectedStorageException("Storage failure injected after the store committed.");
            }
        }
    }
}

[GenerateSerializer]
public sealed class InjectedStorageException(string message) : Exception(message);
