using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Transactions;
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
        silo.Services.AddTransient<IFaultInjectionTransactionalStateFactory, TargetedStateFactory>();
        silo.Services.AddScoped<StorageFaultInjector>();
        silo.Services.AddScoped<IControlledTransactionFaultInjector>(services => services.GetRequiredService<StorageFaultInjector>());
        silo.Services.AddScoped<ITransactionFaultInjector>(services => services.GetRequiredService<IControlledTransactionFaultInjector>());
    }

    private sealed class FaultInjectingStorageFactory(ITransactionalStateStorageFactory inner) : ITransactionalStateStorageFactory
    {
        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainContext context)
            where TState : class, new() =>
            new FaultInjectingStorage<TState>(inner.Create<TState>(stateName, context), context.ActivationServices.GetRequiredService<StorageFaultInjector>());
    }

    // The TestKit's transaction-scoped injector interface is internal. Capture the target from its public state facet
    // before the protocol consumes FaultInjectionControl, so a previous transaction's cleanup cannot take its fault.
    private sealed class TargetedStateFactory(IGrainContextAccessor accessor) : IFaultInjectionTransactionalStateFactory
    {
        public IFaultInjectionTransactionalState<TState> Create<TState>(IFaultInjectionTransactionalStateConfiguration config)
            where TState : class, new() =>
            new TargetedState<TState>(new FaultInjectionTransactionalStateFactory(accessor).Create<TState>(config),
                accessor.GrainContext.ActivationServices.GetRequiredService<StorageFaultInjector>());
    }

    private sealed class TargetedState<TState>(IFaultInjectionTransactionalState<TState> inner, StorageFaultInjector injector)
        : IFaultInjectionTransactionalState<TState> where TState : class, new()
    {
        public FaultInjectionControl FaultInjectionControl { get => inner.FaultInjectionControl; set => inner.FaultInjectionControl = value; }

        public Task<TResult> PerformRead<TResult>(Func<TState, TResult> readFunction) => inner.PerformRead(readFunction);

        public Task<TResult> PerformUpdate<TResult>(Func<TState, TResult> updateFunction)
        {
            injector.TargetTransactionId = FaultInjectionControl.FaultInjectionPhase == TransactionFaultInjectPhase.BeforePrepareAndCommit
                ? Guid.Parse(TransactionContext.CurrentTransactionId)
                : null;
            return inner.PerformUpdate(updateFunction);
        }
    }

    internal sealed class FaultInjectingStorage<TState>(ITransactionalStateStorage<TState> inner, StorageFaultInjector injector) : ITransactionalStateStorage<TState>
        where TState : class, new()
    {
        public Task<TransactionalStorageLoadResponse<TState>> Load() => inner.Load();

        public async Task<string> Store(string? expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>>? statesToPrepare, long? commitUpTo, long? abortAfter)
        {
            var isTarget = injector.TargetTransactionId is not { } target
                || metadata.CommitRecords.ContainsKey(target)
                || statesToPrepare?.Any(state => state.TransactionId == target.ToString()) == true;
            // Consume at entry, before awaiting the database. A store already in flight when a fault is armed must
            // neither throw it on completion nor clear a fault armed for a later store.
            var before = isTarget && injector.InjectBeforeStore;
            var after = isTarget && injector.InjectAfterStore;
            var genericAfter = isTarget && injector.InjectGenericAfterStore;
            if (isTarget)
            {
                injector.InjectBeforeStore = injector.InjectAfterStore = injector.InjectGenericAfterStore = false;
            }

            if (before)
            {
                throw new InjectedStorageException("Storage failure injected before the store.");
            }

            var etag = await inner.Store(expectedETag, metadata, statesToPrepare, commitUpTo, abortAfter);
            if (after)
            {
                throw new InjectedStorageException("Storage failure injected after the store committed.");
            }

            if (genericAfter)
            {
                throw new InvalidOperationException("Generic storage failure injected after the store committed.");
            }

            return etag;
        }
    }

    /// <summary>
    /// Fails the next store once when armed: before it reaches the database, or after it committed, so the grain does
    /// not learn that it did.
    /// </summary>
    internal sealed class StorageFaultInjector : IControlledTransactionFaultInjector
    {
        public Guid? TargetTransactionId { get; set; }

        public bool InjectBeforeStore { get; set; }

        public bool InjectAfterStore { get; set; }

        public bool InjectGenericAfterStore { get; set; }

        public void BeforeStore() => throw new NotSupportedException("Faults must be consumed by the targeted storage wrapper.");

        public void AfterStore() => throw new NotSupportedException("Faults must be consumed by the targeted storage wrapper.");
    }
}

[GenerateSerializer]
public sealed class InjectedStorageException(string message) : Exception(message);
