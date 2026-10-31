using System.Transactions;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.Transactions.Abstractions;

namespace Scarlet.Orleans.Transactions;

/// <summary>
/// Creates the storage of each transactional state of one named storage, and checks the schema when the silo starts.
/// </summary>
internal sealed partial class RelationalTransactionalStateStorageFactory(
    string name,
    string serviceId,
    ITransactionalStateStore store,
    IGrainStorageSerializer stateSerializer,
    Serializer serializer,
    int initStage,
    TimeProvider timeProvider,
    ILoggerFactory loggerFactory) : ITransactionalStateStorageFactory, ILifecycleParticipant<ISiloLifecycle>
{
    private readonly ILogger logger = loggerFactory.CreateLogger<RelationalTransactionalStateStorageFactory>();
    private readonly TransactionalStateObserver observer = new(name, timeProvider, loggerFactory.CreateLogger<TransactionalStateObserver>());

    public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainContext context)
        where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(context);
        return new RelationalTransactionalStateStorage<TState>(
            TransactionalStateKey.Create(serviceId, stateName, context.GrainId), store, stateSerializer, serializer, observer, timeProvider);
    }

    public void Participate(ISiloLifecycle lifecycle) =>
        lifecycle.Subscribe($"{nameof(RelationalTransactionalStateStorageFactory)}-{name}", initStage, InitializeAsync);

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        using (new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
        {
            await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        }

        LogInitialized(name, serviceId);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Initialized transactional state storage {Name} for service {ServiceId}.")]
    private partial void LogInitialized(string name, string serviceId);
}
