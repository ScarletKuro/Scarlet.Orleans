using System.Collections.Concurrent;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// The parts of one relational stream provider, on a silo or a client: one store, one queue mapper, one cache, and the
/// cache positions its receivers and failure handler share.
/// </summary>
/// <remarks>
/// The store is initialized, checking or applying the schema, when Orleans first creates the adapter, which it does
/// while the silo or client starts, so a missing schema fails startup.
/// </remarks>
internal sealed class RelationalQueueAdapterFactory : IQueueAdapterFactory
{
    private readonly string name;
    private readonly string serviceId;
    private readonly IStreamQueueStore store;
    private readonly Serializer serializer;
    private readonly ScarletAdoNetStreamOptions options;
    private readonly TimeProvider timeProvider;
    private readonly ILoggerFactory loggerFactory;
    private readonly HashRingBasedStreamQueueMapper mapper;
    private readonly SimpleQueueAdapterCache cache;
    private readonly StreamPositions positions = new();
    private readonly ConcurrentDictionary<QueueId, StreamQueueKey> queues = new();
    private readonly ConcurrentDictionary<QueueId, RelationalQueueAdapterReceiver> receivers = new();
    private readonly Lock creationLock = new();
    private readonly StreamingObserver observer;
    private readonly RelationalStreamFailureHandler failureHandler;
    private Task<IQueueAdapter>? creation;

    public RelationalQueueAdapterFactory(
        string name,
        string serviceId,
        IStreamQueueStore store,
        Serializer serializer,
        ScarletAdoNetStreamOptions options,
        HashRingStreamQueueMapperOptions mapperOptions,
        SimpleQueueCacheOptions cacheOptions,
        TimeProvider timeProvider,
        ILoggerFactory loggerFactory)
    {
        this.name = name;
        this.serviceId = serviceId;
        this.store = store;
        this.serializer = serializer;
        this.options = options;
        this.timeProvider = timeProvider;
        this.loggerFactory = loggerFactory;
        mapper = new HashRingBasedStreamQueueMapper(mapperOptions, name);
        cache = new SimpleQueueAdapterCache(cacheOptions, name, loggerFactory);
        observer = new StreamingObserver(name, timeProvider);
        failureHandler = new RelationalStreamFailureHandler(store, positions, options, observer, timeProvider);
        StreamingObserver.Track(this, name);
    }

    /// <summary>The queues this silo reads now: those whose receiver it created and has not shut down.</summary>
    public IReadOnlyCollection<QueueId> OwnedQueues => [.. receivers.Keys];

    /// <summary>The cache positions that still resolve to an attempt; none outlive the attempts held.</summary>
    public int LivePositions => positions.Count;

    /// <summary>Messages this silo's receivers hold, dequeued and not yet confirmed or released.</summary>
    public int HeldMessages => receivers.Values.Sum(static r => r.HeldMessages);

    public Task<IQueueAdapter> CreateAdapter() => CreateAdapter(CancellationToken.None);

    /// <summary>
    /// Initializes the store once, and creates the adapter. A failed initialization is started again by the next call.
    /// </summary>
    public Task<IQueueAdapter> CreateAdapter(CancellationToken cancellationToken)
    {
        lock (creationLock)
        {
            if (creation is null || creation.IsFaulted || creation.IsCanceled)
            {
                creation = CreateCoreAsync(cancellationToken);
            }

            return creation;
        }

        async Task<IQueueAdapter> CreateCoreAsync(CancellationToken cancellationToken)
        {
            using (new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
            {
                await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
            }

            return new RelationalQueueAdapter(name, this, store, serializer, options, observer, timeProvider);
        }
    }

    public IQueueAdapterCache GetQueueAdapterCache() => cache;

    public IStreamQueueMapper GetStreamQueueMapper() => mapper;

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) => Task.FromResult<IStreamFailureHandler>(failureHandler);

    /// <summary>
    /// Creates the receiver of a queue this silo now owns. It replaces any earlier one for the queue, and leaves the
    /// owned queues when it shuts down.
    /// </summary>
    public RelationalQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var receiver = new RelationalQueueAdapterReceiver(
            GetQueue(queueId), store, positions, serializer, options, observer, timeProvider, loggerFactory.CreateLogger<RelationalQueueAdapterReceiver>(),
            onShutdown: stopped => receivers.TryRemove(new KeyValuePair<QueueId, RelationalQueueAdapterReceiver>(queueId, stopped)));
        receivers[queueId] = receiver;
        return receiver;
    }

    /// <summary>The queue a stream's batches go to, by the same mapper the pulling agents read with.</summary>
    public StreamQueueKey GetQueue(StreamId streamId) => GetQueue(mapper.GetQueueForStream(streamId));

    private StreamQueueKey GetQueue(QueueId queueId) =>
        queues.GetOrAdd(queueId, static (id, self) => StreamQueueKey.Create(self.serviceId, self.name, id.ToString()), this);
}
