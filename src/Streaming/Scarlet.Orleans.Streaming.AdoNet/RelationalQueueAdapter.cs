using System.Transactions;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// Enqueues batches and creates the receivers of one provider. Queues are not rewindable: a dequeued and confirmed
/// message is gone.
/// </summary>
internal sealed class RelationalQueueAdapter(
    string name,
    RelationalQueueAdapterFactory factory,
    IStreamQueueStore store,
    Serializer serializer,
    ScarletAdoNetStreamOptions options,
    TimeProvider timeProvider) : IQueueAdapter
{
    public string Name { get; } = name;

    public bool IsRewindable => false;

    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId) => factory.CreateReceiver(queueId);

    /// <summary>
    /// Stores the events as one message, with a key generated once for this call, so a retry after an ambiguous
    /// failure stores it once (design section 4.7). An application's ambient transaction is suppressed (section 4.6).
    /// </summary>
    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken? token, Dictionary<string, object>? requestContext)
    {
        if (token is not null)
        {
            throw new ArgumentException("Relational streams are not rewindable, so a batch cannot carry a sequence token.", nameof(token));
        }

        var queue = factory.GetQueue(streamId);
        var payload = serializer.SerializeToArray(new RelationalBatchContainer(streamId, [.. events.Cast<object>()], requestContext));
        var now = timeProvider.GetUtcNow();
        using var suppress = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await store.EnqueueAsync(queue, Guid.NewGuid(), payload, now, now + options.ExpiryTimeout, CancellationToken.None).ConfigureAwait(false);
    }
}
