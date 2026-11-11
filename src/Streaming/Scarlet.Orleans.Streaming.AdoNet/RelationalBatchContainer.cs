using Orleans;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// The events of one enqueue, stored as a message's payload, and delivered with the cache position the receiver gave
/// that message's dequeue.
/// </summary>
[GenerateSerializer]
[Alias("Scarlet.Orleans.Streaming.AdoNet.RelationalBatchContainer")]
internal sealed class RelationalBatchContainer(StreamId streamId, List<object> events, Dictionary<string, object>? requestContext) : IBatchContainer
{
    [Id(0)]
    public StreamId StreamId { get; } = streamId;

    [Id(1)]
    public List<object> Events { get; } = events;

    [Id(2)]
    public Dictionary<string, object>? RequestContext { get; } = requestContext;

    /// <summary>
    /// The position the receiver assigned this dequeue, which only increases (design section 5.6); not the message ID.
    /// Stored payloads carry none.
    /// </summary>
    [Id(3)]
    public EventSequenceTokenV2? Position { get; set; }

    public StreamSequenceToken SequenceToken => Position ?? throw new InvalidOperationException("The batch has not been dequeued.");

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        var position = (EventSequenceTokenV2)SequenceToken;
        return Events.OfType<T>().Select((e, i) => Tuple.Create<T, StreamSequenceToken>(e, position.CreateSequenceTokenForEvent(i)));
    }

    public bool ImportRequestContext()
    {
        if (RequestContext is null)
        {
            return false;
        }

        RequestContextExtensions.Import(RequestContext);
        return true;
    }

    public override string ToString() => $"[{nameof(RelationalBatchContainer)}: Stream={StreamId}, Events={Events.Count}, Position={Position?.SequenceNumber}]";
}
