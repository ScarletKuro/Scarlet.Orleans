using System.Collections.Concurrent;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// The cache positions of one provider: a counter that only increases, so the queue cache's tokens increase across reads
/// whatever order messages are dequeued in, and the message attempt each live position stands for.
/// </summary>
/// <remarks>
/// Positions are shared by a provider's receivers and its failure handler, which receives a position as the token of a
/// failed delivery. A position is removed when its attempt leaves the cache, is delivered again, expires or is released,
/// after which it resolves to nothing, rather than to an unrelated message.
/// </remarks>
internal sealed class StreamPositions
{
    private readonly ConcurrentDictionary<long, Attempt> attempts = new();
    private long last;

    public long Add(StreamQueueKey queue, MessageReceipt receipt)
    {
        var position = Interlocked.Increment(ref last);
        attempts[position] = new(queue, receipt);
        return position;
    }

    public bool TryGet(long position, out Attempt attempt) => attempts.TryGetValue(position, out attempt);

    public bool TryRemove(long position, out Attempt attempt) => attempts.TryRemove(position, out attempt);

    public void Remove(long position) => attempts.TryRemove(position, out _);

    /// <param name="Queue">The queue the message is in.</param>
    /// <param name="Receipt">The message and the receipt of this attempt.</param>
    internal readonly record struct Attempt(StreamQueueKey Queue, MessageReceipt Receipt);
}
