using Orleans.Runtime;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// Fails the attempt behind a failed delivery: the message becomes visible again for another attempt, or, out of
/// attempts, moves to the dead letters.
/// </summary>
/// <remarks>
/// Orleans passes the cache position of the failed batch. The position resolves to the queue, the message and the
/// receipt of the attempt that was delivered, so a failure reported late for an attempt that has since been redelivered
/// changes nothing. A position that no longer resolves (confirmed, redelivered, expired or released) is ignored.
/// </remarks>
internal sealed class RelationalStreamFailureHandler(
    IStreamQueueStore store,
    StreamPositions positions,
    ScarletAdoNetStreamOptions options,
    StreamingObserver observer,
    TimeProvider timeProvider) : IStreamFailureHandler
{
    public bool ShouldFaultSubsriptionOnError => false;

    public Task OnDeliveryFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken? sequenceToken) =>
        FailAsync(sequenceToken);

    public Task OnSubscriptionFailure(GuidId subscriptionId, string streamProviderName, StreamId streamIdentity, StreamSequenceToken? sequenceToken) =>
        FailAsync(sequenceToken);

    private async Task FailAsync(StreamSequenceToken? sequenceToken)
    {
        if (sequenceToken is null || !positions.TryGet(sequenceToken.SequenceNumber, out var attempt))
        {
            return;
        }

        var now = timeProvider.GetUtcNow();
        var outcome = await observer.ObserveAsync("fail", attempt.Queue, () =>
            store.FailAsync(attempt.Queue, attempt.Receipt, options.MaxAttempts, now, now + options.DeadLetterRetention, CancellationToken.None)).ConfigureAwait(false);
        observer.Count(outcome switch { FailOutcome.Retried => "retried", FailOutcome.DeadLettered => "dead_lettered", _ => "stale" }, 1);
    }
}
