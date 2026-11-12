using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Scarlet.Orleans.Streaming.Tests;

/// <summary>
/// The batch container's contract with Orleans: it survives the serializer whole, yields its events of a type with tokens
/// after its position in order, and restores the producer's request context.
/// </summary>
public sealed class RelationalBatchContainerTests
{
    private static readonly Serializer Serializer = new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<Serializer>();

    [Fact]
    public void Serializer_RoundTripsTheStreamEventsAndContext()
    {
        var stream = StreamId.Create("ns", "key");
        var batch = new RelationalBatchContainer(stream, [1, "two", 3], new() { ["trace"] = "abc" });

        var copy = Serializer.Deserialize<RelationalBatchContainer>(Serializer.SerializeToArray(batch))!;

        Assert.Equal(stream, copy.StreamId);
        Assert.Equal<object>([1, "two", 3], copy.Events);
        Assert.Equal("abc", copy.RequestContext!["trace"]);
        Assert.Null(copy.Position);
    }

    [Fact]
    public void GetEvents_YieldsTheEventsOfAType_WithTokensAfterThePositionInOrder()
    {
        var batch = new RelationalBatchContainer(StreamId.Create("ns", "key"), [1, "two", 3], null) { Position = new EventSequenceTokenV2(42) };

        var events = batch.GetEvents<int>().ToArray();

        Assert.Equal([1, 3], events.Select(e => e.Item1));
        Assert.All(events, e => Assert.Equal(42, e.Item2.SequenceNumber));
        Assert.True(events[0].Item2.CompareTo(events[1].Item2) < 0);
        Assert.Equal(42, batch.SequenceToken.SequenceNumber);
    }

    [Fact]
    public void SequenceToken_OfABatchNotYetDequeued_Throws() =>
        Assert.Throws<InvalidOperationException>(() => new RelationalBatchContainer(StreamId.Create("ns", "key"), [1], null).SequenceToken);

    [Fact]
    public void ImportRequestContext_RestoresTheProducersContext()
    {
        RequestContext.Clear();
        var batch = new RelationalBatchContainer(StreamId.Create("ns", "key"), [1], new() { ["trace"] = "abc" });

        Assert.True(batch.ImportRequestContext());
        Assert.Equal("abc", RequestContext.Get("trace"));
        Assert.False(new RelationalBatchContainer(StreamId.Create("ns", "key"), [1], null).ImportRequestContext());
        RequestContext.Clear();
    }
}
