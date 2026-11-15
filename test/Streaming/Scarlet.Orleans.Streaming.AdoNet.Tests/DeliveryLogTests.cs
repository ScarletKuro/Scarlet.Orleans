using Orleans.Providers.Streams.Common;
using Xunit.Sdk;

namespace Scarlet.Orleans.Streaming.Tests;

public sealed class DeliveryLogTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SequenceAssertion_RejectsReorderingAndDuplicates_WithFullDeliveryDetails(bool duplicate)
    {
        var stream = Guid.NewGuid();
        int[] actual = duplicate ? [0, 1, 1, 2] : [0, 2, 1];
        for (var i = 0; i < actual.Length; i++)
            DeliveryLog.Record(stream, actual[i], new EventSequenceTokenV2(100 + i), "test-silo");

        var failure = Assert.ThrowsAny<XunitException>(() => DeliveryLog.AssertInOrder(stream, [0, 1, 2]));

        Assert.Contains(stream.ToString(), failure.Message, StringComparison.Ordinal);
        Assert.Contains($"Actual: [{string.Join(", ", actual)}]", failure.Message, StringComparison.Ordinal);
        Assert.Contains("token=100/0 consumer_silo=test-silo", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Provider warnings:", failure.Message, StringComparison.Ordinal);
    }
}
