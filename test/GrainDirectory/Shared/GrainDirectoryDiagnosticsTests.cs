using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans.Runtime;

namespace Scarlet.Orleans.GrainDirectory.Tests;

/// <summary>
/// The traces and metrics that grain directory operations publish under the <c>Scarlet.Orleans</c> source and meter.
/// </summary>
public abstract class GrainDirectoryDiagnosticsTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Operations_RecordTheirDurationAndOutcome_AndTraceASpan()
    {
        var name = $"diagnostics-{Guid.NewGuid():N}";
        var measurements = new ConcurrentBag<(string Operation, string Outcome)>();
        using var meters = new MeterListener();
        meters.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Scarlet.Orleans" && instrument.Name == "scarlet.orleans.grain_directory.duration")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meters.SetMeasurementEventCallback<double>((_, _, tags, _) =>
        {
            if (Tag(tags, "orleans.grain_directory.name") == name)
            {
                measurements.Add((Tag(tags, "orleans.grain_directory.operation"), Tag(tags, "outcome")));
            }
        });
        meters.Start();

        var spans = new ConcurrentBag<Activity>();
        using var activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Scarlet.Orleans",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(activities);

        var directory = FlavourDirectory.Create(fixture.Database, name, $"cluster-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await directory.InitializeAsync(CancellationToken);
        var silo = SiloAddress.FromParsableString("10.0.23.12:1000@5678");
        var address = new GrainAddress { ActivationId = ActivationId.NewId(), GrainId = GrainId.Create("diagnostics", "key"), SiloAddress = silo };
        await directory.Register(address, CancellationToken);
        await directory.Lookup(address.GrainId, CancellationToken);
        await directory.Unregister(address, CancellationToken);
        await directory.UnregisterSilos([silo], CancellationToken);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => directory.Lookup(address.GrainId, new CancellationToken(canceled: true)));

        Assert.Contains(("register", "success"), measurements);
        Assert.Contains(("lookup", "success"), measurements);
        Assert.Contains(("unregister", "success"), measurements);
        Assert.Contains(("unregister_silos", "success"), measurements);
        Assert.Contains(("lookup", "canceled"), measurements);
        Assert.Contains(spans, span => span.OperationName == "grain_directory register"
            && (string?)span.GetTagItem("orleans.grain_directory.name") == name
            && (string?)span.GetTagItem("outcome") == "success");
    }

    private static string Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string name)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == name)
            {
                return (string)tag.Value!;
            }
        }

        return "";
    }
}
