using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans;
using Orleans.Runtime;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// The traces and metrics that reminder table operations publish under the <c>Scarlet.Orleans</c> source and meter.
/// </summary>
public abstract class ReminderDiagnosticsTests(DatabaseFixture fixture)
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Operations_RecordTheirDurationAndOutcome_AndTraceASpan()
    {
        var measurements = new ConcurrentBag<(string Operation, string Outcome)>();
        using var meters = new MeterListener();
        meters.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "Scarlet.Orleans" && instrument.Name == "scarlet.orleans.reminders.duration")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meters.SetMeasurementEventCallback<double>((_, _, tags, _) => measurements.Add((Tag(tags, "orleans.reminders.operation"), Tag(tags, "outcome"))));
        meters.Start();

        var spans = new ConcurrentBag<Activity>();
        using var activities = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Scarlet.Orleans",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = spans.Add,
        };
        ActivitySource.AddActivityListener(activities);

        var table = FlavourTable.Create(fixture.Database, $"service-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await table.StartAsync(CancellationToken);
        var grainId = GrainId.Create("diagnostics", $"{Guid.NewGuid():N}");
        var etag = await table.UpsertRow(new ReminderEntry { GrainId = grainId, ReminderName = "r", StartAt = DateTime.UtcNow, Period = TimeSpan.FromMinutes(1) }, CancellationToken);
        await table.ReadRow(grainId, "r", CancellationToken);
        await table.ReadRows(grainId, CancellationToken);
        await table.ReadRows(0, 0, CancellationToken);
        await table.RemoveRow(grainId, "r", etag!, CancellationToken);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.ReadRow(grainId, "r", new CancellationToken(canceled: true)));

        Assert.Contains(("upsert", "success"), measurements);
        Assert.Contains(("read", "success"), measurements);
        Assert.Contains(("read_grain", "success"), measurements);
        Assert.Contains(("read_range", "success"), measurements);
        Assert.Contains(("remove", "success"), measurements);
        Assert.Contains(("read", "canceled"), measurements);
        Assert.Contains(spans, span => span.OperationName == "reminders upsert" && (string?)span.GetTagItem("outcome") == "success");
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
