using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// The metrics and traces published under <c>Scarlet.Orleans</c>, as an OpenTelemetry exporter would receive them.
/// </summary>
/// <remarks>
/// Listeners see every storage in the process, so each test uses its own storage name and filters by it.
/// </remarks>
public abstract class DiagnosticsTests(DatabaseFixture fixture)
{
    private const string StateName = "state";
    private readonly string storageName = $"diagnostics-{Guid.NewGuid():N}";

    private async Task<RelationalGrainStorage> CreateStorageAsync(int? maxPayloadSize = null)
    {
        var storage = FlavourStorage.Create(FlavourStorage.CreateStore(fixture.Database, TestSchemaMode.Apply), $"service-{Guid.NewGuid():N}", deleteStateOnClear: false, maxPayloadSize, storageName);
        await storage.InitializeAsync(TestContext.Current.CancellationToken);
        return storage;
    }

    [Fact]
    public async Task Operations_AreMeasuredByOperationAndOutcome()
    {
        using var listener = new Measurements(storageName);
        var storage = await CreateStorageAsync();
        var grainId = GrainId.Create("diagnostics-grain", Guid.NewGuid().ToString("N"));
        var state = new GrainState<TestState> { State = new TestState { Value = 1 } };

        await storage.WriteStateAsync(StateName, grainId, state);
        await storage.ReadStateAsync(StateName, grainId, new GrainState<TestState>());
        await Assert.ThrowsAsync<InconsistentStateException>(() => storage.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState() }));
        await storage.ClearStateAsync(StateName, grainId, state);

        Assert.Equal(
            ["clear/success", "read/success", "write/conflict", "write/success"],
            listener.Durations.Select(d => $"{d.Operation}/{d.Outcome}").Order());
        Assert.All(listener.Durations, d => Assert.True(d.Seconds >= 0));
        Assert.Equal(2, listener.PayloadSizes.Count);
        Assert.All(listener.PayloadSizes, size => Assert.True(size > 0));
    }

    [Fact]
    public async Task Operations_AreTraced()
    {
        var activities = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Scarlet.Orleans",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activities.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);
        var storage = await CreateStorageAsync();
        var grainId = GrainId.Create("diagnostics-grain", Guid.NewGuid().ToString("N"));

        await storage.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState { Value = 1 } });

        var write = Assert.Single(activities, a => (string?)a.GetTagItem("orleans.storage.name") == storageName && a.DisplayName == "persistence write");
        Assert.Equal("success", write.GetTagItem("outcome"));
        Assert.Equal(grainId.ToString(), write.GetTagItem("orleans.grain.id"));
        Assert.Equal(ActivityStatusCode.Unset, write.Status);
    }

    [Fact]
    public async Task OversizedPayload_IsMeasuredAsItsOwnOutcome()
    {
        using var listener = new Measurements(storageName);
        var storage = await CreateStorageAsync(maxPayloadSize: 16);
        var grainId = GrainId.Create("diagnostics-grain", Guid.NewGuid().ToString("N"));

        await Assert.ThrowsAsync<GrainStatePayloadTooLargeException>(
            () => storage.WriteStateAsync(StateName, grainId, new GrainState<TestState> { State = new TestState { Text = new string('x', 100) } }));

        Assert.Equal(["write/payload_too_large"], listener.Durations.Select(d => $"{d.Operation}/{d.Outcome}"));
    }

    private sealed class Measurements : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly string storageName;

        public Measurements(string storageName)
        {
            this.storageName = storageName;
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == "Scarlet.Orleans")
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            {
                if (instrument.Name == "scarlet.orleans.persistence.duration" && Tag(tags, "orleans.storage.name") == this.storageName)
                {
                    Durations.Enqueue((Tag(tags, "orleans.storage.operation")!, Tag(tags, "outcome")!, value));
                }
            });
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                if (instrument.Name == "scarlet.orleans.persistence.payload.size" && Tag(tags, "orleans.storage.name") == this.storageName)
                {
                    PayloadSizes.Enqueue(value);
                }
            });
            listener.Start();
        }

        public ConcurrentQueue<(string Operation, string Outcome, double Seconds)> Durations { get; } = new();

        public ConcurrentQueue<long> PayloadSizes { get; } = new();

        public void Dispose() => listener.Dispose();

        private static string? Tag(ReadOnlySpan<KeyValuePair<string, object?>> tags, string key)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == key)
                {
                    return tag.Value as string;
                }
            }

            return null;
        }
    }
}
