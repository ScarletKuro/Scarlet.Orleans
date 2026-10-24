using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// The metrics and traces of every package, under one name for OpenTelemetry to subscribe to: <c>Scarlet.Orleans</c>.
/// </summary>
/// <remarks>
/// Instruments are named <c>scarlet.orleans.{component}.{measurement}</c> and tagged with the storage or provider name,
/// so several components and instances share one meter. Each package compiles its own copy of this class; listeners
/// select by name, so they see every copy.
/// </remarks>
internal static class Diagnostics
{
    public const string Name = "Scarlet.Orleans";

    public static readonly Meter Meter = new(Name);

    public static readonly ActivitySource ActivitySource = new(Name);

    private static readonly Counter<long> Retries = Meter.CreateCounter<long>(
        "scarlet.orleans.retries",
        unit: "{retry}",
        description: "Statements repeated after the server reported that an attempt was not applied, such as a deadlock.");

    public static void RecordRetry(string error)
    {
        Retries.Add(1, new KeyValuePair<string, object?>("error", error));
        Activity.Current?.AddEvent(new ActivityEvent("retry", tags: new ActivityTagsCollection { ["error"] = error }));
    }
}
