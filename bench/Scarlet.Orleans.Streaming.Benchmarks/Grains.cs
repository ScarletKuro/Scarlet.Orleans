using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;

namespace Scarlet.Orleans.Streaming.Benchmarks;

public static class Providers
{
    public const string Scarlet = "scarlet";

    public const string InBox = "inbox";

    /// <summary>The stream namespace whose implicit consumer reads <paramref name="provider"/>.</summary>
    public static string Namespace(string provider) => provider + "-bench";
}

/// <param name="Phase">The measurement phase that sent the event.</param>
/// <param name="Id">The event's number, unique within its phase.</param>
/// <param name="SentAt">When it was sent, as a <see cref="Stopwatch"/> timestamp; the cluster runs in this process.</param>
[GenerateSerializer]
public readonly record struct BenchEvent([property: Id(0)] int Phase, [property: Id(1)] long Id, [property: Id(2)] long SentAt);

/// <summary>
/// The deliveries of the current phase: each event once, with how long it took from send to consumer, and every
/// further delivery of it as a duplicate. Events of an earlier phase that arrive late are ignored.
/// </summary>
public static class Deliveries
{
    private static readonly Lock Gate = new();
    private static int phase;
    private static Dictionary<long, double> latencies = [];
    private static int duplicates;
    private static readonly Dictionary<long, (long Due, long Started)> Starts = [];
    private static readonly Dictionary<long, long> Acknowledged = [];
    private static readonly Dictionary<long, long> Received = [];

    public static int Unique
    {
        get
        {
            lock (Gate)
            {
                return latencies.Count;
            }
        }
    }

    public static int Duplicates
    {
        get
        {
            lock (Gate)
            {
                return duplicates;
            }
        }
    }

    /// <summary>Starts a phase, forgetting the last one's deliveries, and returns its number.</summary>
    public static int Begin()
    {
        lock (Gate)
        {
            latencies = [];
            duplicates = 0;
            Starts.Clear();
            Acknowledged.Clear();
            Received.Clear();
            return ++phase;
        }
    }

    public static void Record(BenchEvent e)
    {
        var latency = Stopwatch.GetElapsedTime(e.SentAt).TotalMilliseconds;
        lock (Gate)
        {
            if (e.Phase != phase)
            {
                return;
            }

            if (!latencies.TryAdd(e.Id, latency))
            {
                duplicates++;
            }
            else if (BenchmarkDiagnostics.Enabled) Received[e.Id] = Stopwatch.GetTimestamp();
        }
    }

    public static void SendStarted(long id, long due)
    {
        if (!BenchmarkDiagnostics.Enabled) return;
        lock (Gate) Starts[id] = (due, Stopwatch.GetTimestamp());
    }

    public static void SendAcknowledged(long id)
    {
        if (!BenchmarkDiagnostics.Enabled) return;
        lock (Gate) Acknowledged[id] = Stopwatch.GetTimestamp();
    }

    public static void PrintSendTimings()
    {
        if (!BenchmarkDiagnostics.Enabled) return;
        lock (Gate)
        {
            Print("due_to_send", Starts.Values.Select(x => Stopwatch.GetElapsedTime(x.Due, x.Started).TotalMilliseconds));
            Print("send_to_ack", Starts.Where(x => Acknowledged.ContainsKey(x.Key)).Select(x => Stopwatch.GetElapsedTime(x.Value.Started, Acknowledged[x.Key]).TotalMilliseconds));
            Print("send_to_delivery", Starts.Where(x => Received.ContainsKey(x.Key)).Select(x => Stopwatch.GetElapsedTime(x.Value.Started, Received[x.Key]).TotalMilliseconds));
        }
    }

    private static void Print(string name, IEnumerable<double> samples)
    {
        var sorted = samples.Order().ToArray();
        if (sorted.Length > 0)
        {
            Console.WriteLine(FormattableString.Invariant($"DIAGNOSTICS {name} n={sorted.Length} p50_ms={sorted[sorted.Length / 2]:0.00} p99_ms={sorted[Math.Min(sorted.Length - 1, (int)(sorted.Length * 0.99))]:0.00} max_ms={sorted[^1]:0.00}"));
        }
    }

    public static double[] Latencies()
    {
        lock (Gate)
        {
            return [.. latencies.Values.Order()];
        }
    }
}

public interface IConsumerGrain : IGrainWithGuidKey
{
    Task Ping();
}

public abstract class ConsumerGrain(string provider) : Grain, IConsumerGrain
{
    public override Task OnActivateAsync(CancellationToken cancellationToken) =>
        this.GetStreamProvider(provider).GetStream<BenchEvent>(StreamId.Create(Providers.Namespace(provider), this.GetPrimaryKey()))
            .SubscribeAsync((e, _) =>
            {
                Deliveries.Record(e);
                return Task.CompletedTask;
            });

    public Task Ping() => Task.CompletedTask;
}

[ImplicitStreamSubscription(Providers.Scarlet + "-bench")]
public sealed class ScarletConsumerGrain() : ConsumerGrain(Providers.Scarlet);

[ImplicitStreamSubscription(Providers.InBox + "-bench")]
public sealed class InBoxConsumerGrain() : ConsumerGrain(Providers.InBox);
