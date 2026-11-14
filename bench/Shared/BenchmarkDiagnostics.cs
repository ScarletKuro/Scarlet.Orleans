using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Microsoft.Extensions.Logging;

internal static class BenchmarkDiagnostics
{
    public static bool Enabled { get; } = Environment.GetEnvironmentVariable("SCARLET_BENCH_DIAGNOSTICS") == "1";

    public static IDisposable? Phase(string name) => Enabled ? new MeasurementPhase(name) : null;

    public static string? Argument(string[] args, string name)
    {
        var at = Array.IndexOf(args, name);
        if (at < 0) return null;
        if (at + 1 >= args.Length || args[at + 1].StartsWith("--", StringComparison.Ordinal))
            throw new ArgumentException($"{name} needs a value.");
        return args[at + 1];
    }

    private sealed class MeasurementPhase : IDisposable
    {
        private readonly string name;
        private readonly long started = Stopwatch.GetTimestamp();
        private readonly MeterListener listener = new();
        private readonly ConcurrentDictionary<string, ConcurrentQueue<double>> durations = new();
        private readonly ConcurrentDictionary<string, long> counts = new();

        public MeasurementPhase(string name)
        {
            this.name = name;
            Console.WriteLine($"DIAGNOSTICS BEGIN {name} {DateTimeOffset.UtcNow:O} threads={ThreadPool.ThreadCount} pending={ThreadPool.PendingWorkItemCount}");
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "Scarlet.Orleans" &&
                    (instrument.Name.EndsWith("duration", StringComparison.Ordinal) || instrument.Name == "scarlet.orleans.retries" || instrument.Name.StartsWith("scarlet.orleans.streaming.messages", StringComparison.Ordinal)))
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
                durations.GetOrAdd(Key(instrument, tags), static _ => new()).Enqueue(value * 1000));
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
                counts.AddOrUpdate(Key(instrument, tags), value, (_, total) => total + value));
            listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) =>
                counts.AddOrUpdate(Key(instrument, tags), value, (_, total) => total + value));
            listener.Start();
        }

        public void Dispose()
        {
            listener.RecordObservableInstruments();
            listener.Dispose();
            Console.WriteLine($"DIAGNOSTICS END {name} elapsed={Stopwatch.GetElapsedTime(started)} threads={ThreadPool.ThreadCount} pending={ThreadPool.PendingWorkItemCount}");
            foreach (var (key, values) in durations.OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                var sorted = values.Order().ToArray();
                if (sorted.Length > 0)
                {
                    Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                        $"DIAGNOSTICS {key} n={sorted.Length} p50_ms={sorted[sorted.Length / 2]:0.00} p99_ms={sorted[Math.Min(sorted.Length - 1, (int)(sorted.Length * 0.99))]:0.00} max_ms={sorted[^1]:0.00}"));
                }
            }

            foreach (var (key, value) in counts.OrderBy(p => p.Key, StringComparer.Ordinal))
            {
                Console.WriteLine($"DIAGNOSTICS {key} count={value}");
            }
        }

        private static string Key(Instrument instrument, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var parts = new List<string> { instrument.Name };
            foreach (var tag in tags)
            {
                parts.Add($"{tag.Key}={tag.Value}");
            }

            return string.Join(' ', parts);
        }
    }
}

internal sealed class BenchmarkWarningLog : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new Logger(categoryName);

    public void Dispose() { }

    private sealed class Logger(string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => BenchmarkDiagnostics.Enabled && logLevel >= LogLevel.Warning &&
            (category.StartsWith("Scarlet.", StringComparison.Ordinal) || category.StartsWith("Orleans.Transactions", StringComparison.Ordinal) || category.StartsWith("Orleans.Stream", StringComparison.Ordinal) || category.StartsWith("Orleans.Providers.Streams", StringComparison.Ordinal));

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
            {
                Console.Error.WriteLine($"{DateTimeOffset.UtcNow:O} {category} {logLevel} {eventId}: {formatter(state, exception)} {exception}");
            }
        }
    }
}
