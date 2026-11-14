using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Scarlet.Orleans.Tests;

/// <summary>
/// Collects the warnings and errors that the providers, and Orleans' streaming and transactions, log in the test
/// clusters, which run in this process, so that a test
/// can show them when it fails.
/// </summary>
public sealed class WarningLog : ILoggerProvider
{
    private static readonly ConcurrentQueue<string> Entries = new();

    public static string Recent => string.Join(Environment.NewLine, Entries.TakeLast(40));

    public ILogger CreateLogger(string categoryName) => new Logger(categoryName);

    public void Dispose()
    {
    }

    private sealed class Logger(string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning && (category.StartsWith("Scarlet.", StringComparison.Ordinal) || category.StartsWith("Orleans.Stream", StringComparison.Ordinal) || category.StartsWith("Orleans.Transactions", StringComparison.Ordinal));

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
            {
                Entries.Enqueue($"{DateTimeOffset.UtcNow:HH:mm:ss.fff} {logLevel} {category}: {formatter(state, exception)} {exception?.GetType().Name} {exception?.Message}");
            }
        }
    }
}
