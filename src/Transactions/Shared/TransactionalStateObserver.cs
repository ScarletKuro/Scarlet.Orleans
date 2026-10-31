using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Storage;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions;

/// <summary>
/// Traces, measures and logs the operations of one transactional state storage, shared by all the states it creates.
/// </summary>
/// <remarks>
/// Conflicts are logged at debug level, because concurrent activations make them part of normal operation; collisions at
/// error level, because they should never happen. Other failures propagate to Orleans, which logs them.
/// </remarks>
internal sealed partial class TransactionalStateObserver(string name, TimeProvider timeProvider, ILogger<TransactionalStateObserver> logger)
{
    private static readonly Histogram<double> Duration = Diagnostics.Meter.CreateHistogram<double>(
        "scarlet.orleans.transactions.duration",
        unit: "s",
        description: "The duration of transactional state storage operations, by storage, operation and outcome.");

    private static readonly Histogram<long> Entries = Diagnostics.Meter.CreateHistogram<long>(
        "scarlet.orleans.transactions.entries",
        unit: "{entry}",
        description: "The number of prepared state entries written and deleted per store, by storage.");

    public string Name => name;

    public void RecordEntries(int written, int deleted)
    {
        Entries.Record(written, new KeyValuePair<string, object?>("orleans.storage.name", name), new KeyValuePair<string, object?>("change", "written"));
        Entries.Record(deleted, new KeyValuePair<string, object?>("orleans.storage.name", name), new KeyValuePair<string, object?>("change", "deleted"));
    }

    /// <summary>
    /// Runs one storage operation in a trace span, and records its duration and outcome.
    /// </summary>
    public async Task<T> ObserveAsync<T>(string operation, TransactionalStateKey key, Func<Task<T>> body)
    {
        using var activity = Diagnostics.ActivitySource.StartActivity($"transactions {operation}");
        activity?.SetTag("orleans.storage.name", name);
        activity?.SetTag("orleans.transactions.operation", operation);
        activity?.SetTag("orleans.grain.id", key.GrainId.ToString());
        activity?.SetTag("orleans.storage.state_name", key.StateName);

        var started = timeProvider.GetTimestamp();
        var outcome = "error";
        try
        {
            var result = await body().ConfigureAwait(false);
            outcome = "success";
            return result;
        }
        catch (InconsistentStateException exception)
        {
            outcome = "conflict";
            LogConflict(exception, name, operation, key.GrainId, key.StateName);
            throw;
        }
        catch (TransactionalStateKeyCollisionException exception)
        {
            outcome = "collision";
            LogCollision(exception, name, key.GrainId, key.StateName);
            throw;
        }
        finally
        {
            Duration.Record(
                timeProvider.GetElapsedTime(started).TotalSeconds,
                new KeyValuePair<string, object?>("orleans.storage.name", name),
                new KeyValuePair<string, object?>("orleans.transactions.operation", operation),
                new KeyValuePair<string, object?>("outcome", outcome));
            activity?.SetTag("outcome", outcome);
            if (outcome is not ("success" or "conflict"))
            {
                activity?.SetStatus(ActivityStatusCode.Error, outcome);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Transactional state storage {Name}: {Operation} of {GrainId} state {StateName} conflicted with a newer version.")]
    private partial void LogConflict(Exception exception, string name, string operation, GrainId grainId, string stateName);

    [LoggerMessage(Level = LogLevel.Error, Message = "Transactional state storage {Name}: the storage key of {GrainId} state {StateName} is held by another transactional state.")]
    private partial void LogCollision(Exception exception, string name, GrainId grainId, string stateName);
}
