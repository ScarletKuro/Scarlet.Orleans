using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Reminders;

/// <summary>
/// Implements the <see cref="IReminderTable"/> contract over an <see cref="IReminderStore"/>.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>An upsert replaces the reminder's schedule whatever its ETag, as Orleans requires, and issues a new ETag.</item>
/// <item>A remove deletes the reminder only if the given ETag is current, and otherwise returns <see langword="false"/>.</item>
/// <item>A row whose hash matches but whose identity does not fails the operation with
/// <see cref="ReminderKeyCollisionException"/>: nothing of another reminder is returned or replaced.</item>
/// </list>
/// <para>
/// Start times are stored as UTC ticks. A <see cref="DateTimeKind.Local"/> start time is converted to UTC; an
/// <see cref="DateTimeKind.Unspecified"/> one is taken as UTC, as Orleans creates them in UTC.
/// </para>
/// <para>
/// Every operation runs outside any ambient <see cref="Transaction"/>, so it is committed when the call returns, and is
/// traced and measured under <see cref="Diagnostics.Name"/>, tagged with the operation and its outcome.
/// </para>
/// </remarks>
internal sealed partial class RelationalReminderTable(
    string serviceId,
    IReminderStore store,
    TimeProvider timeProvider,
    ILogger<RelationalReminderTable> logger) : IReminderTable
{
    /// <summary>
    /// How many times an upsert tries again when a concurrent remove or upsert of the same reminder moves the row between
    /// its update and its insert.
    /// </summary>
    private const int UpsertAttempts = 3;

    private static readonly Histogram<double> Duration = Diagnostics.Meter.CreateHistogram<double>(
        "scarlet.orleans.reminders.duration",
        unit: "s",
        description: "The duration of reminder table operations, by operation and outcome.");

    private readonly byte[] service = StorageIdentity.StrictUtf8.GetBytes(serviceId);

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        using var scope = SuppressAmbientTransaction();
        await store.InitializeAsync(cancellationToken).ConfigureAwait(false);
        LogInitialized(serviceId);
    }

    public Task<ReminderTableData> ReadRows(GrainId grainId) => ReadRows(grainId, CancellationToken.None);

    public Task<ReminderTableData> ReadRows(GrainId grainId, CancellationToken cancellationToken) =>
        ObserveAsync("read_grain", async () =>
        {
            using var scope = SuppressAmbientTransaction();
            var rows = await store.ReadGrainAsync(ReminderKey.ComputeGrainKeyHash(service, grainId), cancellationToken).ConfigureAwait(false);
            var entries = new List<ReminderEntry>(rows.Count);
            foreach (var row in rows)
            {
                var entry = Decode(row);
                if (!entry.GrainId.Equals(grainId))
                {
                    throw Collision($"The reminders of grain {grainId} share their storage key with grain {entry.GrainId}");
                }

                entries.Add(entry);
            }

            return new ReminderTableData(entries);
        });

    public Task<ReminderTableData> ReadRows(uint begin, uint end) => ReadRows(begin, end, CancellationToken.None);

    public Task<ReminderTableData> ReadRows(uint begin, uint end, CancellationToken cancellationToken) =>
        ObserveAsync("read_range", async () =>
        {
            using var scope = SuppressAmbientTransaction();
            var rows = await store.ReadRangeAsync(ReminderKey.ComputeServiceHash(service), begin, end, cancellationToken).ConfigureAwait(false);
            var entries = new List<ReminderEntry>(rows.Count);
            foreach (var row in rows)
            {
                entries.Add(Decode(row));
            }

            return new ReminderTableData(entries);
        });

    public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName) => ReadRow(grainId, reminderName, CancellationToken.None);

    public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName, CancellationToken cancellationToken) =>
        ObserveAsync("read", async () =>
        {
            using var scope = SuppressAmbientTransaction();
            var key = ReminderKey.Create(serviceId, grainId, reminderName);
            return await ReadVerifiedAsync(key, cancellationToken).ConfigureAwait(false) is { } stored ? ToEntry(key.GrainId, key.ReminderName, stored) : null;
        });

    public Task<string?> UpsertRow(ReminderEntry entry) => UpsertRow(entry, CancellationToken.None);

    public Task<string?> UpsertRow(ReminderEntry entry, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return ObserveAsync<string?>("upsert", async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var scope = SuppressAmbientTransaction();
            var key = ReminderKey.Create(serviceId, entry.GrainId, entry.ReminderName);
            var startAt = ToUtc(entry.StartAt).Ticks;
            var period = entry.Period.Ticks;
            var etag = ETags.New(timeProvider);

            // Update first: most upserts replace an existing reminder. A concurrent insert or remove of the same reminder
            // can move the row between the two statements, so they are tried again; the last write wins either way.
            for (var attempt = 0; attempt < UpsertAttempts; attempt++)
            {
                if (await SettleCancellationAsync(store.TryUpdateAsync(key, startAt, period, etag, cancellationToken), key, etag).ConfigureAwait(false)
                    || await SettleCancellationAsync(store.TryInsertAsync(key, startAt, period, etag, cancellationToken), key, etag).ConfigureAwait(false))
                {
                    return ETags.Format(etag);
                }

                // The update matched no row and the insert found one: either another reminder holds the key, or the row
                // was removed and inserted again in between.
                await ReadVerifiedAsync(key, cancellationToken).ConfigureAwait(false);
            }

            throw new InvalidOperationException(
                $"The upsert of reminder {key} raced with concurrent changes to the same reminder {UpsertAttempts} times. Nothing was written by this call.");
        });
    }

    public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag) => RemoveRow(grainId, reminderName, eTag, CancellationToken.None);

    public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag, CancellationToken cancellationToken) =>
        ObserveAsync("remove", async () =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var scope = SuppressAmbientTransaction();

            // An ETag this provider never issued cannot be current.
            return ETags.TryParse(eTag, out var expected)
                && await store.TryDeleteAsync(ReminderKey.Create(serviceId, grainId, reminderName), expected, cancellationToken).ConfigureAwait(false);
        });

    public Task TestOnlyClearTable() => TestOnlyClearTable(CancellationToken.None);

    public Task TestOnlyClearTable(CancellationToken cancellationToken) =>
        ObserveAsync("clear", async () =>
        {
            using var scope = SuppressAmbientTransaction();
            await store.ClearAsync(ReminderKey.ComputeServiceHash(service), cancellationToken).ConfigureAwait(false);
            return true;
        });

    /// <summary>
    /// Awaits a write and, if it is canceled, reports whether it committed anyway.
    /// </summary>
    /// <remarks>
    /// A token that fires while the statement is in flight makes the driver cancel it, but the server may already have
    /// committed. Drivers throw only once the server has answered the cancellation, so a read-back, which must not be
    /// canceled itself, settles it: if the row holds <paramref name="proposedETag"/>, the write committed and is reported,
    /// so the caller learns the reminder's ETag; otherwise the cancellation stands.
    /// </remarks>
    private async Task<bool> SettleCancellationAsync(Task<bool> write, ReminderKey key, byte[] proposedETag)
    {
        try
        {
            return await write.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            var stored = await ReadVerifiedAsync(key, CancellationToken.None).ConfigureAwait(false);
            if (stored is not null && stored.ETag.AsSpan().SequenceEqual(proposedETag))
            {
                return true;
            }

            throw;
        }
    }

    /// <summary>
    /// Reads the row under the key's hash, throwing <see cref="ReminderKeyCollisionException"/> if it belongs to another reminder.
    /// </summary>
    private async Task<StoredReminder?> ReadVerifiedAsync(ReminderKey key, CancellationToken cancellationToken)
    {
        var stored = await store.ReadAsync(key, cancellationToken).ConfigureAwait(false);
        if (stored is not null && !key.Identity.AsSpan().SequenceEqual(stored.Identity))
        {
            throw Collision($"The storage key of reminder {key} is already used by a different reminder");
        }

        return stored;
    }

    /// <summary>
    /// Decodes a row found by a service or grain hash, throwing <see cref="ReminderKeyCollisionException"/> if it belongs
    /// to another service.
    /// </summary>
    private ReminderEntry Decode(StoredReminder row) =>
        ReminderKey.TryDecode(row.Identity, service, out var grainId, out var reminderName)
            ? ToEntry(grainId, reminderName, row)
            : throw Collision($"A reminder stored under the key of service {serviceId} belongs to another service");

    private static ReminderEntry ToEntry(GrainId grainId, string reminderName, StoredReminder stored) => new()
    {
        GrainId = grainId,
        ReminderName = reminderName,
        StartAt = new DateTime(stored.StartAt, DateTimeKind.Utc),
        Period = TimeSpan.FromTicks(stored.Period),
        ETag = ETags.Format(stored.ETag),
    };

    private static DateTime ToUtc(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc),
    };

    private static ReminderKeyCollisionException Collision(string message) =>
        new($"{message}. No reminder was read or written. This is a 128-bit hash collision; please report it.");

    /// <summary>
    /// Runs one table operation in a trace span, and records its duration and outcome.
    /// </summary>
    private async Task<T> ObserveAsync<T>(string operation, Func<Task<T>> body)
    {
        using var activity = Diagnostics.ActivitySource.StartActivity($"reminders {operation}");
        activity?.SetTag("orleans.reminders.operation", operation);

        var started = timeProvider.GetTimestamp();
        var outcome = "error";
        try
        {
            var result = await body().ConfigureAwait(false);
            outcome = "success";
            return result;
        }
        catch (ReminderKeyCollisionException exception)
        {
            outcome = "collision";
            LogCollision(exception, operation);
            throw;
        }
        catch (OperationCanceledException)
        {
            outcome = "canceled";
            throw;
        }
        finally
        {
            Duration.Record(
                timeProvider.GetElapsedTime(started).TotalSeconds,
                new KeyValuePair<string, object?>("orleans.reminders.operation", operation),
                new KeyValuePair<string, object?>("outcome", outcome));
            activity?.SetTag("outcome", outcome);
            if (outcome is not ("success" or "canceled"))
            {
                activity?.SetStatus(ActivityStatusCode.Error, outcome);
            }
        }
    }

    private static TransactionScope SuppressAmbientTransaction() =>
        new(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);

    [LoggerMessage(Level = LogLevel.Information, Message = "Initialized the reminder table for service {ServiceId}.")]
    private partial void LogInitialized(string serviceId);

    [LoggerMessage(Level = LogLevel.Error, Message = "Reminder table: {Operation} found a reminder held by another identity.")]
    private partial void LogCollision(Exception exception, string operation);
}

/// <summary>
/// Two distinct reminders, grains or services hash to the same storage key.
/// </summary>
internal sealed class ReminderKeyCollisionException(string message) : InvalidOperationException(message);
