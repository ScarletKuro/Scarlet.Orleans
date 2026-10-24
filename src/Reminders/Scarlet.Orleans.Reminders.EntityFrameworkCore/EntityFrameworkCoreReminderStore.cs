using System.Data.Common;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.EntityFrameworkCore;

namespace Scarlet.Orleans.Reminders.EntityFrameworkCore;

/// <summary>
/// Reminder operations through EF Core APIs, with no provider special-cased.
/// </summary>
/// <remarks>
/// <para>
/// Every operation uses its own short-lived context, because Orleans calls the reminder table concurrently and a
/// <see cref="DbContext"/> is not thread-safe. Each write is one statement: an insert through <c>SaveChanges</c>, and
/// updates and deletes through <c>ExecuteUpdate</c> and <c>ExecuteDelete</c>.
/// </para>
/// <para>
/// Writes run in a <see cref="SingleAttemptExecutionStrategy"/>, so a retrying execution strategy configured on the
/// context never repeats them, and are repeated only when the server reported that the attempt was not applied
/// (<see cref="TransientRetry"/>), as the <see cref="IReminderStore"/> contract requires. Reads keep the context's
/// strategy, since repeating them is safe.
/// </para>
/// </remarks>
/// <param name="contextFactory">Creates the short-lived context of each operation.</param>
/// <param name="schemaMode">What to check about the schema at startup.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
/// <param name="isDuplicateKeyError">The application's rule for recognising duplicate keys (<see cref="ScarletEntityFrameworkCoreReminderTableOptions.IsDuplicateKeyError"/>).</param>
internal sealed class EntityFrameworkCoreReminderStore<TContext>(
    IDbContextFactory<TContext> contextFactory,
    SchemaMode schemaMode,
    TimeProvider timeProvider,
    ILogger logger,
    Func<DbException, bool>? isDuplicateKeyError = null)
    : IReminderStore
    where TContext : DbContext
{
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        if (context.Model.FindEntityType(typeof(ReminderRecord)) is null)
        {
            throw new InvalidOperationException(
                $"{typeof(TContext).Name} does not map the Orleans reminder table. Call modelBuilder.{nameof(ScarletRemindersModelBuilderExtensions.ApplyOrleansRemindersModel)}() in OnModelCreating.");
        }

        if (schemaMode == SchemaMode.Validate)
        {
            await SchemaValidation.ValidateAsync(
                context,
                RemindersSchema.Component,
                RemindersSchema.MinimumVersion,
                RemindersSchema.LatestVersion,
                nameof(ScarletRemindersModelBuilderExtensions.ApplyOrleansRemindersSchema),
                cancellationToken).ConfigureAwait(false);

            // Only a warning depends on the provider; the schema is the application's to change.
            if (context.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
            {
                await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await SqliteJournal.EnsureWriteAheadLogAsync(context.Database.GetDbConnection(), enable: false, logger, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task<StoredReminder?> ReadAsync(ReminderKey key, CancellationToken cancellationToken)
    {
        var hash = key.Hash;
        var rows = await ReadRowsAsync(r => r.KeyHash == hash, cancellationToken).ConfigureAwait(false);
        return rows.Count == 0 ? null : rows[0];
    }

    public Task<IReadOnlyList<StoredReminder>> ReadGrainAsync(byte[] grainKeyHash, CancellationToken cancellationToken) =>
        ReadRowsAsync(r => r.GrainKeyHash == grainKeyHash, cancellationToken);

    public Task<IReadOnlyList<StoredReminder>> ReadRangeAsync(byte[] serviceHash, uint begin, uint end, CancellationToken cancellationToken)
    {
        long low = begin;
        long high = end;
        return begin < end
            ? ReadRowsAsync(r => r.ServiceHash == serviceHash && r.GrainHash > low && r.GrainHash <= high, cancellationToken)
            : ReadRowsAsync(r => r.ServiceHash == serviceHash && (r.GrainHash > low || r.GrainHash <= high), cancellationToken);
    }

    public Task<bool> TryInsertAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, ct => InsertAsync(key, startAt, period, etag, ct), cancellationToken);

    public Task<bool> TryUpdateAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var hash = key.Hash;
            var identity = key.Identity;
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(
                    c => context.Set<ReminderRecord>()
                        .Where(r => r.KeyHash == hash && r.ReminderIdentity == identity)
                        .ExecuteUpdateAsync(
                            setters => setters
                                .SetProperty(r => r.StartAtTicks, startAt)
                                .SetProperty(r => r.PeriodTicks, period)
                                .SetProperty(r => r.ETag, etag),
                            c),
                    ct)
                .ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task<bool> TryDeleteAsync(ReminderKey key, byte[] expectedETag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var hash = key.Hash;
            var identity = key.Identity;
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(
                    c => context.Set<ReminderRecord>()
                        .Where(r => r.KeyHash == hash && r.ReminderIdentity == identity && r.ETag == expectedETag)
                        .ExecuteDeleteAsync(c),
                    ct)
                .ConfigureAwait(false) == 1;
        }, cancellationToken);

    public Task ClearAsync(byte[] serviceHash, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            return await new SingleAttemptExecutionStrategy(context)
                .ExecuteAsync(c => context.Set<ReminderRecord>().Where(r => r.ServiceHash == serviceHash).ExecuteDeleteAsync(c), ct)
                .ConfigureAwait(false);
        }, cancellationToken);

    private async Task<IReadOnlyList<StoredReminder>> ReadRowsAsync(System.Linq.Expressions.Expression<Func<ReminderRecord, bool>> predicate, CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        return await context.Set<ReminderRecord>()
            .AsNoTracking()
            .Where(predicate)
            .Select(r => new StoredReminder(r.ReminderIdentity, r.StartAtTicks, r.PeriodTicks, r.ETag))
            .ToListAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task<bool> InsertAsync(ReminderKey key, long startAt, long period, byte[] etag, CancellationToken cancellationToken)
    {
        DbUpdateException failure;
        await using (var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false))
        {
            context.Set<ReminderRecord>().Add(new ReminderRecord
            {
                KeyHash = key.Hash,
                ReminderIdentity = key.Identity,
                ServiceHash = key.ServiceHash,
                GrainKeyHash = key.GrainKeyHash,
                GrainHash = key.GrainHash,
                ServiceId = key.ServiceIdText,
                GrainId = key.GrainIdText,
                ReminderName = key.ReminderNameText,
                StartAtTicks = startAt,
                PeriodTicks = period,
                ETag = etag,
            });

            try
            {
                await new SingleAttemptExecutionStrategy(context).ExecuteAsync(c => context.SaveChangesAsync(c), cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (DbUpdateException exception)
            {
                failure = exception;
            }
        }

        var cause = failure.InnerException ?? failure;
        switch (InsertErrors.IsDuplicateKey(cause, isDuplicateKeyError))
        {
            case true:
                return false;
            case false:
                ExceptionDispatchInfo.Throw(failure);
                break;
        }

        // A cause that cannot be recognised. An insert expects no row, so a row under the key now means it could not
        // have succeeded, and the upsert tries its update again. With no row, the original failure surfaces.
        if (await ReadAsync(key, cancellationToken).ConfigureAwait(false) is not null)
        {
            return false;
        }

        ExceptionDispatchInfo.Throw(failure);
        return false;
    }
}
