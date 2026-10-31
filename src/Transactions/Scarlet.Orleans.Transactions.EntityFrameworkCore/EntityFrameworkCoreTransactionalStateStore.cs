using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.EntityFrameworkCore;

namespace Scarlet.Orleans.Transactions.EntityFrameworkCore;

/// <summary>
/// Transactional state operations through EF Core APIs, with no provider special-cased, each in its own transaction.
/// </summary>
/// <remarks>
/// <para>
/// Every operation uses its own short-lived context, because Orleans calls storage concurrently and a
/// <see cref="DbContext"/> is not thread-safe. Conditional updates and deletes are <c>ExecuteUpdate</c> and
/// <c>ExecuteDelete</c>, so the change tracker never turns them into a read followed by a write.
/// </para>
/// <para>
/// Stores run in a <see cref="SingleAttemptExecutionStrategy"/>, so a retrying execution strategy configured on the
/// context never repeats them, and are repeated only when the server reported that the attempt was not applied
/// (<see cref="TransientRetry"/>), as the <see cref="ITransactionalStateStore"/> contract requires. Loads keep the
/// context's strategy, since repeating them is safe.
/// </para>
/// </remarks>
/// <param name="contextFactory">Creates the short-lived context of each operation.</param>
/// <param name="schemaMode">What to check about the schema at startup.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
/// <param name="isDuplicateKeyError">The application's rule for recognising duplicate keys (<see cref="ScarletEntityFrameworkCoreTransactionalStateStorageOptions.IsDuplicateKeyError"/>).</param>
internal sealed class EntityFrameworkCoreTransactionalStateStore<TContext>(
    IDbContextFactory<TContext> contextFactory,
    SchemaMode schemaMode,
    TimeProvider timeProvider,
    ILogger logger,
    Func<DbException, bool>? isDuplicateKeyError = null)
    : ITransactionalStateStore
    where TContext : DbContext
{
    /// <summary>
    /// The most sequence numbers one delete matches, each a parameter, well under SQL Server's 2,100.
    /// </summary>
    private const int SequencesPerDelete = 500;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var context = await contextFactory.CreateDbContextAsync(cancellationToken).ConfigureAwait(false);
        if (context.Model.FindEntityType(typeof(TransactionalStateEntryRecord)) is null)
        {
            throw new InvalidOperationException(
                $"{typeof(TContext).Name} does not map the Orleans transactions tables. Call modelBuilder.{nameof(ScarletTransactionsModelBuilderExtensions.ApplyOrleansTransactionsModel)}() in OnModelCreating.");
        }

        if (schemaMode == SchemaMode.Validate)
        {
            await SchemaValidation.ValidateAsync(
                context,
                TransactionsSchema.Component,
                TransactionsSchema.MinimumVersion,
                TransactionsSchema.LatestVersion,
                nameof(ScarletTransactionsModelBuilderExtensions.ApplyOrleansTransactionsSchema),
                cancellationToken).ConfigureAwait(false);

            // Only a warning depends on the provider; the schema is the application's to change.
            if (context.Database.ProviderName == "Microsoft.EntityFrameworkCore.Sqlite")
            {
                await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
                await SqliteJournal.EnsureWriteAheadLogAsync(context.Database.GetDbConnection(), enable: false, logger, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public Task<StoredTransactionalState?> LoadAsync(TransactionalStateKey key, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.RepeatableRead, write: false, async (context, ct) =>
        {
            var keyHash = key.Hash;
            var state = await context.Set<TransactionalStateRecord>()
                .AsNoTracking()
                .Where(s => s.KeyHash == keyHash)
                .Select(s => new { s.StateIdentity, s.ETag, s.CommittedSequenceId, s.Metadata })
                .SingleOrDefaultAsync(ct)
                .ConfigureAwait(false);
            if (state is null)
            {
                return (default(StoredTransactionalState), true);
            }

            var committed = state.CommittedSequenceId;
            var entries = await context.Set<TransactionalStateEntryRecord>()
                .AsNoTracking()
                .Where(e => e.KeyHash == keyHash && e.SequenceId >= committed)
                .OrderBy(e => e.SequenceId)
                .Select(e => new StoredEntry(e.SequenceId, e.TransactionId, e.TimestampTicks, e.TransactionManager, e.Payload))
                .ToListAsync(ct)
                .ConfigureAwait(false);
            return (new StoredTransactionalState(state.StateIdentity, state.ETag, committed, state.Metadata, entries), true);
        }, cancellationToken);

    public async Task<StoreResult> StoreAsync(TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken)
    {
        DbUpdateException? insertFailure = null;
        var result = await InTransactionAsync(IsolationLevel.ReadCommitted, write: true, async (context, ct) =>
        {
            insertFailure = null;
            try
            {
                return await StoreCoreAsync(context, key, request, ct).ConfigureAwait(false);
            }
            catch (DbUpdateException exception) when (request.ExpectedETag is null
                && context.ChangeTracker.Entries<TransactionalStateRecord>().Any()
                && !(exception.InnerException is DbException error && DbErrors.IsTransient(error)))
            {
                // The insert of the state row failed; the transaction rolls back, and the row under the key decides.
                insertFailure = exception;
                return (new StoreResult(StoreStatus.Conflict), false);
            }
        }, cancellationToken).ConfigureAwait(false);
        if (result.Status != StoreStatus.Conflict)
        {
            return result;
        }

        var existing = await ReadIdentityAsync(key, cancellationToken).ConfigureAwait(false);
        if (insertFailure is not null && (existing is null || InsertErrors.IsDuplicateKey(insertFailure.InnerException ?? insertFailure, isDuplicateKeyError) == false))
        {
            ExceptionDispatchInfo.Throw(insertFailure);
        }

        return result with { StoredIdentity = existing };
    }

    /// <summary>
    /// The steps of a store, in the order of design section 5.5: the state row, which every writer takes first; aborted
    /// entries; prepared entries; the commit, checked against its entry; and the entries the commit made obsolete.
    /// </summary>
    private static async Task<(StoreResult Result, bool Commit)> StoreCoreAsync(TContext context, TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken)
    {
        if (!await WriteStateAsync(context, key, request, cancellationToken).ConfigureAwait(false))
        {
            return (new StoreResult(StoreStatus.Conflict), false);
        }

        var keyHash = key.Hash;
        var entries = context.Set<TransactionalStateEntryRecord>();
        var deleted = 0;
        if (request.AbortAfter is { } abortAfter)
        {
            deleted += await entries.Where(e => e.KeyHash == keyHash && e.SequenceId > abortAfter).ExecuteDeleteAsync(cancellationToken).ConfigureAwait(false);
        }

        if (request.Prepared.Count > 0)
        {
            foreach (var chunk in request.Prepared.Chunk(SequencesPerDelete))
            {
                await entries.Where(AnySequence(keyHash, chunk)).ExecuteDeleteAsync(cancellationToken).ConfigureAwait(false);
            }

            entries.AddRange(request.Prepared.Select(e => new TransactionalStateEntryRecord
            {
                KeyHash = keyHash,
                SequenceId = e.SequenceId,
                TransactionId = e.TransactionId,
                TimestampTicks = e.TimestampTicks,
                TransactionManager = e.TransactionManager,
                Payload = e.Payload,
            }));
            await context.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            context.ChangeTracker.Clear();
        }

        if (request.CommitUpTo is { } commitUpTo)
        {
            var committed = await context.Set<TransactionalStateRecord>()
                .Where(s => s.KeyHash == keyHash)
                .Select(s => s.CommittedSequenceId)
                .SingleAsync(cancellationToken)
                .ConfigureAwait(false);
            if (committed == commitUpTo
                && !await entries.AnyAsync(e => e.KeyHash == keyHash && e.SequenceId == commitUpTo, cancellationToken).ConfigureAwait(false))
            {
                return (new StoreResult(StoreStatus.MissingCommitRecord), false);
            }

            deleted += await entries.Where(e => e.KeyHash == keyHash && e.SequenceId < committed).ExecuteDeleteAsync(cancellationToken).ConfigureAwait(false);
        }

        return (new StoreResult(StoreStatus.Stored, DeletedEntries: deleted), true);
    }

    /// <summary>
    /// Replaces or inserts the state row. Returns <see langword="false"/> if no row with the expected identity and ETag
    /// exists. A failed insert throws, for <see cref="StoreAsync"/> to settle.
    /// </summary>
    private static async Task<bool> WriteStateAsync(TContext context, TransactionalStateKey key, StoreRequest request, CancellationToken cancellationToken)
    {
        if (request.ExpectedETag is not { } expected)
        {
            context.Set<TransactionalStateRecord>().Add(new TransactionalStateRecord
            {
                KeyHash = key.Hash,
                StateIdentity = key.Identity,
                ServiceId = key.ServiceIdText,
                StateName = key.StateNameText,
                GrainId = key.GrainIdText,
                ETag = request.ETag,
                CommittedSequenceId = request.CommitUpTo ?? 0,
                Metadata = request.Metadata,
                ModifiedOn = request.ModifiedOn.UtcDateTime,
            });
            await context.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            context.ChangeTracker.Clear();
            return true;
        }

        var keyHash = key.Hash;
        var identity = key.Identity;
        var commitUpTo = request.CommitUpTo ?? 0;
        var modifiedOn = request.ModifiedOn.UtcDateTime;

        // The committed sequence number only moves forward, so a commit at or below the current one changes nothing.
        return await context.Set<TransactionalStateRecord>()
            .Where(s => s.KeyHash == keyHash && s.StateIdentity == identity && s.ETag == expected)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(s => s.ETag, request.ETag)
                    .SetProperty(s => s.Metadata, request.Metadata)
                    .SetProperty(s => s.CommittedSequenceId, s => commitUpTo > s.CommittedSequenceId ? commitUpTo : s.CommittedSequenceId)
                    .SetProperty(s => s.ModifiedOn, modifiedOn),
                cancellationToken)
            .ConfigureAwait(false) == 1;
    }

    private async Task<byte[]?> ReadIdentityAsync(TransactionalStateKey key, CancellationToken cancellationToken) =>
        await TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var keyHash = key.Hash;
            return await context.Set<TransactionalStateRecord>()
                .AsNoTracking()
                .Where(s => s.KeyHash == keyHash)
                .Select(s => s.StateIdentity)
                .FirstOrDefaultAsync(ct)
                .ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// <c>e =&gt; e.KeyHash == keyHash &amp;&amp; (e.SequenceId == ids[0] || e.SequenceId == ids[1] || …)</c>, with each
    /// sequence number a parameter.
    /// </summary>
    /// <remarks>
    /// The comparisons are combined as a balanced tree, so the SQL nests about log2(n) levels deep rather than n, which
    /// engines limit. A primitive-collection <c>Contains</c> would translate differently across providers and versions.
    /// </remarks>
    private static Expression<Func<TransactionalStateEntryRecord, bool>> AnySequence(byte[] keyHash, StoredEntry[] entries)
    {
        var entry = Expression.Parameter(typeof(TransactionalStateEntryRecord), "e");
        var sequenceId = Expression.Property(entry, nameof(TransactionalStateEntryRecord.SequenceId));
        var sameKey = Expression.Equal(Expression.Property(entry, nameof(TransactionalStateEntryRecord.KeyHash)), Captured(keyHash));
        return Expression.Lambda<Func<TransactionalStateEntryRecord, bool>>(Expression.AndAlso(sameKey, Any(0, entries.Length)), entry);

        Expression Any(int start, int count)
        {
            if (count == 1)
            {
                return Expression.Equal(sequenceId, Captured(entries[start].SequenceId));
            }

            var half = count / 2;
            return Expression.OrElse(Any(start, half), Any(start + half, count - half));
        }

        // A captured value, rather than a constant, so EF Core sends it as a parameter.
        static Expression Captured<T>(T value) => Expression.Field(Expression.Constant(new StrongBox<T>(value)), nameof(StrongBox<T>.Value));
    }

    /// <summary>
    /// Runs <paramref name="body"/> in a transaction on its own context, and commits it if the body says so, or rolls it
    /// back. A transient error rolls the transaction back, as the transaction is disposed, before the whole operation is
    /// repeated (design section 4.7).
    /// </summary>
    private Task<T> InTransactionAsync<T>(
        IsolationLevel isolationLevel,
        bool write,
        Func<TContext, CancellationToken, Task<(T Result, bool Commit)>> body,
        CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var context = await contextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
            var strategy = write ? new SingleAttemptExecutionStrategy(context) : context.Database.CreateExecutionStrategy();
            return await strategy.ExecuteAsync(
                async c =>
                {
                    await using var transaction = await context.Database.BeginTransactionAsync(isolationLevel, c).ConfigureAwait(false);
                    var (result, commit) = await body(context, c).ConfigureAwait(false);
                    if (commit)
                    {
                        await transaction.CommitAsync(c).ConfigureAwait(false);
                    }
                    else
                    {
                        await transaction.RollbackAsync(c).ConfigureAwait(false);
                    }

                    return result;
                },
                ct).ConfigureAwait(false);
        }, cancellationToken);
}
