using System.Data;
using System.Data.Common;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Clustering.AdoNet;

/// <summary>
/// Membership operations as plain SQL (<see cref="ClusteringSql"/>), each in its own transaction.
/// </summary>
/// <param name="dataSource">Opens the connection of each operation.</param>
/// <param name="engineName">The configured engine, or <see langword="null"/> to infer it from the driver.</param>
/// <param name="schemaMode">What to do with the schema in <see cref="InitializeAsync"/>.</param>
/// <param name="timeProvider">Times the delays between retries of transient errors.</param>
/// <param name="logger">Reports schema problems found at startup.</param>
internal sealed class AdoNetMembershipStore(DbDataSource dataSource, string? engineName, SchemaMode schemaMode, TimeProvider timeProvider, ILogger logger) : IMembershipStore
{
    private RelationalEngine engine;

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        engine = engineName is null ? RelationalEngines.FromConnection(connection) : RelationalEngines.Parse(engineName);
        if (engine == RelationalEngine.Sqlite && schemaMode != SchemaMode.None)
        {
            await SqliteJournal.EnsureWriteAheadLogAsync(connection, enable: schemaMode == SchemaMode.Apply, logger, cancellationToken).ConfigureAwait(false);
        }

        if (schemaMode != SchemaMode.None)
        {
            var migrator = new SchemaMigrator(
                ClusteringSchema.Component,
                engine,
                SchemaScript.Load(typeof(AdoNetMembershipStore).Assembly, engine),
                ClusteringSchema.MinimumVersion);
            await migrator.EnsureAsync(connection, apply: schemaMode == SchemaMode.Apply, cancellationToken).ConfigureAwait(false);
        }
    }

    public Task<byte[]?> TryCreateVersionAsync(ClusterScope scope, byte[] etag, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using (var insert = dataSource.CreateCommand(ClusteringSql.InsertVersion(engine)))
            {
                ClusteringSql.AddCluster(insert, scope);
                insert.AddParameter("cluster_identity", scope.Identity, DbType.Binary);
                insert.AddParameter("cluster_id", scope.ClusterIdText, DbType.String);
                insert.AddParameter("etag", etag, DbType.Binary);
                try
                {
                    if (await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1)
                    {
                        return null;
                    }
                }
                catch (DbException exception) when (DbErrors.IsDuplicateKey(exception))
                {
                }
            }

            // A row exists. Version rows are only ever deleted with their cluster, so it is still there to compare.
            await using var select = dataSource.CreateCommand(ClusteringSql.SelectVersion);
            ClusteringSql.AddCluster(select, scope);
            await using var reader = await select.ExecuteReaderAsync(CommandBehavior.SingleRow, ct).ConfigureAwait(false);
            return await reader.ReadAsync(ct).ConfigureAwait(false) ? reader.GetFieldValue<byte[]>(0) : scope.Identity;
        }, cancellationToken);

    public Task<MembershipSnapshot> ReadAsync(ClusterScope scope, byte[]? memberKeyHash, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.RepeatableRead, async (connection, transaction, ct) =>
        {
            StoredVersion? version = null;
            await using (var select = Command(connection, transaction, ClusteringSql.SelectVersion))
            {
                ClusteringSql.AddCluster(select, scope);
                await using var reader = await select.ExecuteReaderAsync(CommandBehavior.SingleRow, ct).ConfigureAwait(false);
                if (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    version = new StoredVersion(reader.GetFieldValue<byte[]>(0), reader.GetInt32(1), reader.GetFieldValue<byte[]>(2));
                }
            }

            // Members exist only with a version row. An absent row is not locked on SQL Server, so members read now could
            // belong to a version created after it.
            var members = new List<StoredMember>();
            if (version is null)
            {
                return (new MembershipSnapshot(null, members), true);
            }

            await using (var select = Command(connection, transaction, memberKeyHash is null ? ClusteringSql.SelectMembers : ClusteringSql.SelectMember))
            {
                ClusteringSql.AddCluster(select, scope);
                if (memberKeyHash is not null)
                {
                    select.AddParameter("key_hash", memberKeyHash, DbType.Binary);
                }

                await using var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    members.Add(ClusteringSql.ReadMember(reader));
                }
            }

            return (new MembershipSnapshot(version, members), true);
        }, cancellationToken);

    public Task<MemberInsertResult> TryInsertAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] etag, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, async (connection, transaction, ct) =>
        {
            if (!await SwapVersionAsync(connection, transaction, scope, version, ct).ConfigureAwait(false))
            {
                return (default(MemberInsertResult), false);
            }

            // The version row is locked, and every writer of this cluster's members locks it first, so no row can
            // appear under the key between this check and the insert.
            await using (var select = Command(connection, transaction, ClusteringSql.SelectMemberIdentity))
            {
                ClusteringSql.AddCluster(select, scope);
                select.AddParameter("key_hash", row.KeyHash, DbType.Binary);
                if (await select.ExecuteScalarAsync(ct).ConfigureAwait(false) is byte[] existing)
                {
                    return (new MemberInsertResult(false, existing), false);
                }
            }

            await using var insert = Command(connection, transaction, ClusteringSql.InsertMember);
            ClusteringSql.AddMemberKey(insert, scope, row.KeyHash, row.Identity);
            ClusteringSql.AddEntry(insert, row);
            insert.AddParameter("silo_address", row.SiloAddress, DbType.String);
            insert.AddParameter("etag", etag, DbType.Binary);
            await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return (new MemberInsertResult(true, null), true);
        }, cancellationToken);

    public Task<bool> TryUpdateAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] expectedETag, byte[] etag, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, async (connection, transaction, ct) =>
        {
            if (!await SwapVersionAsync(connection, transaction, scope, version, ct).ConfigureAwait(false))
            {
                return (false, false);
            }

            await using var update = Command(connection, transaction, ClusteringSql.UpdateMember);
            ClusteringSql.AddMemberKey(update, scope, row.KeyHash, row.Identity);
            ClusteringSql.AddEntry(update, row);
            update.AddParameter("etag", etag, DbType.Binary);
            update.AddParameter("expected_etag", expectedETag, DbType.Binary);
            var updated = await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 1;
            return (updated, updated);
        }, cancellationToken);

    public Task UpdateIAmAliveAsync(ClusterScope scope, byte[] memberKeyHash, byte[] memberIdentity, long iAmAliveTicks, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var update = dataSource.CreateCommand(ClusteringSql.Heartbeat);
            ClusteringSql.AddMemberKey(update, scope, memberKeyHash, memberIdentity);
            update.AddParameter("i_am_alive_ticks", iAmAliveTicks, DbType.Int64);
            return await update.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }, cancellationToken);

    public Task<(int Deleted, byte[]? VersionIdentity)> DeleteDefunctAsync(ClusterScope scope, long beforeTicks, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, async (connection, transaction, ct) =>
        {
            var identity = await LockVersionAsync(connection, transaction, scope, ct).ConfigureAwait(false);
            if (identity is null || !identity.AsSpan().SequenceEqual(scope.Identity))
            {
                return ((0, identity), false);
            }

            await using var delete = Command(connection, transaction, ClusteringSql.DeleteDefunct);
            ClusteringSql.AddCluster(delete, scope);
            delete.AddParameter("before_ticks", beforeTicks, DbType.Int64);
            return ((await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false), identity), true);
        }, cancellationToken);

    /// <remarks>
    /// The version row is deleted first, which locks it as every writer of the cluster does, and its count is reliable on
    /// every driver, unlike that of an update that changes nothing.
    /// </remarks>
    public Task<byte[]?> DeleteClusterAsync(ClusterScope scope, CancellationToken cancellationToken) =>
        InTransactionAsync(IsolationLevel.ReadCommitted, async (connection, transaction, ct) =>
        {
            await using (var delete = Command(connection, transaction, ClusteringSql.DeleteVersion))
            {
                ClusteringSql.AddCluster(delete, scope);
                delete.AddParameter("cluster_identity", scope.Identity, DbType.Binary);
                if (await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false) == 0)
                {
                    return (await ReadVersionIdentityAsync(connection, transaction, scope, ct).ConfigureAwait(false), false);
                }
            }

            await using (var delete = Command(connection, transaction, ClusteringSql.DeleteMembers))
            {
                ClusteringSql.AddCluster(delete, scope);
                await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            return (default(byte[]), true);
        }, cancellationToken);

    public Task<IReadOnlyList<StoredGateway>> ReadGatewaysAsync(ClusterScope scope, CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync<IReadOnlyList<StoredGateway>>(timeProvider, async ct =>
        {
            await using var select = dataSource.CreateCommand(ClusteringSql.SelectGateways);
            ClusteringSql.AddCluster(select, scope);
            await using var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false);
            var gateways = new List<StoredGateway>();
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                gateways.Add(new StoredGateway(reader.GetFieldValue<byte[]>(0), reader.GetString(1), reader.GetInt32(2)));
            }

            return gateways;
        }, cancellationToken);

    private static async Task<bool> SwapVersionAsync(DbConnection connection, DbTransaction transaction, ClusterScope scope, VersionChange version, CancellationToken cancellationToken)
    {
        await using var swap = Command(connection, transaction, ClusteringSql.SwapVersion);
        ClusteringSql.AddVersionChange(swap, scope, version);
        return await swap.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    /// <summary>
    /// Locks the version row until the transaction ends and returns its identity, or returns <see langword="null"/>,
    /// locking nothing, if there is none. A row created after the lock attempt is never reported, as it is not locked.
    /// </summary>
    private async Task<byte[]?> LockVersionAsync(DbConnection connection, DbTransaction transaction, ClusterScope scope, CancellationToken cancellationToken)
    {
        await using (var lockVersion = Command(connection, transaction, ClusteringSql.LockVersion(engine)))
        {
            ClusteringSql.AddCluster(lockVersion, scope);
            if (ClusteringSql.LockVersionSelects(engine))
            {
                return await lockVersion.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as byte[];
            }

            if (await lockVersion.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 0)
            {
                return null;
            }
        }

        return await ReadVersionIdentityAsync(connection, transaction, scope, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<byte[]?> ReadVersionIdentityAsync(DbConnection connection, DbTransaction transaction, ClusterScope scope, CancellationToken cancellationToken)
    {
        await using var select = Command(connection, transaction, ClusteringSql.SelectVersion);
        ClusteringSql.AddCluster(select, scope);
        return await select.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as byte[];
    }

    /// <summary>
    /// Runs <paramref name="body"/> in a transaction on its own connection, and commits it if the body says so, or rolls
    /// it back. A transient error rolls the transaction back, as the transaction is disposed, before the whole operation
    /// is repeated (design section 4.7).
    /// </summary>
    private Task<T> InTransactionAsync<T>(
        IsolationLevel isolationLevel,
        Func<DbConnection, DbTransaction, CancellationToken, Task<(T Result, bool Commit)>> body,
        CancellationToken cancellationToken) =>
        TransientRetry.ExecuteAsync(timeProvider, async ct =>
        {
            await using var connection = await dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(isolationLevel, ct).ConfigureAwait(false);
            var (result, commit) = await body(connection, transaction, ct).ConfigureAwait(false);
            if (commit)
            {
                await transaction.CommitAsync(ct).ConfigureAwait(false);
            }
            else
            {
                await transaction.RollbackAsync(ct).ConfigureAwait(false);
            }

            return result;
        }, cancellationToken);

    private static DbCommand Command(DbConnection connection, DbTransaction transaction, string text)
    {
        var command = connection.CreateCommand();
        command.CommandText = text;
        command.Transaction = transaction;
        return command;
    }
}
