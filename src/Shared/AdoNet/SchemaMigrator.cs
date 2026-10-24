using System.Data;
using System.Data.Common;
using System.Globalization;

namespace Scarlet.Orleans.Relational.Schema;

/// <summary>
/// Validates a component's schema against the ledger, and optionally applies pending scripts.
/// </summary>
/// <remarks>
/// <para>
/// Many silos start at once, so the whole check runs under a database-level lock: a session lock on server engines,
/// and the write transaction itself on SQLite. Silos that wait for the lock then find the ledger current.
/// </para>
/// <para>
/// PostgreSQL, SQL Server and SQLite have transactional DDL, so a script and its ledger row commit together. MySQL
/// and MariaDB commit implicitly on DDL, so their scripts are idempotent and safe to rerun after a crash before the
/// ledger row was written.
/// </para>
/// </remarks>
/// <param name="component">The ledger key of the component, for example <c>persistence</c>.</param>
/// <param name="engine">The engine whose lock, ledger and transaction rules apply.</param>
/// <param name="scripts">The component's scripts for <paramref name="engine"/>, numbered 1..n.</param>
/// <param name="minimumVersion">The oldest schema version the calling code can run against without migrating.</param>
internal sealed class SchemaMigrator(string component, RelationalEngine engine, IReadOnlyList<SchemaScript> scripts, int minimumVersion)
{
    private static readonly TimeSpan LockTimeout = TimeSpan.FromMinutes(2);

    public int LatestVersion => scripts[^1].Version;

    /// <param name="connection">An open connection, used for the whole check so that the session lock covers it.</param>
    /// <param name="apply">Whether to apply pending scripts, rather than fail when the schema is below the minimum version.</param>
    /// <param name="cancellationToken">Cancels the check.</param>
    public async Task EnsureAsync(DbConnection connection, bool apply, CancellationToken cancellationToken)
    {
        if (engine == RelationalEngine.Sqlite)
        {
            // Microsoft.Data.Sqlite starts non-deferred transactions with BEGIN IMMEDIATE, which takes the write lock up front.
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await RunAsync(connection, transaction, apply, cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await AcquireLockAsync(connection, cancellationToken).ConfigureAwait(false);
        try
        {
            await RunAsync(connection, transaction: null, apply, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await using var release = Command(connection, null, SchemaLedgerSql.ReleaseLock(engine));
            await release.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task RunAsync(DbConnection connection, DbTransaction? transaction, bool apply, CancellationToken cancellationToken)
    {
        await using (var exists = Command(connection, transaction, SchemaLedgerSql.TableExists(engine)))
        {
            if (Convert.ToInt64(await exists.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture) == 0)
            {
                if (!apply)
                {
                    throw new SchemaVersionException(
                        $"The {component} schema is not installed. Run these scripts, or set the schema mode to Apply: {string.Join(", ", scripts.Select(s => s.Name))}.");
                }

                await using var create = Command(connection, transaction, SchemaLedgerSql.CreateTable(engine));
                await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        var applied = await ReadAppliedAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        var current = Validate(applied);
        if (current == LatestVersion || (!apply && current >= minimumVersion))
        {
            return;
        }

        var pending = scripts.Where(s => s.Version > current).ToList();
        if (!apply)
        {
            throw new SchemaVersionException(
                $"The {component} schema is at version {current}, and this code needs at least version {minimumVersion}. Run these scripts, or set the schema mode to Apply: {string.Join(", ", pending.Select(s => s.Name))}.");
        }

        if (applied.Any(row => row.Source == SchemaLedger.EntityFrameworkSource))
        {
            throw new SchemaVersionException(
                $"The {component} schema is managed by EF Core migrations, so the runner does not apply scripts to it. Add the pending version to an EF migration instead.");
        }

        foreach (var script in pending)
        {
            await ApplyAsync(connection, transaction, script, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ApplyAsync(DbConnection connection, DbTransaction? outer, SchemaScript script, CancellationToken cancellationToken)
    {
        var transactional = engine is RelationalEngine.SqlServer or RelationalEngine.PostgreSql;
        await using var own = transactional ? await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false) : null;
        var transaction = own ?? outer;

        await using (var run = Command(connection, transaction, script.Text))
        {
            await run.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // Shipped scripts record themselves as run by hand; the runner then records what it applied, with the checksum.
        foreach (var sql in new[] { SchemaLedger.Update, SchemaLedger.Insert })
        {
            await using var record = Command(connection, transaction, sql);
            record.AddParameter("component", component, DbType.String);
            record.AddParameter("version", script.Version, DbType.Int32);
            record.AddParameter("script", script.Name, DbType.String);
            record.AddParameter("checksum", script.Checksum, DbType.String);
            record.AddParameter("source", SchemaLedger.ScriptSource, DbType.String);
            if (await record.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0)
            {
                break;
            }
        }

        if (own is not null)
        {
            await own.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Checks the applied rows and returns the current version.
    /// </summary>
    private int Validate(List<AppliedVersion> applied)
    {
        for (var i = 0; i < applied.Count; i++)
        {
            var row = applied[i];
            if (row.Version > LatestVersion)
            {
                throw new SchemaVersionException(
                    $"The {component} schema is at version {applied[^1].Version}, newer than the latest version {LatestVersion} this code supports. Upgrade the package.");
            }

            if (row.Version != i + 1)
            {
                throw new SchemaVersionException($"The {component} schema ledger is missing version {i + 1}.");
            }

            var script = scripts[i];
            if (row.Source == SchemaLedger.ScriptSource && !string.Equals(row.Checksum, script.Checksum, StringComparison.OrdinalIgnoreCase))
            {
                throw new SchemaVersionException(
                    $"The {component} schema script {script.Name} was applied with checksum {row.Checksum}, but this code ships checksum {script.Checksum}. A shipped script must never change.");
            }
        }

        return applied.Count;
    }

    private async Task<List<AppliedVersion>> ReadAppliedAsync(DbConnection connection, DbTransaction? transaction, CancellationToken cancellationToken)
    {
        await using var select = Command(connection, transaction, SchemaLedger.SelectApplied);
        select.AddParameter("component", component, DbType.String);
        await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var applied = new List<AppliedVersion>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            applied.Add(new AppliedVersion(
                Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.GetString(2)));
        }

        return applied;
    }

    private async Task AcquireLockAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        await using var acquire = Command(connection, null, SchemaLedgerSql.AcquireLock(engine));
        acquire.CommandTimeout = (int)LockTimeout.TotalSeconds + 30;
        if (engine == RelationalEngine.SqlServer)
        {
            acquire.AddParameter("timeout_ms", (int)LockTimeout.TotalMilliseconds, DbType.Int32);
        }
        else if (engine is RelationalEngine.MySql or RelationalEngine.MariaDb)
        {
            acquire.AddParameter("timeout_s", (int)LockTimeout.TotalSeconds, DbType.Int32);
        }

        var granted = Convert.ToInt64(await acquire.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), CultureInfo.InvariantCulture);
        if (granted <= 0)
        {
            throw new TimeoutException($"Timed out after {LockTimeout} waiting for the schema migration lock held by another process.");
        }
    }

    private static DbCommand Command(DbConnection connection, DbTransaction? transaction, string text)
    {
        var command = connection.CreateCommand();
        command.CommandText = text;
        command.Transaction = transaction;
        return command;
    }

    private sealed record AppliedVersion(int Version, string? Checksum, string Source);
}
