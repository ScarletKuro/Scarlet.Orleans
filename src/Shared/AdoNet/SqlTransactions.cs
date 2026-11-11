using System.Data;
using System.Data.Common;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// Helpers for transactions started in SQL (<c>START TRANSACTION</c> inside a command), which save the round trip of
/// the driver's own begin, but which the driver does not track.
/// </summary>
internal static class SqlTransactions
{
    /// <summary>
    /// Rolls back a transaction started in SQL that did not commit. The driver cannot, since it does not track it, and
    /// MySql.Data does not reset a pooled connection by default, so without this an open transaction, and its locks,
    /// would return to the pool.
    /// </summary>
    public static async Task RollbackAsync(DbConnection connection)
    {
        if (connection.State != ConnectionState.Open)
        {
            return;
        }

        await using var rollback = connection.CreateCommand();
        rollback.CommandText = "ROLLBACK";
        try
        {
            await rollback.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (DbException)
        {
            // A broken connection; the server rolls back when the session ends.
        }
    }

    /// <summary>
    /// Moves to the next result set that has columns, skipping the statements of a multi-statement command that return
    /// none. Returns <see langword="false"/> if there is none.
    /// </summary>
    public static async Task<bool> NextResultWithColumnsAsync(DbDataReader reader, bool first, CancellationToken cancellationToken)
    {
        if (!first && !await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        while (reader.FieldCount == 0)
        {
            if (!await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }
}
