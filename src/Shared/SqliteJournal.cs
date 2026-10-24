using System.Data.Common;
using Microsoft.Extensions.Logging;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// Checks, and on request enables, SQLite's write-ahead log.
/// </summary>
/// <remarks>
/// In SQLite's default rollback journal, readers block writers, so concurrent grain writes can wait out the busy timeout
/// and fail with "database is locked". The write-ahead log lets readers and a writer proceed together. The journal mode is
/// a property of the database file and persists, so only a caller that may change the schema enables it; others warn.
/// In-memory databases report <c>memory</c> and are left as they are.
/// </remarks>
internal static partial class SqliteJournal
{
    public static async Task EnsureWriteAheadLogAsync(DbConnection connection, bool enable, ILogger logger, CancellationToken cancellationToken)
    {
        var mode = await ExecuteAsync(connection, "PRAGMA journal_mode", cancellationToken).ConfigureAwait(false);
        if (string.Equals(mode, "wal", StringComparison.OrdinalIgnoreCase) || string.Equals(mode, "memory", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (!enable)
        {
            LogNotWriteAheadLog(logger, connection.DataSource, mode);
            return;
        }

        var enabled = await ExecuteAsync(connection, "PRAGMA journal_mode = WAL", cancellationToken).ConfigureAwait(false);
        LogEnabledWriteAheadLog(logger, connection.DataSource, mode, enabled);
    }

    private static async Task<string> ExecuteAsync(DbConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string ?? "";
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "SQLite database {DataSource} uses the {Mode} journal, in which concurrent writes can fail with \"database is locked\". Run PRAGMA journal_mode = WAL once on the database file.")]
    private static partial void LogNotWriteAheadLog(ILogger logger, string dataSource, string mode);

    [LoggerMessage(Level = LogLevel.Information, Message = "Switched SQLite database {DataSource} from the {Previous} journal to {Mode}.")]
    private static partial void LogEnabledWriteAheadLog(ILogger logger, string dataSource, string previous, string mode);
}
