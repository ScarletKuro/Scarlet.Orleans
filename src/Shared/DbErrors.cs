using System.Data.Common;
using System.Globalization;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// Classifies engine errors from what <see cref="DbException"/> exposes, so no driver is referenced.
/// </summary>
/// <remarks>
/// <para>
/// This is the one place that knows which driver reports what. Each rule reads a driver behaviour that is pinned by a
/// regression test against that driver's real exception type, so a driver release that changes it fails the build
/// rather than silently turning conflicts into errors, or disabling retries.
/// </para>
/// <para>
/// Microsoft.Data.Sqlite exposes its result code only on its own exception type, so SQLite errors cannot be classified
/// here. SQLite statements are written so they need no classification (<c>ON CONFLICT DO NOTHING</c>), and the driver
/// retries a busy database itself until the command timeout.
/// </para>
/// </remarks>
internal static class DbErrors
{
    /// <summary>
    /// Whether an insert failed because its primary or unique key already exists.
    /// </summary>
    public static bool IsDuplicateKey(RelationalEngine engine, DbException exception) => engine switch
    {
        // unique_violation
        RelationalEngine.PostgreSql => exception.SqlState == "23505",
        // ER_DUP_ENTRY. Not SQLSTATE 23000, which MySQL reports for every integrity violation.
        RelationalEngine.MySql or RelationalEngine.MariaDb => MySqlErrorNumber(exception) == 1062,
        // Unique index, primary key or unique constraint violation.
        RelationalEngine.SqlServer => SqlServerErrorNumber(exception) is 2601 or 2627,
        _ => false,
    };

    /// <summary>
    /// Whether the server rejected a statement in a way that rolled it back and that a later attempt may not repeat.
    /// </summary>
    /// <remarks>
    /// Only errors returned by the server qualify. A broken connection is never transient here, because the statement
    /// may have committed before the connection failed (design section 4.7).
    /// </remarks>
    public static bool IsTransient(RelationalEngine engine, DbException exception) => engine switch
    {
        // serialization_failure, deadlock_detected
        RelationalEngine.PostgreSql => exception.SqlState is "40001" or "40P01",
        // ER_LOCK_DEADLOCK, which rolls back the whole transaction. Not ER_LOCK_WAIT_TIMEOUT, which rolls back only the
        // statement.
        RelationalEngine.MySql or RelationalEngine.MariaDb => MySqlErrorNumber(exception) == 1213,
        // Chosen as deadlock victim.
        RelationalEngine.SqlServer => SqlServerErrorNumber(exception) == 1205,
        _ => false,
    };

    /// <summary>
    /// SqlClient exposes the error number only on its own exception type, but also records it in
    /// <see cref="Exception.Data"/>, which is readable without referencing the driver.
    /// </summary>
    private static int? SqlServerErrorNumber(DbException exception) =>
        exception.Data["HelpLink.EvtID"] is string number && int.TryParse(number, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    /// <summary>
    /// Both MySqlConnector and MySql.Data record the server's error number in <see cref="Exception.Data"/>. It is precise,
    /// unlike SQLSTATE, and MySql.Data leaves <see cref="DbException.SqlState"/> empty for errors returned by the server.
    /// </summary>
    private static int? MySqlErrorNumber(DbException exception) =>
        exception.Data["Server Error Code"] is int number ? number : null;
}
