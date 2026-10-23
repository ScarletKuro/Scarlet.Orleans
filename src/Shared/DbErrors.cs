using System.Data.Common;
using System.Globalization;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// Classifies server errors from what <see cref="DbException"/> exposes, so no driver is referenced.
/// </summary>
/// <remarks>
/// <para>
/// This is the one place that knows which driver reports what. Every rule matches a signature that only its own driver
/// produces, so no engine has to be known: a standard SQLSTATE, or an entry that one driver records in
/// <see cref="Exception.Data"/>. Each rule is pinned by a regression test against that driver's real exception type, so
/// a driver release that changes it fails the build rather than silently turning conflicts into errors, or disabling
/// retries.
/// </para>
/// <para>
/// Microsoft.Data.Sqlite exposes its result codes only on its own exception type. The ADO.NET flavour writes its SQLite
/// statements so that they need no classification (<c>ON CONFLICT DO NOTHING</c>), and the driver retries a busy
/// database itself until the command timeout.
/// </para>
/// </remarks>
internal static class DbErrors
{
    /// <summary>
    /// Whether an insert failed because its primary or unique key already exists.
    /// </summary>
    public static bool IsDuplicateKey(DbException exception) =>
        // unique_violation (PostgreSQL, and any engine that reports the standard SQLSTATE)
        exception.SqlState == "23505"
        // ER_DUP_ENTRY (MySQL and MariaDB). Not SQLSTATE 23000, which MySQL reports for every integrity violation.
        || MySqlErrorNumber(exception) == 1062
        // Unique index, primary key or unique constraint violation (SQL Server)
        || SqlServerErrorNumber(exception) is 2601 or 2627;

    /// <summary>
    /// Whether the server rejected a statement in a way that rolled it back and that a later attempt may not repeat.
    /// </summary>
    /// <remarks>
    /// Only errors returned by the server qualify. A broken connection is never transient here, because the statement
    /// may have committed before the connection failed (design section 4.7).
    /// </remarks>
    public static bool IsTransient(DbException exception) =>
        // serialization_failure, deadlock_detected (PostgreSQL, and any engine that reports the standard SQLSTATE)
        exception.SqlState is "40001" or "40P01"
        // ER_LOCK_DEADLOCK, which rolls back the whole transaction (MySQL and MariaDB). Not ER_LOCK_WAIT_TIMEOUT, which
        // rolls back only the statement.
        || MySqlErrorNumber(exception) == 1213
        // Chosen as deadlock victim (SQL Server)
        || SqlServerErrorNumber(exception) == 1205;

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
