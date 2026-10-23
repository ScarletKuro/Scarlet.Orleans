using System.Data.Common;
using System.Globalization;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// Classifies engine errors from what <see cref="DbException"/> exposes, so no driver is referenced.
/// </summary>
internal static class DbErrors
{
    /// <summary>
    /// Whether an insert failed because its primary or unique key already exists.
    /// </summary>
    /// <remarks>
    /// <para>
    /// MySqlConnector reports every integrity violation as SQLSTATE 23000, so callers only use this for inserts whose
    /// sole possible violation is the key: every other column is supplied and unconstrained.
    /// </para>
    /// <para>
    /// Microsoft.Data.Sqlite exposes its result code only on its own exception type, so SQLite errors cannot be
    /// classified here. SQLite statements are written so they need no classification (<c>ON CONFLICT DO NOTHING</c>).
    /// </para>
    /// </remarks>
    public static bool IsDuplicateKey(RelationalEngine engine, DbException exception) => engine switch
    {
        RelationalEngine.PostgreSql => exception.SqlState == "23505",
        // ER_DUP_ENTRY
        RelationalEngine.MySql or RelationalEngine.MariaDb => exception.SqlState == "23000" || MySqlErrorNumber(exception) == 1062,
        RelationalEngine.SqlServer => SqlServerErrorNumber(exception) is 2601 or 2627,
        _ => false,
    };

    /// <summary>
    /// Whether the server rejected a statement in a way that rolled it back and that a later attempt may not repeat.
    /// </summary>
    /// <remarks>
    /// Only errors returned by the server qualify. A broken connection is never transient here, because the statement
    /// may have committed before the connection failed (design section 4.7). SQLite is absent: Microsoft.Data.Sqlite
    /// already retries a busy database itself until the command timeout.
    /// </remarks>
    public static bool IsTransient(RelationalEngine engine, DbException exception) => engine switch
    {
        // serialization_failure, deadlock_detected
        RelationalEngine.PostgreSql => exception.SqlState is "40001" or "40P01",
        // ER_LOCK_DEADLOCK rolls back the whole transaction and reports SQLSTATE 40001.
        RelationalEngine.MySql or RelationalEngine.MariaDb => exception.SqlState == "40001" || MySqlErrorNumber(exception) == 1213,
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
    /// MySql.Data leaves <see cref="DbException.SqlState"/> empty for errors returned by the server, but records the
    /// error number in <see cref="Exception.Data"/>. MySqlConnector populates <see cref="DbException.SqlState"/> instead.
    /// </summary>
    private static int? MySqlErrorNumber(DbException exception) =>
        exception.Data["Server Error Code"] is int number ? number : null;
}
