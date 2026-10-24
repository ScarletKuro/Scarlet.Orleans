using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Relational.EntityFrameworkCore;

/// <summary>
/// Tells from a failed insert's exception whether a row with the same key already existed.
/// </summary>
/// <remarks>
/// The exception is the only reliable evidence: the database's later contents cannot identify the cause of an earlier
/// failure, because the conflicting row may be deleted in between. Drivers are recognised by their exception types'
/// names, so none is referenced.
/// </remarks>
internal static class InsertErrors
{
    private const string SqliteException = "Microsoft.Data.Sqlite.SqliteException";

    /// <summary>The exception types of the drivers whose duplicate-key errors can be recognised.</summary>
    private static readonly HashSet<string> KnownDrivers =
    [
        "Npgsql.NpgsqlException",
        "Microsoft.Data.SqlClient.SqlException",
        "MySqlConnector.MySqlException",
        "MySql.Data.MySqlClient.MySqlException",
        SqliteException,
    ];

    /// <summary>
    /// Returns whether <paramref name="exception"/> reports a duplicate key, or <see langword="null"/> if its cause cannot
    /// be recognised.
    /// </summary>
    /// <param name="exception">The error of the failed insert, unwrapped from EF Core's <c>DbUpdateException</c>.</param>
    /// <param name="isDuplicateKeyError">
    /// The application's own rule, the <c>IsDuplicateKeyError</c> option of the EF Core packages. When
    /// set, it alone decides, for drivers whose errors the built-in rules do not recognise.
    /// </param>
    public static bool? IsDuplicateKey(Exception exception, Func<DbException, bool>? isDuplicateKeyError = null)
    {
        if (exception is not DbException { } error)
        {
            return null;
        }

        if (isDuplicateKeyError is not null)
        {
            return isDuplicateKeyError(error);
        }

        if (IsFromKnownDriver(error))
        {
            return DbErrors.IsDuplicateKey(error) || SqliteExtendedErrorCode(error) is 1555 or 2067;
        }

        // Any other driver that reports a standard SQLSTATE: 23505 is unique_violation, and a code outside class 23
        // (integrity constraint violation) cannot be one. Other class 23 codes are left open, because some drivers report
        // a duplicate key as the general 23000.
        return error.SqlState switch
        {
            "23505" => true,
            { Length: 5 } state when !state.StartsWith("23", StringComparison.Ordinal) => false,
            _ => null,
        };
    }

    private static bool IsFromKnownDriver(DbException exception)
    {
        for (var type = exception.GetType(); type is not null; type = type.BaseType)
        {
            if (type.FullName is { } name && KnownDrivers.Contains(name))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Microsoft.Data.Sqlite exposes its result codes only on its own exception type, read here by property name:
    /// SQLITE_CONSTRAINT_PRIMARYKEY (1555) or SQLITE_CONSTRAINT_UNIQUE (2067).
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "The EF Core flavour is not trimming-compatible; the property is public on a driver type.")]
    private static int? SqliteExtendedErrorCode(DbException exception) =>
        exception.GetType().FullName == SqliteException
            ? exception.GetType().GetProperty("SqliteExtendedErrorCode")?.GetValue(exception) as int?
            : null;
}
