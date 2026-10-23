using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

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
    /// Returns whether <paramref name="exception"/> reports a duplicate key, or <see langword="null"/> if it comes from a
    /// driver whose errors cannot be recognised.
    /// </summary>
    public static bool? IsDuplicateKey(Exception exception)
    {
        if (exception is not DbException { } error || !IsFromKnownDriver(error))
        {
            return null;
        }

        return DbErrors.IsDuplicateKey(error) || SqliteExtendedErrorCode(error) is 1555 or 2067;
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
