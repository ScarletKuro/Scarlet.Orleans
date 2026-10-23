using System.Data.Common;
using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Npgsql;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Pins what each driver exposes for the server errors that <see cref="DbErrors"/> classifies.
/// </summary>
/// <remarks>
/// Exceptions are the drivers' own types, built the way each driver builds them for an error returned by the server.
/// MySqlConnector, MySql.Data and SqlClient do that through internal constructors only, reached here by reflection, so a
/// driver release that changes them fails these tests instead of silently changing conflict handling and retries.
/// </remarks>
public sealed class DbErrorsTests
{
    public static TheoryData<string, int, bool> DuplicateKeys => new()
    {
        { "Npgsql", 23505, true },
        { "Npgsql", 23503, false },
        { "MySqlConnector", 1062, true },
        { "MySqlConnector", 1452, false },
        { "MySql.Data", 1062, true },
        { "MySql.Data", 1452, false },
        { "SqlClient", 2627, true },
        { "SqlClient", 2601, true },
        { "SqlClient", 547, false },
        { "Sqlite", 19, false },
    };

    public static TheoryData<string, int, bool> TransientErrors => new()
    {
        { "Npgsql", 40001, true },
        { "Npgsql", 40002, false },
        { "MySqlConnector", 1213, true },
        { "MySqlConnector", 1205, false },
        { "MySql.Data", 1213, true },
        { "MySql.Data", 1205, false },
        { "SqlClient", 1205, true },
        { "SqlClient", 1222, false },
        { "Sqlite", 5, false },
    };

    [Theory]
    [MemberData(nameof(DuplicateKeys))]
    public void IsDuplicateKey_ReadsWhatTheDriverReports(string driver, int error, bool expected) =>
        Assert.Equal(expected, DbErrors.IsDuplicateKey(ServerError(driver, error)));

    [Theory]
    [MemberData(nameof(TransientErrors))]
    public void IsTransient_ReadsWhatTheDriverReports(string driver, int error, bool expected) =>
        Assert.Equal(expected, DbErrors.IsTransient(ServerError(driver, error)));

    [Fact]
    public void PostgreSqlDeadlock_IsTransient() =>
        Assert.True(DbErrors.IsTransient(new PostgresException("deadlock detected", "ERROR", "ERROR", "40P01")));

    /// <param name="error">The SQLSTATE for PostgreSQL, otherwise the engine's error number.</param>
    private static DbException ServerError(string driver, int error) => driver switch
    {
        "Npgsql" => new PostgresException("server error", "ERROR", "ERROR", error.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        "MySqlConnector" => (DbException)Construct(
            typeof(MySqlConnector.MySqlException),
            [typeof(MySqlConnector.MySqlErrorCode), typeof(string), typeof(string)],
            [(MySqlConnector.MySqlErrorCode)error, MySqlSqlState(error), "server error"]),
        "MySql.Data" => (DbException)Construct(typeof(MySql.Data.MySqlClient.MySqlException), [typeof(string), typeof(int)], ["server error", error]),
        "SqlClient" => SqlServerError(error),
        "Sqlite" => new SqliteException("server error", error),
        _ => throw new ArgumentOutOfRangeException(nameof(driver)),
    };

    /// <summary>The SQLSTATE MySQL sends with each error: integrity violations share 23000, whatever their cause.</summary>
    private static string MySqlSqlState(int error) => error switch
    {
        1062 or 1452 => "23000",
        1213 => "40001",
        _ => "HY000",
    };

    private static SqlException SqlServerError(int number)
    {
        var error = (SqlError)Construct(
            typeof(SqlError),
            [typeof(int), typeof(byte), typeof(byte), typeof(string), typeof(string), typeof(string), typeof(int), typeof(Exception)],
            [number, (byte)1, (byte)16, "server", "server error", "", 0, null]);
        var errors = (SqlErrorCollection)Construct(typeof(SqlErrorCollection), [], []);
        typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance)!.Invoke(errors, [error]);
        return (SqlException)typeof(SqlException)
            .GetMethod("CreateException", BindingFlags.NonPublic | BindingFlags.Static, [typeof(SqlErrorCollection), typeof(string)])!
            .Invoke(null, [errors, "16.0"])!;
    }

    private static object Construct(Type type, Type[] parameterTypes, object?[] arguments) =>
        (type.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, parameterTypes)
            ?? throw new MissingMethodException($"{type.FullName} no longer has the constructor this test uses; check how the driver now reports server errors."))
        .Invoke(arguments);
}
