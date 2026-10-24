using System.Data.Common;
using Microsoft.Data.Sqlite;
using Npgsql;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;
using Scarlet.Orleans.Relational.EntityFrameworkCore;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Duplicate keys recognised from the failed insert's own exception, including SQLite's, which only its own type exposes.
/// </summary>
public sealed class InsertErrorsTests
{
    [Theory]
    [InlineData("INSERT INTO t VALUES (1, 'b')", true)]
    [InlineData("INSERT INTO t VALUES (2, NULL)", false)]
    public void Sqlite_RecognisesDuplicateKeysOnly(string insert, bool expected)
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT NOT NULL); INSERT INTO t VALUES (1, 'a');";
            create.ExecuteNonQuery();
        }

        using var command = connection.CreateCommand();
        command.CommandText = insert;
        var exception = Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());

        Assert.Equal(expected, InsertErrors.IsDuplicateKey(exception));
    }

    [Fact]
    public void KnownDriver_RecognisesItsDuplicateKeys()
    {
        Assert.True(InsertErrors.IsDuplicateKey(new PostgresException("duplicate", "ERROR", "ERROR", "23505")));
        Assert.False(InsertErrors.IsDuplicateKey(new PostgresException("foreign key", "ERROR", "ERROR", "23503")));
    }

    [Theory]
    [InlineData("23505", true)]
    [InlineData("23000", null)]
    [InlineData("23503", null)]
    [InlineData("42P01", false)]
    [InlineData(null, null)]
    public void UnknownDriver_IsClassifiedByItsStandardSqlState(string? sqlState, bool? expected) =>
        Assert.Equal(expected, InsertErrors.IsDuplicateKey(new UnknownDriverException(sqlState)));

    [Fact]
    public void ApplicationRule_DecidesAlone()
    {
        Assert.False(InsertErrors.IsDuplicateKey(new PostgresException("duplicate", "ERROR", "ERROR", "23505"), _ => false));
        Assert.True(InsertErrors.IsDuplicateKey(new UnknownDriverException(sqlState: null), _ => true));
    }

    [Fact]
    public void NonDatabaseError_IsNotClassified() =>
        Assert.Null(InsertErrors.IsDuplicateKey(new InvalidOperationException(), _ => true));

    private sealed class UnknownDriverException(string? sqlState) : DbException("unknown driver")
    {
        public override string? SqlState { get; } = sqlState;
    }
}
