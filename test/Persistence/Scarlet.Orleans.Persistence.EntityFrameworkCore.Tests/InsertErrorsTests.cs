using System.Data.Common;
using Microsoft.Data.Sqlite;
using Npgsql;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;

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

    [Fact]
    public void UnknownDriver_IsNotClassified() =>
        Assert.Null(InsertErrors.IsDuplicateKey(new UnknownDriverException()));

    private sealed class UnknownDriverException() : DbException("unknown");
}
