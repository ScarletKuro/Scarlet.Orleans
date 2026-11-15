using System.Data.Common;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// The database engines that have a dialect.
/// </summary>
/// <remarks>
/// MariaDB has its own name so that an application can state it, and so that SQL can diverge from MySQL's if a
/// MariaDB difference ever needs it; today every component runs the same SQL and scripts for both.
/// </remarks>
internal enum RelationalEngine
{
    SqlServer,
    PostgreSql,
    MySql,
    MariaDb,
    Sqlite,
}

internal static class RelationalEngines
{
    /// <summary>
    /// The engine names accepted by the <c>Engine</c> option of every package.
    /// </summary>
    public static IReadOnlyList<string> Names { get; } = Enum.GetNames<RelationalEngine>();

    public static bool TryParse(string name, out RelationalEngine engine) =>
        Enum.TryParse(name, ignoreCase: true, out engine) && Enum.IsDefined(engine);

    public static RelationalEngine Parse(string name) =>
        TryParse(name, out var engine)
            ? engine
            : throw new ArgumentException($"Unknown database engine \"{name}\". Supported engines: {string.Join(", ", Names)}.", nameof(name));

    /// <summary>
    /// Infers the engine from the driver's connection type, by name, so no driver is referenced.
    /// </summary>
    /// <remarks>
    /// MySqlConnector serves both MySQL and MariaDB, so its connections map to MySQL, which is also correct for MariaDB.
    /// Oracle's MySql.Data is supported as well; it populates <see cref="DbException.SqlState"/> like MySqlConnector.
    /// </remarks>
    public static RelationalEngine FromConnection(DbConnection connection) => connection.GetType().FullName switch
    {
        "Microsoft.Data.SqlClient.SqlConnection" => RelationalEngine.SqlServer,
        "Npgsql.NpgsqlConnection" => RelationalEngine.PostgreSql,
        "MySqlConnector.MySqlConnection" or "MySql.Data.MySqlClient.MySqlConnection" => RelationalEngine.MySql,
        "Microsoft.Data.Sqlite.SqliteConnection" => RelationalEngine.Sqlite,
        var name => throw new NotSupportedException(
            $"Cannot infer the database engine from connection type \"{name}\". Set the Engine option to one of: {string.Join(", ", Names)}."),
    };
}
