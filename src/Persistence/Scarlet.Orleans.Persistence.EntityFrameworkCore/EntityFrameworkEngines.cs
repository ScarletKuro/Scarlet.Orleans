using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

internal static class EntityFrameworkEngines
{
    /// <summary>
    /// Maps an EF Core provider name to its engine, by name, so no EF provider package is referenced.
    /// </summary>
    /// <remarks>
    /// MySQL has three providers: Pomelo, its maintained fork Microting (over MySqlConnector), and Oracle's
    /// MySql.EntityFrameworkCore (over MySql.Data). MariaDB is reached through the MySqlConnector-based providers.
    /// </remarks>
    public static RelationalEngine FromProviderName(string? providerName) => providerName switch
    {
        "Microsoft.EntityFrameworkCore.SqlServer" => RelationalEngine.SqlServer,
        "Npgsql.EntityFrameworkCore.PostgreSQL" => RelationalEngine.PostgreSql,
        "Pomelo.EntityFrameworkCore.MySql" or "Microting.EntityFrameworkCore.MySql" or "MySql.EntityFrameworkCore" => RelationalEngine.MySql,
        "Microsoft.EntityFrameworkCore.Sqlite" => RelationalEngine.Sqlite,
        _ => throw new NotSupportedException(
            $"EF Core provider \"{providerName}\" is not supported. Supported providers: Microsoft.EntityFrameworkCore.SqlServer, Npgsql.EntityFrameworkCore.PostgreSQL, Microting.EntityFrameworkCore.MySql, Pomelo.EntityFrameworkCore.MySql, MySql.EntityFrameworkCore, Microsoft.EntityFrameworkCore.Sqlite."),
    };
}
