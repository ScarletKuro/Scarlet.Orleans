using Microsoft.EntityFrameworkCore;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Every supported EF Core provider is recognised by the name it reports, without a database.
/// </summary>
public sealed class ProviderNameTests
{
    public static TheoryData<string, string> Providers => new()
    {
        { "Sqlite", "Sqlite" },
        { "Npgsql", "PostgreSql" },
        { "SqlServer", "SqlServer" },
        { "Microting", "MySql" },
        { "Oracle", "MySql" },
    };

    [Theory]
    [MemberData(nameof(Providers))]
    public void ProviderName_MapsToItsEngine(string provider, string expected)
    {
        var builder = new DbContextOptionsBuilder<TestDbContext>();
        _ = provider switch
        {
            "Sqlite" => builder.UseSqlite("Data Source=:memory:"),
            "Npgsql" => builder.UseNpgsql("Host=localhost"),
            "SqlServer" => builder.UseSqlServer("Server=localhost"),
            "Microting" => builder.UseMySql("Server=localhost", new MySqlServerVersion(new Version(8, 4, 0))),
            "Oracle" => builder.UseMySQL("Server=localhost"),
            _ => throw new ArgumentOutOfRangeException(nameof(provider)),
        };
        using var context = new TestDbContext(builder.Options);

        Assert.Equal(expected, EntityFrameworkEngines.FromProviderName(context.Database.ProviderName).ToString());
    }
}
