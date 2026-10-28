using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.GrainDirectory;
using Orleans.Hosting;

namespace Scarlet.Orleans.GrainDirectory.Tests;

/// <summary>
/// The hosting extensions register the directory under the name Orleans resolves. Silo tests cover named directories
/// only, because the in-process test cluster replaces the default one (see <see cref="GrainDirectoryClusterTests"/>).
/// </summary>
public sealed class RegistrationTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("named")]
    public void Directory_IsRegisteredUnderItsName(string? name)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContextFactory<TestDbContext>(options => options.UseSqlite("Data Source=:memory:"));
        builder.UseOrleans(silo =>
        {
            silo.UseLocalhostClustering();
            if (name is null)
            {
                silo.UseScarletEntityFrameworkCoreGrainDirectoryAsDefault<TestDbContext>();
            }
            else
            {
                silo.AddScarletEntityFrameworkCoreGrainDirectory<TestDbContext>(name);
            }
        });
        using var host = builder.Build();

        var directory = host.Services.GetRequiredKeyedService<IGrainDirectory>(name ?? GrainDirectoryAttribute.DEFAULT_GRAIN_DIRECTORY);

        Assert.IsType<RelationalGrainDirectory>(directory);
    }
}
