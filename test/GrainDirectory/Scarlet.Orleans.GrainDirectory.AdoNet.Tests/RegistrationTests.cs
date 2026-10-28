using Microsoft.Data.Sqlite;
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
        builder.UseOrleans(silo =>
        {
            silo.UseLocalhostClustering();
            var dataSource = SqliteFactory.Instance.CreateDataSource("Data Source=:memory:");
            if (name is null)
            {
                silo.UseScarletAdoNetGrainDirectoryAsDefault(options => options.DataSource = dataSource);
            }
            else
            {
                silo.AddScarletAdoNetGrainDirectory(name, options => options.DataSource = dataSource);
            }
        });
        using var host = builder.Build();

        var directory = host.Services.GetRequiredKeyedService<IGrainDirectory>(name ?? GrainDirectoryAttribute.DEFAULT_GRAIN_DIRECTORY);

        Assert.IsType<RelationalGrainDirectory>(directory);
    }
}
