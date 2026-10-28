using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory;
using Orleans.Runtime.Hosting;
using Scarlet.Orleans.GrainDirectory;
using Scarlet.Orleans.GrainDirectory.AdoNet;
using Scarlet.Orleans.Relational;

namespace Orleans.Hosting;

/// <summary>
/// Registers ADO.NET grain directories.
/// </summary>
public static class ScarletAdoNetGrainDirectoryExtensions
{
    /// <summary>
    /// Adds an ADO.NET grain directory as the default directory, used by grains without a <see cref="GrainDirectoryAttribute"/>.
    /// </summary>
    public static ISiloBuilder UseScarletAdoNetGrainDirectoryAsDefault(this ISiloBuilder builder, Action<ScarletAdoNetGrainDirectoryOptions>? configure = null) =>
        builder.AddScarletAdoNetGrainDirectory(GrainDirectoryAttribute.DEFAULT_GRAIN_DIRECTORY, configure);

    /// <summary>
    /// Adds an ADO.NET grain directory named <paramref name="name"/>, used by grains with <c>[GrainDirectory("name")]</c>.
    /// </summary>
    public static ISiloBuilder AddScarletAdoNetGrainDirectory(this ISiloBuilder builder, string name, Action<ScarletAdoNetGrainDirectoryOptions>? configure = null) =>
        builder.ConfigureServices(services => services.AddScarletAdoNetGrainDirectory(name, configure));

    /// <summary>
    /// Adds an ADO.NET grain directory named <paramref name="name"/>, used by grains with <c>[GrainDirectory("name")]</c>.
    /// </summary>
    public static IServiceCollection AddScarletAdoNetGrainDirectory(this IServiceCollection services, string name, Action<ScarletAdoNetGrainDirectoryOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var options = services.AddOptions<ScarletAdoNetGrainDirectoryOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.Validate(
            static o => o.Engine is null || RelationalEngines.TryParse(o.Engine, out _),
            $"{nameof(ScarletAdoNetGrainDirectoryOptions.Engine)} must be one of: {string.Join(", ", RelationalEngines.Names)}.");
        return services.AddGrainDirectory(name, Create);
    }

    private static RelationalGrainDirectory Create(IServiceProvider services, string name)
    {
        var options = services.GetRequiredService<IOptionsMonitor<ScarletAdoNetGrainDirectoryOptions>>().Get(name);
        var dataSource = options.DataSource
            ?? (options.DataSourceServiceKey is { } key
                ? services.GetRequiredKeyedService<DbDataSource>(key)
                : services.GetService<DbDataSource>())
            ?? throw new InvalidOperationException(
                $"Grain directory \"{name}\" has no data source. Set {nameof(ScarletAdoNetGrainDirectoryOptions)}.{nameof(ScarletAdoNetGrainDirectoryOptions.DataSource)} or register a {nameof(DbDataSource)}.");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalGrainDirectory(
            name,
            services.GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId,
            new AdoNetGrainDirectoryStore(dataSource, options.Engine, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<AdoNetGrainDirectoryStore>>()),
            options.InitStage,
            timeProvider,
            services.GetRequiredService<ILogger<RelationalGrainDirectory>>());
    }
}
