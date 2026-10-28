using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration.Overrides;
using Orleans.Providers;
using Orleans.Runtime.Hosting;
using Orleans.Serialization.Serializers;
using Orleans.Storage;
using Scarlet.Orleans.Persistence;
using Scarlet.Orleans.Persistence.AdoNet;
using Scarlet.Orleans.Relational;

namespace Orleans.Hosting;

/// <summary>
/// Registers ADO.NET grain storage.
/// </summary>
public static class ScarletAdoNetGrainStorageExtensions
{
    /// <summary>
    /// Adds ADO.NET grain storage as the default storage provider.
    /// </summary>
    public static ISiloBuilder AddScarletAdoNetGrainStorageAsDefault(this ISiloBuilder builder, Action<ScarletAdoNetGrainStorageOptions>? configure = null) =>
        builder.AddScarletAdoNetGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configure);

    /// <summary>
    /// Adds ADO.NET grain storage named <paramref name="name"/>.
    /// </summary>
    public static ISiloBuilder AddScarletAdoNetGrainStorage(this ISiloBuilder builder, string name, Action<ScarletAdoNetGrainStorageOptions>? configure = null) =>
        builder.ConfigureServices(services => services.AddScarletAdoNetGrainStorage(name, configure));

    /// <summary>
    /// Adds ADO.NET grain storage named <paramref name="name"/>.
    /// </summary>
    public static IServiceCollection AddScarletAdoNetGrainStorage(this IServiceCollection services, string name, Action<ScarletAdoNetGrainStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var options = services.AddOptions<ScarletAdoNetGrainStorageOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.Validate(
            static o => o.Engine is null || RelationalEngines.TryParse(o.Engine, out _),
            $"{nameof(ScarletAdoNetGrainStorageOptions.Engine)} must be one of: {string.Join(", ", RelationalEngines.Names)}.");
        services.AddTransient<IPostConfigureOptions<ScarletAdoNetGrainStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<ScarletAdoNetGrainStorageOptions>>();
        return services.AddGrainStorage(name, Create);
    }

    private static RelationalGrainStorage Create(IServiceProvider services, string name)
    {
        var options = services.GetRequiredService<IOptionsMonitor<ScarletAdoNetGrainStorageOptions>>().Get(name);
        var dataSource = options.DataSource
            ?? (options.DataSourceServiceKey is { } key
                ? services.GetRequiredKeyedService<DbDataSource>(key)
                : services.GetService<DbDataSource>())
            ?? throw new InvalidOperationException(
                $"Grain storage \"{name}\" has no data source. Set {nameof(ScarletAdoNetGrainStorageOptions)}.{nameof(ScarletAdoNetGrainStorageOptions.DataSource)} or register a {nameof(DbDataSource)}.");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalGrainStorage(
            name,
            services.GetProviderClusterOptions(name).Value.ServiceId,
            new AdoNetGrainStateStore(dataSource, options.Engine, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<AdoNetGrainStateStore>>()),
            options.GrainStorageSerializer,
            services.GetRequiredService<IActivatorProvider>(),
            options.DeleteStateOnClear,
            options.MaxPayloadSize,
            options.InitStage,
            timeProvider,
            services.GetRequiredService<ILogger<RelationalGrainStorage>>());
    }
}
