using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration.Overrides;
using Orleans.Providers;
using Orleans.Runtime.Hosting;
using Orleans.Serialization.Serializers;
using Orleans.Storage;
using Scarlet.Orleans.Persistence;
using Scarlet.Orleans.Persistence.EntityFrameworkCore;

namespace Orleans.Hosting;

/// <summary>
/// Registers EF Core grain storage.
/// </summary>
public static class ScarletEntityFrameworkCoreGrainStorageExtensions
{
    /// <summary>
    /// Adds EF Core grain storage on <typeparamref name="TContext"/> as the default storage provider.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansPersistenceModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static ISiloBuilder AddScarletEntityFrameworkCoreGrainStorageAsDefault<TContext>(this ISiloBuilder builder, Action<ScarletEntityFrameworkCoreGrainStorageOptions>? configure = null)
        where TContext : DbContext =>
        builder.AddScarletEntityFrameworkCoreGrainStorage<TContext>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configure);

    /// <summary>
    /// Adds EF Core grain storage on <typeparamref name="TContext"/> named <paramref name="name"/>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansPersistenceModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static ISiloBuilder AddScarletEntityFrameworkCoreGrainStorage<TContext>(this ISiloBuilder builder, string name, Action<ScarletEntityFrameworkCoreGrainStorageOptions>? configure = null)
        where TContext : DbContext =>
        builder.ConfigureServices(services => services.AddScarletEntityFrameworkCoreGrainStorage<TContext>(name, configure));

    /// <summary>
    /// Adds EF Core grain storage on <typeparamref name="TContext"/> named <paramref name="name"/>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansPersistenceModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static IServiceCollection AddScarletEntityFrameworkCoreGrainStorage<TContext>(this IServiceCollection services, string name, Action<ScarletEntityFrameworkCoreGrainStorageOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var options = services.AddOptions<ScarletEntityFrameworkCoreGrainStorageOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        services.AddTransient<IPostConfigureOptions<ScarletEntityFrameworkCoreGrainStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<ScarletEntityFrameworkCoreGrainStorageOptions>>();
        return services.AddGrainStorage(name, Create<TContext>);
    }

    private static RelationalGrainStorage Create<TContext>(IServiceProvider services, string name)
        where TContext : DbContext
    {
        var options = services.GetRequiredService<IOptionsMonitor<ScarletEntityFrameworkCoreGrainStorageOptions>>().Get(name);
        var contextFactory = services.GetService<IDbContextFactory<TContext>>()
            ?? throw new InvalidOperationException(
                $"Grain storage \"{name}\" needs IDbContextFactory<{typeof(TContext).Name}>. Register it with services.AddDbContextFactory<{typeof(TContext).Name}>(...).");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalGrainStorage(
            name,
            services.GetProviderClusterOptions(name).Value.ServiceId,
            new EntityFrameworkCoreGrainStateStore<TContext>(contextFactory, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<RelationalGrainStorage>>(), options.IsDuplicateKeyError),
            options.GrainStorageSerializer,
            services.GetRequiredService<IActivatorProvider>(),
            options.DeleteStateOnClear,
            options.MaxPayloadSize,
            options.InitStage,
            timeProvider,
            services.GetRequiredService<ILogger<RelationalGrainStorage>>());
    }
}
