using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory;
using Orleans.Runtime.Hosting;
using Scarlet.Orleans.GrainDirectory;
using Scarlet.Orleans.GrainDirectory.EntityFrameworkCore;

namespace Orleans.Hosting;

/// <summary>
/// Registers EF Core grain directories.
/// </summary>
public static class ScarletEntityFrameworkCoreGrainDirectoryExtensions
{
    /// <summary>
    /// Adds an EF Core grain directory on <typeparamref name="TContext"/> as the default directory, used by grains without
    /// a <see cref="GrainDirectoryAttribute"/>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansGrainDirectoryModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static ISiloBuilder UseScarletEntityFrameworkCoreGrainDirectoryAsDefault<TContext>(this ISiloBuilder builder, Action<ScarletEntityFrameworkCoreGrainDirectoryOptions>? configure = null)
        where TContext : DbContext =>
        builder.AddScarletEntityFrameworkCoreGrainDirectory<TContext>(GrainDirectoryAttribute.DEFAULT_GRAIN_DIRECTORY, configure);

    /// <summary>
    /// Adds an EF Core grain directory on <typeparamref name="TContext"/> named <paramref name="name"/>, used by grains
    /// with <c>[GrainDirectory("name")]</c>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansGrainDirectoryModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static ISiloBuilder AddScarletEntityFrameworkCoreGrainDirectory<TContext>(this ISiloBuilder builder, string name, Action<ScarletEntityFrameworkCoreGrainDirectoryOptions>? configure = null)
        where TContext : DbContext =>
        builder.ConfigureServices(services => services.AddScarletEntityFrameworkCoreGrainDirectory<TContext>(name, configure));

    /// <summary>
    /// Adds an EF Core grain directory on <typeparamref name="TContext"/> named <paramref name="name"/>, used by grains
    /// with <c>[GrainDirectory("name")]</c>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansGrainDirectoryModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static IServiceCollection AddScarletEntityFrameworkCoreGrainDirectory<TContext>(this IServiceCollection services, string name, Action<ScarletEntityFrameworkCoreGrainDirectoryOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var options = services.AddOptions<ScarletEntityFrameworkCoreGrainDirectoryOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        return services.AddGrainDirectory(name, Create<TContext>);
    }

    private static RelationalGrainDirectory Create<TContext>(IServiceProvider services, string name)
        where TContext : DbContext
    {
        var options = services.GetRequiredService<IOptionsMonitor<ScarletEntityFrameworkCoreGrainDirectoryOptions>>().Get(name);
        var contextFactory = services.GetService<IDbContextFactory<TContext>>()
            ?? throw new InvalidOperationException(
                $"Grain directory \"{name}\" needs IDbContextFactory<{typeof(TContext).Name}>. Register it with services.AddDbContextFactory<{typeof(TContext).Name}>(...).");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalGrainDirectory(
            name,
            services.GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId,
            new EntityFrameworkCoreGrainDirectoryStore<TContext>(contextFactory, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<RelationalGrainDirectory>>(), options.IsDuplicateKeyError),
            options.InitStage,
            timeProvider,
            services.GetRequiredService<ILogger<RelationalGrainDirectory>>());
    }
}
