using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Messaging;
using Scarlet.Orleans.Clustering;
using Scarlet.Orleans.Clustering.EntityFrameworkCore;

namespace Orleans.Hosting;

/// <summary>
/// Registers EF Core clustering on silos and clients.
/// </summary>
public static class ScarletEntityFrameworkCoreClusteringExtensions
{
    /// <summary>
    /// Stores the cluster's membership in a relational database, through <typeparamref name="TContext"/>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansClusteringModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static ISiloBuilder UseScarletEntityFrameworkCoreClustering<TContext>(this ISiloBuilder builder, Action<ScarletEntityFrameworkCoreClusteringOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        AddOptions(builder.Services, configure);
        builder.Services.AddSingleton<IMembershipTable>(services =>
        {
            var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;
            return new RelationalMembershipTable(
                services.GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId,
                CreateStore<TContext>(services, timeProvider),
                timeProvider,
                services.GetRequiredService<ILogger<RelationalMembershipTable>>());
        });
        return builder;
    }

    /// <summary>
    /// Finds the cluster's gateways in the relational database its silos store their membership in, through
    /// <typeparamref name="TContext"/>. The client never changes the schema.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansClusteringModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static IClientBuilder UseScarletEntityFrameworkCoreClustering<TContext>(this IClientBuilder builder, Action<ScarletEntityFrameworkCoreClusteringOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        AddOptions(builder.Services, configure);
        builder.Services.AddSingleton<IGatewayListProvider>(services => new RelationalGatewayListProvider(
            services.GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId,
            CreateStore<TContext>(services, services.GetService<TimeProvider>() ?? TimeProvider.System),
            services.GetRequiredService<IOptions<GatewayOptions>>().Value.GatewayListRefreshPeriod));
        return builder;
    }

    private static void AddOptions(IServiceCollection services, Action<ScarletEntityFrameworkCoreClusteringOptions>? configure)
    {
        var options = services.AddOptions<ScarletEntityFrameworkCoreClusteringOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }
    }

    private static EntityFrameworkCoreMembershipStore<TContext> CreateStore<TContext>(IServiceProvider services, TimeProvider timeProvider)
        where TContext : DbContext
    {
        var options = services.GetRequiredService<IOptions<ScarletEntityFrameworkCoreClusteringOptions>>().Value;
        var contextFactory = services.GetService<IDbContextFactory<TContext>>()
            ?? throw new InvalidOperationException(
                $"Clustering needs IDbContextFactory<{typeof(TContext).Name}>. Register it with services.AddDbContextFactory<{typeof(TContext).Name}>(...).");
        return new EntityFrameworkCoreMembershipStore<TContext>(
            contextFactory,
            options.SchemaMode,
            timeProvider,
            services.GetRequiredService<ILogger<RelationalMembershipTable>>(),
            options.IsDuplicateKeyError);
    }
}
