using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Messaging;
using Scarlet.Orleans.Clustering;
using Scarlet.Orleans.Clustering.AdoNet;
using Scarlet.Orleans.Relational;

namespace Orleans.Hosting;

/// <summary>
/// Registers ADO.NET clustering on silos and clients.
/// </summary>
public static class ScarletAdoNetClusteringExtensions
{
    /// <summary>
    /// Stores the cluster's membership in a relational database, through ADO.NET.
    /// </summary>
    public static ISiloBuilder UseScarletAdoNetClustering(this ISiloBuilder builder, Action<ScarletAdoNetClusteringOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        AddOptions(builder.Services, configure);
        builder.Services.AddSingleton<IMembershipTable>(services =>
        {
            var options = services.GetRequiredService<IOptions<ScarletAdoNetClusteringOptions>>().Value;
            var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;
            return new RelationalMembershipTable(
                services.GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId,
                CreateStore(services, options, timeProvider),
                timeProvider,
                services.GetRequiredService<ILogger<RelationalMembershipTable>>());
        });
        return builder;
    }

    /// <summary>
    /// Finds the cluster's gateways in the relational database its silos store their membership in, through ADO.NET.
    /// </summary>
    /// <remarks>
    /// The client never changes the schema, so <see cref="ScarletAdoNetClusteringOptions.SchemaMode"/> must be
    /// <see cref="SchemaMode.Validate"/> or <see cref="SchemaMode.None"/>.
    /// </remarks>
    public static IClientBuilder UseScarletAdoNetClustering(this IClientBuilder builder, Action<ScarletAdoNetClusteringOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        AddOptions(builder.Services, configure).Validate(
            static o => o.SchemaMode != SchemaMode.Apply,
            $"Clients never change the schema. Set {nameof(ScarletAdoNetClusteringOptions.SchemaMode)} to {nameof(SchemaMode.Validate)} or {nameof(SchemaMode.None)}, and apply the schema from a silo or by hand.");
        builder.Services.AddSingleton<IGatewayListProvider>(services =>
        {
            var options = services.GetRequiredService<IOptions<ScarletAdoNetClusteringOptions>>().Value;
            return new RelationalGatewayListProvider(
                services.GetRequiredService<IOptions<ClusterOptions>>().Value.ClusterId,
                CreateStore(services, options, services.GetService<TimeProvider>() ?? TimeProvider.System),
                services.GetRequiredService<IOptions<GatewayOptions>>().Value.GatewayListRefreshPeriod);
        });
        return builder;
    }

    private static OptionsBuilder<ScarletAdoNetClusteringOptions> AddOptions(IServiceCollection services, Action<ScarletAdoNetClusteringOptions>? configure)
    {
        var options = services.AddOptions<ScarletAdoNetClusteringOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        return options.Validate(
            static o => o.Engine is null || RelationalEngines.TryParse(o.Engine, out _),
            $"{nameof(ScarletAdoNetClusteringOptions.Engine)} must be one of: {string.Join(", ", RelationalEngines.Names)}.");
    }

    private static AdoNetMembershipStore CreateStore(IServiceProvider services, ScarletAdoNetClusteringOptions options, TimeProvider timeProvider)
    {
        var dataSource = options.DataSource
            ?? (options.DataSourceServiceKey is { } key
                ? services.GetRequiredKeyedService<DbDataSource>(key)
                : services.GetService<DbDataSource>())
            ?? throw new InvalidOperationException(
                $"Clustering has no data source. Set {nameof(ScarletAdoNetClusteringOptions)}.{nameof(ScarletAdoNetClusteringOptions.DataSource)} or register a {nameof(DbDataSource)}.");
        return new AdoNetMembershipStore(dataSource, options.Engine, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<AdoNetMembershipStore>>());
    }
}
