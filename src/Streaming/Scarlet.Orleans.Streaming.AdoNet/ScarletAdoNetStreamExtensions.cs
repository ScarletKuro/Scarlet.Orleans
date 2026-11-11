using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.Serialization;
using Orleans.Streams;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Streaming;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Orleans.Hosting;

/// <summary>
/// Adds relational persistent streams to silos and clients.
/// </summary>
public static class ScarletAdoNetStreamExtensions
{
    /// <summary>
    /// Adds a persistent stream provider named <paramref name="name"/> that queues its messages in a relational
    /// database.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="name">The stream provider's name.</param>
    /// <param name="configure">Configures the provider's options.</param>
    /// <param name="configureStreams">Configures Orleans' side of the provider: its cache, its queue count
    /// (<see cref="HashRingStreamQueueMapperOptions"/>), its pulling agents and its pub-sub.</param>
    public static ISiloBuilder AddScarletAdoNetStreams(
        this ISiloBuilder builder,
        string name,
        Action<ScarletAdoNetStreamOptions>? configure = null,
        Action<ISiloPersistentStreamConfigurator>? configureStreams = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        AddOptions(builder.Services, name, configure);
        var streams = new SiloPersistentStreamConfigurator(name, configureServices => configureServices(builder.Services), Create);
        configureStreams?.Invoke(streams);
        return builder;
    }

    /// <summary>
    /// Adds a persistent stream provider named <paramref name="name"/> to a client, which enqueues its messages in a
    /// relational database. It must match the silos' provider of the same name.
    /// </summary>
    /// <param name="builder">The client builder.</param>
    /// <param name="name">The stream provider's name.</param>
    /// <param name="configure">Configures the provider's options.</param>
    /// <param name="configureStreams">Configures Orleans' side of the provider, such as its queue count
    /// (<see cref="HashRingStreamQueueMapperOptions"/>), which must match the silos'.</param>
    public static IClientBuilder AddScarletAdoNetStreams(
        this IClientBuilder builder,
        string name,
        Action<ScarletAdoNetStreamOptions>? configure = null,
        Action<IClusterClientPersistentStreamConfigurator>? configureStreams = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        AddOptions(builder.Services, name, configure);
        var streams = new ClusterClientPersistentStreamConfigurator(name, builder, Create);
        configureStreams?.Invoke(streams);
        return builder;
    }

    private static void AddOptions(IServiceCollection services, string name, Action<ScarletAdoNetStreamOptions>? configure)
    {
        var options = services.AddOptions<ScarletAdoNetStreamOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options
            .Validate(static o => o.Engine is null || RelationalEngines.TryParse(o.Engine, out _), $"{nameof(ScarletAdoNetStreamOptions.Engine)} must be one of: {string.Join(", ", RelationalEngines.Names)}.")
            .Validate(static o => o.MaxAttempts > 0, $"{nameof(ScarletAdoNetStreamOptions.MaxAttempts)} must be positive.")
            .Validate(static o => o.VisibilityTimeout > TimeSpan.Zero, $"{nameof(ScarletAdoNetStreamOptions.VisibilityTimeout)} must be positive.")
            .Validate(static o => o.ExpiryTimeout > TimeSpan.Zero, $"{nameof(ScarletAdoNetStreamOptions.ExpiryTimeout)} must be positive.")
            .Validate(static o => o.DeadLetterRetention >= TimeSpan.Zero, $"{nameof(ScarletAdoNetStreamOptions.DeadLetterRetention)} must not be negative.")
            .Validate(static o => o.EvictionInterval > TimeSpan.Zero, $"{nameof(ScarletAdoNetStreamOptions.EvictionInterval)} must be positive.")
            .Validate(
                static o => o.EvictionBatchSize is > 0 and <= AdoNetStreamQueueStore.MaxEvictionBatch,
                $"{nameof(ScarletAdoNetStreamOptions.EvictionBatchSize)} must be between 1 and {AdoNetStreamQueueStore.MaxEvictionBatch}.");
    }

    private static RelationalQueueAdapterFactory Create(IServiceProvider services, string name)
    {
        var options = services.GetRequiredService<IOptionsMonitor<ScarletAdoNetStreamOptions>>().Get(name);
        var dataSource = options.DataSource
            ?? (options.DataSourceServiceKey is { } key
                ? services.GetRequiredKeyedService<DbDataSource>(key)
                : services.GetService<DbDataSource>())
            ?? throw new InvalidOperationException(
                $"Stream provider \"{name}\" has no data source. Set {nameof(ScarletAdoNetStreamOptions)}.{nameof(ScarletAdoNetStreamOptions.DataSource)} or register a {nameof(DbDataSource)}.");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();

        return new RelationalQueueAdapterFactory(
            name,
            services.GetProviderClusterOptions(name).Value.ServiceId,
            new AdoNetStreamQueueStore(dataSource, options.Engine, options.SchemaMode, timeProvider, loggerFactory.CreateLogger<AdoNetStreamQueueStore>()),
            services.GetRequiredService<Serializer>(),
            options,
            services.GetRequiredService<IOptionsMonitor<HashRingStreamQueueMapperOptions>>().Get(name),
            services.GetRequiredService<IOptionsMonitor<SimpleQueueCacheOptions>>().Get(name),
            timeProvider,
            loggerFactory);
    }
}
