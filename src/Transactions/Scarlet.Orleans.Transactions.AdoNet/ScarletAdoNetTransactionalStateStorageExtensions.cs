using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration.Overrides;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.Transactions.Abstractions;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Transactions;
using Scarlet.Orleans.Transactions.AdoNet;

namespace Orleans.Hosting;

/// <summary>
/// Registers ADO.NET transactional state storage.
/// </summary>
public static class ScarletAdoNetTransactionalStateStorageExtensions
{
    /// <summary>
    /// Adds ADO.NET transactional state storage as the default, used by <c>[TransactionalState]</c> without a storage name.
    /// </summary>
    /// <remarks>Transactions also need the application's <c>UseTransactions()</c>.</remarks>
    public static ISiloBuilder AddScarletAdoNetTransactionalStateStorageAsDefault(this ISiloBuilder builder, Action<ScarletAdoNetTransactionalStateStorageOptions>? configure = null) =>
        builder.AddScarletAdoNetTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configure);

    /// <summary>
    /// Adds ADO.NET transactional state storage named <paramref name="name"/>, used by
    /// <c>[TransactionalState("state", "name")]</c>.
    /// </summary>
    /// <remarks>Transactions also need the application's <c>UseTransactions()</c>.</remarks>
    public static ISiloBuilder AddScarletAdoNetTransactionalStateStorage(this ISiloBuilder builder, string name, Action<ScarletAdoNetTransactionalStateStorageOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var options = builder.Services.AddOptions<ScarletAdoNetTransactionalStateStorageOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.Validate(
            static o => o.Engine is null || RelationalEngines.TryParse(o.Engine, out _),
            $"{nameof(ScarletAdoNetTransactionalStateStorageOptions.Engine)} must be one of: {string.Join(", ", RelationalEngines.Names)}.");
        builder.Services.AddTransient<IPostConfigureOptions<ScarletAdoNetTransactionalStateStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<ScarletAdoNetTransactionalStateStorageOptions>>();

        // Orleans resolves a storage name through a keyed factory, and no name through the unkeyed one, which is null
        // unless a default is added, so Orleans can fall back to the default grain storage.
        builder.Services.TryAddSingleton<ITransactionalStateStorageFactory>(static services => services.GetKeyedService<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME)!);
        builder.Services.AddKeyedSingleton<ITransactionalStateStorageFactory>(name, (services, _) => Create(services, name));
        builder.Services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(services =>
            (RelationalTransactionalStateStorageFactory)services.GetRequiredKeyedService<ITransactionalStateStorageFactory>(name));
        return builder;
    }

    private static RelationalTransactionalStateStorageFactory Create(IServiceProvider services, string name)
    {
        var options = services.GetRequiredService<IOptionsMonitor<ScarletAdoNetTransactionalStateStorageOptions>>().Get(name);
        var dataSource = options.DataSource
            ?? (options.DataSourceServiceKey is { } key
                ? services.GetRequiredKeyedService<DbDataSource>(key)
                : services.GetService<DbDataSource>())
            ?? throw new InvalidOperationException(
                $"Transactional state storage \"{name}\" has no data source. Set {nameof(ScarletAdoNetTransactionalStateStorageOptions)}.{nameof(ScarletAdoNetTransactionalStateStorageOptions.DataSource)} or register a {nameof(DbDataSource)}.");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalTransactionalStateStorageFactory(
            name,
            services.GetProviderClusterOptions(name).Value.ServiceId,
            new AdoNetTransactionalStateStore(dataSource, options.Engine, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<AdoNetTransactionalStateStore>>()),
            options.GrainStorageSerializer,
            services.GetRequiredService<Serializer>(),
            options.InitStage,
            timeProvider,
            services.GetRequiredService<ILoggerFactory>());
    }
}
