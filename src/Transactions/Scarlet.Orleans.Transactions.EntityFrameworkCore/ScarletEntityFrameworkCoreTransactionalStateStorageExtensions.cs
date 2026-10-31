using Microsoft.EntityFrameworkCore;
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
using Scarlet.Orleans.Transactions;
using Scarlet.Orleans.Transactions.EntityFrameworkCore;

namespace Orleans.Hosting;

/// <summary>
/// Registers EF Core transactional state storage.
/// </summary>
public static class ScarletEntityFrameworkCoreTransactionalStateStorageExtensions
{
    /// <summary>
    /// Adds EF Core transactional state storage on <typeparamref name="TContext"/> as the default, used by
    /// <c>[TransactionalState]</c> without a storage name.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansTransactionsModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    /// <remarks>Transactions also need the application's <c>UseTransactions()</c>.</remarks>
    public static ISiloBuilder AddScarletEntityFrameworkCoreTransactionalStateStorageAsDefault<TContext>(this ISiloBuilder builder, Action<ScarletEntityFrameworkCoreTransactionalStateStorageOptions>? configure = null)
        where TContext : DbContext =>
        builder.AddScarletEntityFrameworkCoreTransactionalStateStorage<TContext>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configure);

    /// <summary>
    /// Adds EF Core transactional state storage on <typeparamref name="TContext"/> named <paramref name="name"/>, used by
    /// <c>[TransactionalState("state", "name")]</c>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansTransactionsModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    /// <remarks>Transactions also need the application's <c>UseTransactions()</c>.</remarks>
    public static ISiloBuilder AddScarletEntityFrameworkCoreTransactionalStateStorage<TContext>(this ISiloBuilder builder, string name, Action<ScarletEntityFrameworkCoreTransactionalStateStorageOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var options = builder.Services.AddOptions<ScarletEntityFrameworkCoreTransactionalStateStorageOptions>(name);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        builder.Services.AddTransient<IPostConfigureOptions<ScarletEntityFrameworkCoreTransactionalStateStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<ScarletEntityFrameworkCoreTransactionalStateStorageOptions>>();

        // Orleans resolves a storage name through a keyed factory, and no name through the unkeyed one, which is null
        // unless a default is added, so Orleans can fall back to the default grain storage.
        builder.Services.TryAddSingleton<ITransactionalStateStorageFactory>(static services => services.GetKeyedService<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME)!);
        builder.Services.AddKeyedSingleton<ITransactionalStateStorageFactory>(name, (services, _) => Create<TContext>(services, name));
        builder.Services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(services =>
            (RelationalTransactionalStateStorageFactory)services.GetRequiredKeyedService<ITransactionalStateStorageFactory>(name));
        return builder;
    }

    private static RelationalTransactionalStateStorageFactory Create<TContext>(IServiceProvider services, string name)
        where TContext : DbContext
    {
        var options = services.GetRequiredService<IOptionsMonitor<ScarletEntityFrameworkCoreTransactionalStateStorageOptions>>().Get(name);
        var contextFactory = services.GetService<IDbContextFactory<TContext>>()
            ?? throw new InvalidOperationException(
                $"Transactional state storage \"{name}\" needs IDbContextFactory<{typeof(TContext).Name}>. Register it with services.AddDbContextFactory<{typeof(TContext).Name}>(...).");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalTransactionalStateStorageFactory(
            name,
            services.GetProviderClusterOptions(name).Value.ServiceId,
            new EntityFrameworkCoreTransactionalStateStore<TContext>(contextFactory, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<RelationalTransactionalStateStorageFactory>>(), options.IsDuplicateKeyError),
            options.GrainStorageSerializer,
            services.GetRequiredService<Serializer>(),
            options.InitStage,
            timeProvider,
            services.GetRequiredService<ILoggerFactory>());
    }
}
