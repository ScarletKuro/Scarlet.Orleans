using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Scarlet.Orleans.Reminders;
using Scarlet.Orleans.Reminders.EntityFrameworkCore;

namespace Orleans.Hosting;

/// <summary>
/// Registers the EF Core reminder table.
/// </summary>
public static class ScarletEntityFrameworkCoreReminderExtensions
{
    /// <summary>
    /// Adds the reminder service, storing reminders through <typeparamref name="TContext"/>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansRemindersModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static ISiloBuilder UseScarletEntityFrameworkCoreReminderService<TContext>(this ISiloBuilder builder, Action<ScarletEntityFrameworkCoreReminderTableOptions>? configure = null)
        where TContext : DbContext =>
        builder.ConfigureServices(services => services.UseScarletEntityFrameworkCoreReminderService<TContext>(configure));

    /// <summary>
    /// Adds the reminder service, storing reminders through <typeparamref name="TContext"/>.
    /// </summary>
    /// <typeparam name="TContext">A context whose model calls <c>ApplyOrleansRemindersModel()</c>, registered with <c>AddDbContextFactory</c>.</typeparam>
    public static IServiceCollection UseScarletEntityFrameworkCoreReminderService<TContext>(this IServiceCollection services, Action<ScarletEntityFrameworkCoreReminderTableOptions>? configure = null)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = services.AddOptions<ScarletEntityFrameworkCoreReminderTableOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        services.AddReminders();
        services.AddSingleton<IReminderTable>(Create<TContext>);
        return services;
    }

    private static RelationalReminderTable Create<TContext>(IServiceProvider services)
        where TContext : DbContext
    {
        var options = services.GetRequiredService<IOptions<ScarletEntityFrameworkCoreReminderTableOptions>>().Value;
        var contextFactory = services.GetService<IDbContextFactory<TContext>>()
            ?? throw new InvalidOperationException(
                $"The reminder table needs IDbContextFactory<{typeof(TContext).Name}>. Register it with services.AddDbContextFactory<{typeof(TContext).Name}>(...).");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalReminderTable(
            services.GetRequiredService<IOptions<ClusterOptions>>().Value.ServiceId,
            new EntityFrameworkCoreReminderStore<TContext>(contextFactory, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<RelationalReminderTable>>(), options.IsDuplicateKeyError),
            timeProvider,
            services.GetRequiredService<ILogger<RelationalReminderTable>>());
    }
}
