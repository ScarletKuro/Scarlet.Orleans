using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Reminders;
using Scarlet.Orleans.Reminders.AdoNet;

namespace Orleans.Hosting;

/// <summary>
/// Registers the ADO.NET reminder table.
/// </summary>
public static class ScarletAdoNetReminderExtensions
{
    /// <summary>
    /// Adds the reminder service, storing reminders through ADO.NET.
    /// </summary>
    public static ISiloBuilder UseScarletAdoNetReminderService(this ISiloBuilder builder, Action<ScarletAdoNetReminderTableOptions>? configure = null) =>
        builder.ConfigureServices(services => services.UseScarletAdoNetReminderService(configure));

    /// <summary>
    /// Adds the reminder service, storing reminders through ADO.NET.
    /// </summary>
    public static IServiceCollection UseScarletAdoNetReminderService(this IServiceCollection services, Action<ScarletAdoNetReminderTableOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = services.AddOptions<ScarletAdoNetReminderTableOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.Validate(
            static o => o.Engine is null || RelationalEngines.TryParse(o.Engine, out _),
            $"{nameof(ScarletAdoNetReminderTableOptions.Engine)} must be one of: {string.Join(", ", RelationalEngines.Names)}.");
        services.AddReminders();
        services.AddSingleton<IReminderTable>(Create);
        return services;
    }

    private static RelationalReminderTable Create(IServiceProvider services)
    {
        var options = services.GetRequiredService<IOptions<ScarletAdoNetReminderTableOptions>>().Value;
        var dataSource = options.DataSource
            ?? (options.DataSourceServiceKey is { } key
                ? services.GetRequiredKeyedService<DbDataSource>(key)
                : services.GetService<DbDataSource>())
            ?? throw new InvalidOperationException(
                $"The reminder table has no data source. Set {nameof(ScarletAdoNetReminderTableOptions)}.{nameof(ScarletAdoNetReminderTableOptions.DataSource)} or register a {nameof(DbDataSource)}.");
        var timeProvider = services.GetService<TimeProvider>() ?? TimeProvider.System;

        return new RelationalReminderTable(
            services.GetRequiredService<IOptions<ClusterOptions>>().Value.ServiceId,
            new AdoNetReminderStore(dataSource, options.Engine, options.SchemaMode, timeProvider, services.GetRequiredService<ILogger<AdoNetReminderStore>>()),
            timeProvider,
            services.GetRequiredService<ILogger<RelationalReminderTable>>());
    }
}
