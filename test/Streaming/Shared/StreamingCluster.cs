using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.TestingHost;
using Scarlet.Orleans.Persistence.AdoNet;
using Scarlet.Orleans.Streaming.AdoNet;

namespace Scarlet.Orleans.Streaming.Tests;

/// <summary>
/// An in-process cluster whose stream provider queues in <paramref name="database"/>, with pub-sub state in Scarlet
/// grain storage on the same database.
/// </summary>
public static class StreamingCluster
{
    /// <summary>Four queues, so that both silos of a two-silo cluster own some.</summary>
    public const int QueueCount = 4;

    public static async Task<InProcessTestCluster> StartAsync(TestDatabase database, int silos, TimeSpan visibilityTimeout)
    {
        var engine = database.Engine == "MariaDb" ? "MariaDb" : null;
        var builder = new InProcessTestClusterBuilder((short)silos);
        builder.ConfigureHost(host => host.Logging.AddProvider(new WarningLog()));
        builder.ConfigureSilo((_, silo) =>
        {
            silo.Services.Configure<ExceptionSerializationOptions>(SerializeDriverExceptions);
            silo.AddScarletAdoNetGrainStorage("PubSubStore", options =>
            {
                options.DataSource = database.DataSource;
                options.Engine = engine;
                options.SchemaMode = Persistence.AdoNet.SchemaMode.Apply;
            });
            silo.AddScarletAdoNetStreams(StreamNamespaces.Provider, Configure, streams =>
            {
                streams.Configure<HashRingStreamQueueMapperOptions>(o => o.Configure(q => q.TotalQueueCount = QueueCount));
                streams.ConfigurePullingAgent(o => o.Configure(agent => agent.MaxEventDeliveryTime = TimeSpan.FromSeconds(2)));
            });
        });
        builder.ConfigureClient(client =>
        {
            client.Services.Configure<ExceptionSerializationOptions>(SerializeDriverExceptions);
            client.AddScarletAdoNetStreams(StreamNamespaces.Provider, Configure, streams =>
                streams.Configure<HashRingStreamQueueMapperOptions>(o => o.Configure(q => q.TotalQueueCount = QueueCount)));
        });

        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;

        void Configure(ScarletAdoNetStreamOptions options)
        {
            options.DataSource = database.DataSource;
            options.Engine = engine;
            options.SchemaMode = AdoNet.SchemaMode.Apply;
            options.VisibilityTimeout = visibilityTimeout;
        }
    }

    private static void SerializeDriverExceptions(ExceptionSerializationOptions options)
    {
        options.SupportedNamespacePrefixes.Add("MySql");
        options.SupportedNamespacePrefixes.Add("Npgsql");
    }
}

/// <summary>A two-silo cluster shared by an engine's end-to-end tests, which isolate themselves by stream ID.</summary>
public abstract class StreamingClusterFixture(DatabaseFixture database) : IAsyncLifetime
{
    private InProcessTestCluster? cluster;

    public DatabaseFixture Database { get; } = database;

    public InProcessTestCluster Cluster
    {
        get
        {
            Database.Require();
            return cluster!;
        }
    }

    public async ValueTask InitializeAsync()
    {
        await Database.InitializeAsync();
        if (Database.IsAvailable)
        {
            cluster = await StreamingCluster.StartAsync(Database.Database, silos: 2, visibilityTimeout: TimeSpan.FromMinutes(1));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (cluster is not null)
        {
            await cluster.DisposeAsync();
        }

        await Database.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}
