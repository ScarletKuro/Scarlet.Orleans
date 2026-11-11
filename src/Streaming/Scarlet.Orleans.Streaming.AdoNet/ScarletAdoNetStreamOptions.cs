using System.Data.Common;

namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// Options of a relational stream provider.
/// </summary>
public sealed class ScarletAdoNetStreamOptions
{
    /// <summary>
    /// The data source to use. If not set, the provider resolves a <see cref="DbDataSource"/> from dependency injection,
    /// keyed by <see cref="DataSourceServiceKey"/> if that is set.
    /// </summary>
    public DbDataSource? DataSource { get; set; }

    /// <summary>The service key of the <see cref="DbDataSource"/> to resolve, when <see cref="DataSource"/> is not set.</summary>
    public object? DataSourceServiceKey { get; set; }

    /// <summary>
    /// The engine: <c>SqlServer</c>, <c>PostgreSql</c>, <c>MySql</c>, <c>MariaDb</c> or <c>Sqlite</c>. Inferred from the
    /// driver if not set; MariaDB, which shares its driver with MySQL, must be set.
    /// </summary>
    public string? Engine { get; set; }

    /// <summary>What to do with the schema at startup.</summary>
    public SchemaMode SchemaMode { get; set; } = SchemaMode.Validate;

    /// <summary>How many times a message is dequeued before it is moved to the dead letters.</summary>
    public int MaxAttempts { get; set; } = 5;

    /// <summary>
    /// How long a dequeued message stays hidden from other dequeues. A message not confirmed within it is delivered
    /// again, so it should outlast the time a message spends in the silo's cache.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How long a message can wait to be delivered before it expires and is moved to the dead letters.</summary>
    public TimeSpan ExpiryTimeout { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>How long dead letters are kept before they are deleted.</summary>
    public TimeSpan DeadLetterRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>How often each queue moves expired and exhausted messages to the dead letters, and deletes old dead letters.</summary>
    public TimeSpan EvictionInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>The most messages or dead letters one eviction moves or deletes, at most 1,000.</summary>
    public int EvictionBatchSize { get; set; } = 1000;
}
