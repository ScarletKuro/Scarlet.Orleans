using System.Data.Common;
using Orleans;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.AdoNet;

/// <summary>
/// Options for ADO.NET grain storage.
/// </summary>
public sealed class ScarletAdoNetGrainStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    /// The data source to store grain state in. When <see langword="null"/>, the <see cref="DbDataSource"/> registered in
    /// dependency injection is used, keyed by <see cref="DataSourceServiceKey"/> if that is set.
    /// </summary>
    /// <remarks>
    /// The driver is whatever created the data source, for example <c>NpgsqlDataSource.Create(...)</c> or
    /// <c>SqlClientFactory.Instance.CreateDataSource(...)</c>. MySQL works with both MySqlConnector and Oracle's MySql.Data.
    /// Nothing is loaded by name or reflection.
    /// </remarks>
    public DbDataSource? DataSource { get; set; }

    /// <summary>
    /// The service key of the <see cref="DbDataSource"/> to resolve when <see cref="DataSource"/> is not set.
    /// </summary>
    public object? DataSourceServiceKey { get; set; }

    /// <summary>
    /// The database engine: <c>SqlServer</c>, <c>PostgreSql</c>, <c>MySql</c>, <c>MariaDb</c> or <c>Sqlite</c>.
    /// When <see langword="null"/>, it is inferred from the driver; MariaDB must be set explicitly.
    /// </summary>
    public string? Engine { get; set; }

    /// <summary>
    /// What to do with the schema at startup. Defaults to <see cref="SchemaMode.Validate"/>.
    /// </summary>
    public SchemaMode SchemaMode { get; set; } = SchemaMode.Validate;

    /// <summary>
    /// Whether clearing state deletes the row, rather than keeping it with no payload and a new ETag.
    /// </summary>
    public bool DeleteStateOnClear { get; set; }

    /// <inheritdoc/>
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = null!;

    /// <summary>
    /// The silo lifecycle stage at which the schema is checked.
    /// </summary>
    public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
}
