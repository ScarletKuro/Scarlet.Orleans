using System.Data.Common;
using Orleans;
using Orleans.Storage;

namespace Scarlet.Orleans.Transactions.EntityFrameworkCore;

/// <summary>
/// Options for EF Core transactional state storage.
/// </summary>
/// <remarks>
/// Stores ignore any retrying execution strategy configured on the context, such as <c>EnableRetryOnFailure</c>: after a
/// connection failure a store's outcome is unknown, and Orleans settles it by loading the state again. A store is
/// repeated only when the server reports that the attempt was not applied, such as a deadlock. Loads use the context's
/// strategy.
/// </remarks>
public sealed class ScarletEntityFrameworkCoreTransactionalStateStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    /// What to check about the schema at startup. Defaults to <see cref="SchemaMode.Validate"/>.
    /// </summary>
    public SchemaMode SchemaMode { get; set; } = SchemaMode.Validate;

    /// <summary>
    /// Decides whether the error of a failed insert reports a duplicate key, for an ADO.NET driver whose errors are not
    /// recognised. When <see langword="null"/>, the default, the errors of Npgsql, Microsoft.Data.SqlClient,
    /// MySqlConnector, MySql.Data and Microsoft.Data.Sqlite are recognised, as is the standard SQLSTATE 23505 of any driver.
    /// </summary>
    /// <remarks>
    /// It is consulted when the first store of a state fails to insert its row. When set, this rule alone decides, so it
    /// must match only errors that report a duplicate primary key. For a driver that is not recognised and this rule is
    /// not set, the provider checks for a row under the key after the failure: if one exists, the store is reported as a
    /// conflict; if none does, the driver's error surfaces.
    /// </remarks>
    public Func<DbException, bool>? IsDuplicateKeyError { get; set; }

    /// <summary>
    /// The serializer of the transactional states themselves. The transaction metadata always uses the Orleans
    /// serializer, because it holds grain references.
    /// </summary>
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = null!;

    /// <summary>
    /// The silo lifecycle stage at which the schema is checked.
    /// </summary>
    public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
}
