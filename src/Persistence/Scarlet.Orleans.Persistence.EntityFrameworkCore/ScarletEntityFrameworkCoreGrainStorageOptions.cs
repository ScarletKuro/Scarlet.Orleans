using System.Data.Common;
using Orleans;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// Options for EF Core grain storage.
/// </summary>
/// <remarks>
/// Writes ignore any retrying execution strategy configured on the context, such as <c>EnableRetryOnFailure</c>: after
/// a connection failure a write's outcome is unknown, and repeating it could report a committed write as a conflict.
/// A write is repeated only when the server reports that the attempt was not applied, such as a deadlock; other
/// failures surface to Orleans. Reads use the context's strategy.
/// </remarks>
public sealed class ScarletEntityFrameworkCoreGrainStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    /// What to check about the schema at startup. Defaults to <see cref="SchemaMode.Validate"/>.
    /// </summary>
    public SchemaMode SchemaMode { get; set; } = SchemaMode.Validate;

    /// <summary>
    /// Whether clearing state deletes the row, rather than keeping it with no payload and a new ETag.
    /// </summary>
    public bool DeleteStateOnClear { get; set; }

    /// <summary>
    /// Decides whether the error of a failed insert reports a duplicate key, for an ADO.NET driver whose errors are not
    /// recognised. When <see langword="null"/>, the default, the errors of Npgsql, Microsoft.Data.SqlClient,
    /// MySqlConnector, MySql.Data and Microsoft.Data.Sqlite are recognised, as is the standard SQLSTATE 23505 of any driver.
    /// </summary>
    /// <remarks>
    /// When set, this rule alone decides. A duplicate key it does not recognise surfaces as the driver's error, rather than
    /// as <see cref="global::Orleans.Storage.InconsistentStateException"/>. For a driver that is not recognised and this
    /// rule is not set, the provider checks for a row under the key after the failure: if one exists, the insert could
    /// not have succeeded and is reported as a conflict; if none does, the driver's error surfaces. A conflicting row
    /// deleted in between therefore surfaces as the driver's error rather than as a conflict, which this rule avoids.
    /// </remarks>
    public Func<DbException, bool>? IsDuplicateKeyError { get; set; }

    /// <inheritdoc/>
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = null!;

    /// <summary>
    /// The silo lifecycle stage at which the schema is checked.
    /// </summary>
    public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
}
