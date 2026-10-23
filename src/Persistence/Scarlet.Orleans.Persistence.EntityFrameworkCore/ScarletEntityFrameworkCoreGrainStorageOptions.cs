using System.Data.Common;
using Orleans;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// Options for EF Core grain storage.
/// </summary>
/// <remarks>
/// Retries of transient database errors come from the execution strategy configured on the context, for example
/// <c>EnableRetryOnFailure</c>. Inserts and updates are safe to repeat; deletes run exactly once.
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
    /// rule is not set, a failed insert is reported as a duplicate key if a row exists under the key afterwards, which
    /// misjudges a conflicting row deleted in between.
    /// </remarks>
    public Func<DbException, bool>? IsDuplicateKeyError { get; set; }

    /// <inheritdoc/>
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = null!;

    /// <summary>
    /// The silo lifecycle stage at which the schema is checked.
    /// </summary>
    public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
}
