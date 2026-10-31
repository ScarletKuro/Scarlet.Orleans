using System.Data.Common;

namespace Scarlet.Orleans.Clustering.EntityFrameworkCore;

/// <summary>
/// Options for EF Core clustering, on silos and on clients.
/// </summary>
/// <remarks>
/// Writes ignore any retrying execution strategy configured on the context, such as <c>EnableRetryOnFailure</c>: after
/// a connection failure a write's outcome is unknown. A write is repeated only when the server reports that the attempt
/// was not applied, such as a deadlock; other failures surface to Orleans, which reads the table again. Reads use the
/// context's strategy.
/// </remarks>
public sealed class ScarletEntityFrameworkCoreClusteringOptions
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
    /// Only the creation of a cluster's version row, when silos start, can meet a duplicate key; member rows are checked
    /// under the version row's lock instead. When set, this rule alone decides, so it must match only errors that report
    /// a duplicate primary or unique key. For a driver that is not recognised and this rule is not set, the provider
    /// checks for the version row after the failure: if it exists, startup continues; if not, the driver's error surfaces.
    /// </remarks>
    public Func<DbException, bool>? IsDuplicateKeyError { get; set; }
}
