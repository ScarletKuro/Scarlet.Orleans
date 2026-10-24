using System.Data.Common;

namespace Scarlet.Orleans.Reminders.EntityFrameworkCore;

/// <summary>
/// Options for the EF Core reminder table.
/// </summary>
/// <remarks>
/// Writes ignore any retrying execution strategy configured on the context, such as <c>EnableRetryOnFailure</c>: after
/// a connection failure a write's outcome is unknown. A write is repeated only when the server reports that the attempt
/// was not applied, such as a deadlock; other failures surface to Orleans, whose reminder service retries. Reads use
/// the context's strategy.
/// </remarks>
public sealed class ScarletEntityFrameworkCoreReminderTableOptions
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
    /// When set, this rule alone decides, so it must match only errors that report a duplicate primary or unique key. A
    /// duplicate key it does not recognise surfaces as the driver's error. Any other error it matches is hidden: the
    /// upsert updates the existing row instead, and the original error is lost. For a driver that is not recognised and
    /// this rule is not set, the provider checks for a row under the key after the failure: if one exists, the upsert
    /// updates it; if none does, the driver's error surfaces.
    /// </remarks>
    public Func<DbException, bool>? IsDuplicateKeyError { get; set; }
}
