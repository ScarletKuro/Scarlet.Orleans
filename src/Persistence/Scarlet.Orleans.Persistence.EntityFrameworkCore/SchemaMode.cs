namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// What the provider does with the database schema when the silo starts.
/// </summary>
public enum SchemaMode
{
    /// <summary>
    /// No checks. The schema is assumed to be current.
    /// </summary>
    None,

    /// <summary>
    /// Checks the schema history and fails startup, listing the scripts to run, if the schema is missing or outdated.
    /// </summary>
    Validate,

    /// <summary>
    /// Creates the schema or applies pending migration scripts. Needs DDL permissions. Only for Orleans tables that are
    /// not managed by the application's EF Core migrations.
    /// </summary>
    Apply,
}
