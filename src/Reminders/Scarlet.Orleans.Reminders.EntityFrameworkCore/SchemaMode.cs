namespace Scarlet.Orleans.Reminders.EntityFrameworkCore;

/// <summary>
/// What the provider checks about the database schema when the silo starts.
/// </summary>
/// <remarks>
/// The application's EF Core migrations create and evolve the Orleans tables, after
/// <c>modelBuilder.ApplyOrleansRemindersModel()</c> and <c>migrationBuilder.ApplyOrleansRemindersSchema(version)</c>.
/// The provider never changes the schema itself.
/// </remarks>
public enum SchemaMode
{
    /// <summary>
    /// No checks. The schema is assumed to be current.
    /// </summary>
    None,

    /// <summary>
    /// Checks the schema history and fails startup if the recorded schema version is missing or outside the range this
    /// package supports.
    /// </summary>
    Validate,
}
