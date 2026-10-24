namespace Scarlet.Orleans.Reminders;

internal static class RemindersSchema
{
    /// <summary>The ledger key of this component.</summary>
    public const string Component = "reminders";

    public const string TableName = "orleans_reminder";

    /// <summary>
    /// The oldest schema version this code runs against. Raised only by a release that needs a newer schema.
    /// </summary>
    public const int MinimumVersion = 1;

    /// <summary>
    /// The newest schema version. The ADO.NET flavour ships one script per version and engine; a test keeps them in step.
    /// </summary>
    public const int LatestVersion = 1;
}
