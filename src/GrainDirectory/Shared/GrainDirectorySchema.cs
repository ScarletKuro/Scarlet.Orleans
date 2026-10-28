namespace Scarlet.Orleans.GrainDirectory;

internal static class GrainDirectorySchema
{
    /// <summary>The ledger key of this component.</summary>
    public const string Component = "grain-directory";

    public const string TableName = "orleans_grain_directory";

    /// <summary>
    /// The oldest schema version this code runs against. Raised only by a release that needs a newer schema.
    /// </summary>
    public const int MinimumVersion = 1;

    /// <summary>
    /// The newest schema version. The ADO.NET flavour ships one script per version and engine; a test keeps them in step.
    /// </summary>
    public const int LatestVersion = 1;
}
