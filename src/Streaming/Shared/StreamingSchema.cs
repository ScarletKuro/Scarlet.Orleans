namespace Scarlet.Orleans.Streaming;

internal static class StreamingSchema
{
    /// <summary>The ledger key of this component.</summary>
    public const string Component = "streaming";

    public const string QueueTableName = "orleans_stream_queue";

    public const string MessageTableName = "orleans_stream_message";

    public const string DeadLetterTableName = "orleans_stream_dead_letter";

    /// <summary>
    /// The oldest schema version this code runs against. Raised only by a release that needs a newer schema.
    /// </summary>
    public const int MinimumVersion = 1;

    /// <summary>
    /// The newest schema version. The ADO.NET flavour ships one script per version and engine; a test keeps them in step.
    /// </summary>
    public const int LatestVersion = 1;
}
