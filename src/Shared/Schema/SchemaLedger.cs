namespace Scarlet.Orleans.Relational.Schema;

/// <summary>
/// The <c>orleans_schema_history</c> ledger, shared by every component and both flavours: its names, and the standard
/// SQL that works on every engine. The ADO.NET flavour's per-engine SQL is in <c>SchemaLedgerSql</c>.
/// </summary>
/// <remarks>
/// Several assemblies, possibly at different versions, share this table, so its layout only ever changes in
/// backward-compatible ways.
/// </remarks>
internal static class SchemaLedger
{
    public const string TableName = "orleans_schema_history";

    /// <summary>The <c>source</c> of rows written by the migration runner, which records script checksums.</summary>
    public const string ScriptSource = "script";

    /// <summary>The <c>source</c> of rows written by EF Core migrations, whose DDL EF generated, so there is no checksum.</summary>
    public const string EntityFrameworkSource = "ef";

    /// <summary>
    /// The <c>source</c> of rows a shipped script writes about itself, when run by hand. A script cannot contain its own
    /// checksum, so there is none; the runner replaces the row with a checksummed one when it applies the script.
    /// </summary>
    public const string ManualSource = "manual";

    public const string SelectApplied = $"SELECT version, checksum, source FROM {TableName} WHERE component = @component ORDER BY version";

    /// <summary>
    /// Records an applied version. <c>CURRENT_TIMESTAMP</c> works on every engine.
    /// </summary>
    /// <summary>
    /// Records an applied version over the row a self-recording script wrote, if any. Followed by <see cref="Insert"/>
    /// when it matches no row.
    /// </summary>
    public const string Update = $"UPDATE {TableName} SET script = @script, checksum = @checksum, source = @source, applied_on = CURRENT_TIMESTAMP WHERE component = @component AND version = @version";

    public const string Insert = $"INSERT INTO {TableName} (component, version, script, checksum, source, applied_on) VALUES (@component, @version, @script, @checksum, @source, CURRENT_TIMESTAMP)";
}
