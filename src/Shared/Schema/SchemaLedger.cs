namespace Scarlet.Orleans.Relational.Schema;

/// <summary>
/// SQL for the <c>orleans_schema_history</c> ledger and the migration lock, per engine.
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

    private const string LockName = "scarlet-orleans-schema";

    /// <summary>An arbitrary constant key for PostgreSQL's advisory lock, which takes a number, not a name.</summary>
    private const long PostgreSqlLockKey = 0x5C0A_0E1E_A115_0001;

    public static string CreateTable(RelationalEngine engine) => engine switch
    {
        RelationalEngine.SqlServer => $"""
            IF OBJECT_ID(N'{TableName}', N'U') IS NULL
            CREATE TABLE {TableName} (
                component nvarchar(128) NOT NULL,
                version int NOT NULL,
                script nvarchar(256) NOT NULL,
                checksum nvarchar(64) NULL,
                source nvarchar(16) NOT NULL,
                applied_on datetime2 NOT NULL,
                CONSTRAINT pk_{TableName} PRIMARY KEY (component, version));
            """,
        RelationalEngine.PostgreSql => $"""
            CREATE TABLE IF NOT EXISTS {TableName} (
                component varchar(128) NOT NULL,
                version integer NOT NULL,
                script varchar(256) NOT NULL,
                checksum varchar(64) NULL,
                source varchar(16) NOT NULL,
                applied_on timestamp with time zone NOT NULL,
                CONSTRAINT pk_{TableName} PRIMARY KEY (component, version));
            """,
        RelationalEngine.MySql or RelationalEngine.MariaDb => $"""
            CREATE TABLE IF NOT EXISTS {TableName} (
                component varchar(128) NOT NULL,
                version int NOT NULL,
                script varchar(256) NOT NULL,
                checksum varchar(64) NULL,
                source varchar(16) NOT NULL,
                applied_on datetime(6) NOT NULL,
                CONSTRAINT pk_{TableName} PRIMARY KEY (component, version)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
            """,
        RelationalEngine.Sqlite => $"""
            CREATE TABLE IF NOT EXISTS {TableName} (
                component TEXT NOT NULL,
                version INTEGER NOT NULL,
                script TEXT NOT NULL,
                checksum TEXT NULL,
                source TEXT NOT NULL,
                applied_on TEXT NOT NULL,
                CONSTRAINT pk_{TableName} PRIMARY KEY (component, version));
            """,
        _ => throw new NotSupportedException(engine.ToString()),
    };

    /// <summary>Returns 1 if the ledger exists, otherwise 0.</summary>
    public static string TableExists(RelationalEngine engine) => engine switch
    {
        RelationalEngine.SqlServer => $"SELECT CASE WHEN OBJECT_ID(N'{TableName}', N'U') IS NULL THEN 0 ELSE 1 END",
        RelationalEngine.PostgreSql => $"SELECT CASE WHEN to_regclass('{TableName}') IS NULL THEN 0 ELSE 1 END",
        RelationalEngine.MySql or RelationalEngine.MariaDb =>
            $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{TableName}'",
        RelationalEngine.Sqlite => $"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{TableName}'",
        _ => throw new NotSupportedException(engine.ToString()),
    };

    public const string SelectApplied = $"SELECT version, checksum, source FROM {TableName} WHERE component = @component ORDER BY version";

    /// <summary>
    /// Records an applied version. <c>CURRENT_TIMESTAMP</c> works on every engine, which lets EF migrations emit the
    /// same statement as literal SQL.
    /// </summary>
    public const string Insert = $"INSERT INTO {TableName} (component, version, script, checksum, source, applied_on) VALUES (@component, @version, @script, @checksum, @source, CURRENT_TIMESTAMP)";

    /// <summary>
    /// Takes the session-level migration lock, or for SQLite starts the transaction that serves as the lock.
    /// Returns a positive number when the lock was granted.
    /// </summary>
    public static string AcquireLock(RelationalEngine engine) => engine switch
    {
        RelationalEngine.SqlServer => $"""
            DECLARE @result int;
            EXEC @result = sp_getapplock @Resource = N'{LockName}', @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = @timeout_ms;
            SELECT CASE WHEN @result >= 0 THEN 1 ELSE 0 END;
            """,
        RelationalEngine.PostgreSql => $"SELECT 1 FROM (SELECT pg_advisory_lock({PostgreSqlLockKey})) AS acquired",
        RelationalEngine.MySql or RelationalEngine.MariaDb => $"SELECT COALESCE(GET_LOCK('{LockName}', @timeout_s), 0)",
        _ => throw new NotSupportedException(engine.ToString()),
    };

    public static string ReleaseLock(RelationalEngine engine) => engine switch
    {
        RelationalEngine.SqlServer => $"EXEC sp_releaseapplock @Resource = N'{LockName}', @LockOwner = N'Session'",
        RelationalEngine.PostgreSql => $"SELECT pg_advisory_unlock({PostgreSqlLockKey})",
        RelationalEngine.MySql or RelationalEngine.MariaDb => $"SELECT RELEASE_LOCK('{LockName}')",
        _ => throw new NotSupportedException(engine.ToString()),
    };
}
