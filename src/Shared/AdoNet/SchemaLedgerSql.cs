namespace Scarlet.Orleans.Relational.Schema;

/// <summary>
/// The per-engine SQL of the <see cref="SchemaLedger"/> table and the migration lock, used by the ADO.NET flavour's runner.
/// </summary>
internal static class SchemaLedgerSql
{
    private const string LockName = "scarlet-orleans-schema";

    /// <summary>An arbitrary constant key for PostgreSQL's advisory lock, which takes a number, not a name.</summary>
    private const long PostgreSqlLockKey = 0x5C0A_0E1E_A115_0001;

    public static string CreateTable(RelationalEngine engine) => engine switch
    {
        RelationalEngine.SqlServer => $"""
            IF OBJECT_ID(N'{SchemaLedger.TableName}', N'U') IS NULL
            CREATE TABLE {SchemaLedger.TableName} (
                component nvarchar(128) NOT NULL,
                version int NOT NULL,
                script nvarchar(256) NOT NULL,
                checksum nvarchar(64) NULL,
                source nvarchar(16) NOT NULL,
                applied_on datetime2 NOT NULL,
                CONSTRAINT pk_{SchemaLedger.TableName} PRIMARY KEY (component, version));
            """,
        RelationalEngine.PostgreSql => $"""
            CREATE TABLE IF NOT EXISTS {SchemaLedger.TableName} (
                component varchar(128) NOT NULL,
                version integer NOT NULL,
                script varchar(256) NOT NULL,
                checksum varchar(64) NULL,
                source varchar(16) NOT NULL,
                applied_on timestamp with time zone NOT NULL,
                CONSTRAINT pk_{SchemaLedger.TableName} PRIMARY KEY (component, version));
            """,
        RelationalEngine.MySql or RelationalEngine.MariaDb => $"""
            CREATE TABLE IF NOT EXISTS {SchemaLedger.TableName} (
                component varchar(128) NOT NULL,
                version int NOT NULL,
                script varchar(256) NOT NULL,
                checksum varchar(64) NULL,
                source varchar(16) NOT NULL,
                applied_on datetime(6) NOT NULL,
                CONSTRAINT pk_{SchemaLedger.TableName} PRIMARY KEY (component, version)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
            """,
        RelationalEngine.Sqlite => $"""
            CREATE TABLE IF NOT EXISTS {SchemaLedger.TableName} (
                component TEXT NOT NULL,
                version INTEGER NOT NULL,
                script TEXT NOT NULL,
                checksum TEXT NULL,
                source TEXT NOT NULL,
                applied_on TEXT NOT NULL,
                CONSTRAINT pk_{SchemaLedger.TableName} PRIMARY KEY (component, version));
            """,
        _ => throw new NotSupportedException(engine.ToString()),
    };

    /// <summary>Returns 1 if the ledger exists, otherwise 0.</summary>
    public static string TableExists(RelationalEngine engine) => engine switch
    {
        RelationalEngine.SqlServer => $"SELECT CASE WHEN OBJECT_ID(N'{SchemaLedger.TableName}', N'U') IS NULL THEN 0 ELSE 1 END",
        RelationalEngine.PostgreSql => $"SELECT CASE WHEN to_regclass('{SchemaLedger.TableName}') IS NULL THEN 0 ELSE 1 END",
        RelationalEngine.MySql or RelationalEngine.MariaDb =>
            $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{SchemaLedger.TableName}'",
        RelationalEngine.Sqlite => $"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{SchemaLedger.TableName}'",
        _ => throw new NotSupportedException(engine.ToString()),
    };

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
