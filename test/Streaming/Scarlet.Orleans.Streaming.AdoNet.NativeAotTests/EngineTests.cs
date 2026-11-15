namespace Scarlet.Orleans.Streaming.Tests;

public sealed class PostgreSqlDequeueOrderingTests(PostgreSqlDatabaseFixture fixture) : DequeueOrderingTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerDequeueOrderingTests(SqlServerDatabaseFixture fixture) : DequeueOrderingTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class SqlServerSnapshotDequeueOrderingTests(SqlServerDatabaseFixture fixture) : DequeueOrderingTests(fixture, snapshot: true), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlDequeueOrderingTests(MySqlDatabaseFixture fixture) : DequeueOrderingTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbDequeueOrderingTests(MariaDbDatabaseFixture fixture) : DequeueOrderingTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteStreamQueueStoreTests(SqliteDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class SqliteStreamingSchemaManagementTests(SqliteDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;


public sealed class PostgreSqlStreamQueueStoreTests(PostgreSqlDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class PostgreSqlStreamingSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;


public sealed class SqlServerStreamQueueStoreTests(SqlServerDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class SqlServerStreamingSchemaManagementTests(SqlServerDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;


public sealed class MySqlStreamQueueStoreTests(MySqlDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MySqlStreamingSchemaManagementTests(MySqlDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;


public sealed class MariaDbStreamQueueStoreTests(MariaDbDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MariaDbStreamingSchemaManagementTests(MariaDbDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

