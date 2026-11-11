namespace Scarlet.Orleans.Streaming.Tests;

public sealed class SqliteStreamQueueStoreTests(SqliteDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class SqliteStreamingSchemaManagementTests(SqliteDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlStreamQueueStoreTests(PostgreSqlDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class PostgreSqlStreamingSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerStreamQueueStoreTests(SqlServerDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class SqlServerStreamingSchemaManagementTests(SqlServerDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlStreamQueueStoreTests(MySqlDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MySqlStreamingSchemaManagementTests(MySqlDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MySqlDataStreamQueueStoreTests(MySqlDataDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class MySqlDataStreamingSchemaManagementTests(MySqlDataDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class MariaDbStreamQueueStoreTests(MariaDbDatabaseFixture fixture) : StreamQueueStoreTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MariaDbStreamingSchemaManagementTests(MariaDbDatabaseFixture fixture) : StreamingSchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteStreamReceiverTests(SqliteDatabaseFixture fixture) : StreamReceiverTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlStreamReceiverTests(PostgreSqlDatabaseFixture fixture) : StreamReceiverTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerStreamReceiverTests(SqlServerDatabaseFixture fixture) : StreamReceiverTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlStreamReceiverTests(MySqlDatabaseFixture fixture) : StreamReceiverTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MySqlDataStreamReceiverTests(MySqlDataDatabaseFixture fixture) : StreamReceiverTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class MariaDbStreamReceiverTests(MariaDbDatabaseFixture fixture) : StreamReceiverTests(fixture), IClassFixture<MariaDbDatabaseFixture>;
