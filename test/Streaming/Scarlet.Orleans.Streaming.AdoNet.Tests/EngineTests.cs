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

// One collection per engine: its cluster is shared by its end-to-end classes, which run one at a time, since each
// two-silo cluster holds its own connection pools.

public sealed class SqliteStreamingClusterFixture() : StreamingClusterFixture(new SqliteDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class SqliteStreamingCluster : ICollectionFixture<SqliteStreamingClusterFixture>, ICollectionFixture<SqliteDatabaseFixture>
{
    public const string Name = "Sqlite streaming cluster";
}

[Collection(SqliteStreamingCluster.Name)]
public sealed class SqliteStreamingClusterTests(SqliteStreamingClusterFixture fixture) : StreamingClusterTests(fixture);

[Collection(SqliteStreamingCluster.Name)]
public sealed class SqliteStreamingHandOverTests(SqliteDatabaseFixture fixture) : StreamingHandOverTests(fixture);

public sealed class PostgreSqlStreamingClusterFixture() : StreamingClusterFixture(new PostgreSqlDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class PostgreSqlStreamingCluster : ICollectionFixture<PostgreSqlStreamingClusterFixture>, ICollectionFixture<PostgreSqlDatabaseFixture>
{
    public const string Name = "PostgreSql streaming cluster";
}

[Collection(PostgreSqlStreamingCluster.Name)]
public sealed class PostgreSqlStreamingClusterTests(PostgreSqlStreamingClusterFixture fixture) : StreamingClusterTests(fixture);

[Collection(PostgreSqlStreamingCluster.Name)]
public sealed class PostgreSqlStreamingHandOverTests(PostgreSqlDatabaseFixture fixture) : StreamingHandOverTests(fixture);

public sealed class SqlServerStreamingClusterFixture() : StreamingClusterFixture(new SqlServerDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class SqlServerStreamingCluster : ICollectionFixture<SqlServerStreamingClusterFixture>, ICollectionFixture<SqlServerDatabaseFixture>
{
    public const string Name = "SqlServer streaming cluster";
}

[Collection(SqlServerStreamingCluster.Name)]
public sealed class SqlServerStreamingClusterTests(SqlServerStreamingClusterFixture fixture) : StreamingClusterTests(fixture);

[Collection(SqlServerStreamingCluster.Name)]
public sealed class SqlServerStreamingHandOverTests(SqlServerDatabaseFixture fixture) : StreamingHandOverTests(fixture);

public sealed class MySqlStreamingClusterFixture() : StreamingClusterFixture(new MySqlDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class MySqlStreamingCluster : ICollectionFixture<MySqlStreamingClusterFixture>, ICollectionFixture<MySqlDatabaseFixture>
{
    public const string Name = "MySql streaming cluster";
}

[Collection(MySqlStreamingCluster.Name)]
public sealed class MySqlStreamingClusterTests(MySqlStreamingClusterFixture fixture) : StreamingClusterTests(fixture);

[Collection(MySqlStreamingCluster.Name)]
public sealed class MySqlStreamingHandOverTests(MySqlDatabaseFixture fixture) : StreamingHandOverTests(fixture);

public sealed class MySqlDataStreamingClusterFixture() : StreamingClusterFixture(new MySqlDataDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class MySqlDataStreamingCluster : ICollectionFixture<MySqlDataStreamingClusterFixture>, ICollectionFixture<MySqlDataDatabaseFixture>
{
    public const string Name = "MySqlData streaming cluster";
}

[Collection(MySqlDataStreamingCluster.Name)]
public sealed class MySqlDataStreamingClusterTests(MySqlDataStreamingClusterFixture fixture) : StreamingClusterTests(fixture);

[Collection(MySqlDataStreamingCluster.Name)]
public sealed class MySqlDataStreamingHandOverTests(MySqlDataDatabaseFixture fixture) : StreamingHandOverTests(fixture);

public sealed class MariaDbStreamingClusterFixture() : StreamingClusterFixture(new MariaDbDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class MariaDbStreamingCluster : ICollectionFixture<MariaDbStreamingClusterFixture>, ICollectionFixture<MariaDbDatabaseFixture>
{
    public const string Name = "MariaDb streaming cluster";
}

[Collection(MariaDbStreamingCluster.Name)]
public sealed class MariaDbStreamingClusterTests(MariaDbStreamingClusterFixture fixture) : StreamingClusterTests(fixture);

[Collection(MariaDbStreamingCluster.Name)]
public sealed class MariaDbStreamingHandOverTests(MariaDbDatabaseFixture fixture) : StreamingHandOverTests(fixture);

public sealed class SqliteStreamingModelTests(SqliteDatabaseFixture fixture) : StreamingModelTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlStreamingModelTests(PostgreSqlDatabaseFixture fixture) : StreamingModelTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerStreamingModelTests(SqlServerDatabaseFixture fixture) : StreamingModelTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlStreamingModelTests(MySqlDatabaseFixture fixture) : StreamingModelTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MySqlDataStreamingModelTests(MySqlDataDatabaseFixture fixture) : StreamingModelTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class MariaDbStreamingModelTests(MariaDbDatabaseFixture fixture) : StreamingModelTests(fixture), IClassFixture<MariaDbDatabaseFixture>;
