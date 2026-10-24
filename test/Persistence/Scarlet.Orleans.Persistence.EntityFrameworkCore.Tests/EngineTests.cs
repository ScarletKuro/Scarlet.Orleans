namespace Scarlet.Orleans.Persistence.Tests;

public sealed class SqliteGrainStorageTests(SqliteDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainStorageTests(PostgreSqlDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainStorageTests(SqlServerDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<SqlServerDatabaseFixture>;




public sealed class SqliteModelTests(SqliteDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlModelTests(PostgreSqlDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerModelTests(SqlServerDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainStorageTests(MySqlDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainStorageTests(MariaDbDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataGrainStorageTests(MySqlDataDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;




public sealed class MySqlModelTests(MySqlDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbModelTests(MariaDbDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataModelTests(MySqlDataDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteDiagnosticsTests(SqliteDatabaseFixture fixture) : DiagnosticsTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlConnectionFailureTests(PostgreSqlDatabaseFixture fixture) : ConnectionFailureTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerConnectionFailureTests(SqlServerDatabaseFixture fixture) : ConnectionFailureTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlConnectionFailureTests(MySqlDatabaseFixture fixture) : ConnectionFailureTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbConnectionFailureTests(MariaDbDatabaseFixture fixture) : ConnectionFailureTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataConnectionFailureTests(MySqlDataDatabaseFixture fixture) : ConnectionFailureTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;
