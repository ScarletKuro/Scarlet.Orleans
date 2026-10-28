namespace Scarlet.Orleans.GrainDirectory.Tests;

public sealed class SqliteGrainDirectoryConformanceTests(SqliteDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainDirectoryConformanceTests(PostgreSqlDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectoryConformanceTests(SqlServerDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectoryConformanceTests(MySqlDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectoryConformanceTests(MariaDbDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataGrainDirectoryConformanceTests(MySqlDataDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteGrainDirectoryModelTests(SqliteDatabaseFixture fixture) : GrainDirectoryModelTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainDirectoryModelTests(PostgreSqlDatabaseFixture fixture) : GrainDirectoryModelTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectoryModelTests(SqlServerDatabaseFixture fixture) : GrainDirectoryModelTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectoryModelTests(MySqlDatabaseFixture fixture) : GrainDirectoryModelTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectoryModelTests(MariaDbDatabaseFixture fixture) : GrainDirectoryModelTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataGrainDirectoryModelTests(MySqlDataDatabaseFixture fixture) : GrainDirectoryModelTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteGrainDirectoryCancellationTests(SqliteDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainDirectoryCancellationTests(PostgreSqlDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectoryCancellationTests(SqlServerDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectoryCancellationTests(MySqlDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectoryCancellationTests(MariaDbDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataGrainDirectoryCancellationTests(MySqlDataDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteGrainDirectoryDiagnosticsTests(SqliteDatabaseFixture fixture) : GrainDirectoryDiagnosticsTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainDirectoryConnectionFailureTests(PostgreSqlDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectoryConnectionFailureTests(SqlServerDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectoryConnectionFailureTests(MySqlDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectoryConnectionFailureTests(MariaDbDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

// No MySql.Data connection failure tests: under concurrent session kills its async reads can wait on a dead
// connection past the command timeout, so the tests would hang in the driver. The README documents it.

public sealed class SqliteGrainDirectoryClusterTests() : GrainDirectoryClusterTests(new SqliteDatabaseFixture());

public sealed class PostgreSqlGrainDirectoryClusterTests() : GrainDirectoryClusterTests(new PostgreSqlDatabaseFixture());

public sealed class SqlServerGrainDirectoryClusterTests() : GrainDirectoryClusterTests(new SqlServerDatabaseFixture());

public sealed class MySqlGrainDirectoryClusterTests() : GrainDirectoryClusterTests(new MySqlDatabaseFixture());

public sealed class MariaDbGrainDirectoryClusterTests() : GrainDirectoryClusterTests(new MariaDbDatabaseFixture());

public sealed class MySqlDataGrainDirectoryClusterTests() : GrainDirectoryClusterTests(new MySqlDataDatabaseFixture());
