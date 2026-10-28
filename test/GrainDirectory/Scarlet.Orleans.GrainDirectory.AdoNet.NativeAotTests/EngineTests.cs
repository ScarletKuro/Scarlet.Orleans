namespace Scarlet.Orleans.GrainDirectory.Tests;

public sealed class SqliteGrainDirectoryConformanceTests(SqliteDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainDirectoryConformanceTests(PostgreSqlDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectoryConformanceTests(SqlServerDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectoryConformanceTests(MySqlDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectoryConformanceTests(MariaDbDatabaseFixture fixture) : GrainDirectoryConformanceTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteGrainDirectorySchemaManagementTests(SqliteDatabaseFixture fixture) : GrainDirectorySchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainDirectorySchemaManagementTests(PostgreSqlDatabaseFixture fixture) : GrainDirectorySchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectorySchemaManagementTests(SqlServerDatabaseFixture fixture) : GrainDirectorySchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectorySchemaManagementTests(MySqlDatabaseFixture fixture) : GrainDirectorySchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectorySchemaManagementTests(MariaDbDatabaseFixture fixture) : GrainDirectorySchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteGrainDirectoryCancellationTests(SqliteDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainDirectoryCancellationTests(PostgreSqlDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectoryCancellationTests(SqlServerDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectoryCancellationTests(MySqlDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectoryCancellationTests(MariaDbDatabaseFixture fixture) : GrainDirectoryCancellationTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class PostgreSqlGrainDirectoryConnectionFailureTests(PostgreSqlDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainDirectoryConnectionFailureTests(SqlServerDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainDirectoryConnectionFailureTests(MySqlDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainDirectoryConnectionFailureTests(MariaDbDatabaseFixture fixture) : GrainDirectoryConnectionFailureTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteGrainDirectoryDiagnosticsTests(SqliteDatabaseFixture fixture) : GrainDirectoryDiagnosticsTests(fixture), IClassFixture<SqliteDatabaseFixture>;
