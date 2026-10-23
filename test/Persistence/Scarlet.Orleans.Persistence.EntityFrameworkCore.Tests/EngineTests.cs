namespace Scarlet.Orleans.Persistence.Tests;

public sealed class SqliteGrainStorageTests(SqliteDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainStorageTests(PostgreSqlDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainStorageTests(SqlServerDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class SqliteSchemaManagementTests(SqliteDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerSchemaManagementTests(SqlServerDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class SqliteModelTests(SqliteDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlModelTests(PostgreSqlDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerModelTests(SqlServerDatabaseFixture fixture) : ModelTests(fixture), IClassFixture<SqlServerDatabaseFixture>;
