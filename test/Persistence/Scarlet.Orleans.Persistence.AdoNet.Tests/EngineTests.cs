namespace Scarlet.Orleans.Persistence.Tests;

public sealed class SqliteGrainStorageTests(SqliteDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlGrainStorageTests(PostgreSqlDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerGrainStorageTests(SqlServerDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlGrainStorageTests(MySqlDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbGrainStorageTests(MariaDbDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteSchemaManagementTests(SqliteDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerSchemaManagementTests(SqlServerDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlSchemaManagementTests(MySqlDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbSchemaManagementTests(MariaDbDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteSchemaMigratorTests(SqliteDatabaseFixture fixture) : SchemaMigratorTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlSchemaMigratorTests(PostgreSqlDatabaseFixture fixture) : SchemaMigratorTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerSchemaMigratorTests(SqlServerDatabaseFixture fixture) : SchemaMigratorTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlSchemaMigratorTests(MySqlDatabaseFixture fixture) : SchemaMigratorTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbSchemaMigratorTests(MariaDbDatabaseFixture fixture) : SchemaMigratorTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataGrainStorageTests(MySqlDataDatabaseFixture fixture) : GrainStorageConformanceTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class MySqlDataSchemaManagementTests(MySqlDataDatabaseFixture fixture) : SchemaManagementTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class MySqlDataSchemaMigratorTests(MySqlDataDatabaseFixture fixture) : SchemaMigratorTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteDiagnosticsTests(SqliteDatabaseFixture fixture) : DiagnosticsTests(fixture), IClassFixture<SqliteDatabaseFixture>;
