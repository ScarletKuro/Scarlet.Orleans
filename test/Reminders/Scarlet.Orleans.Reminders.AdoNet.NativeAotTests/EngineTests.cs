namespace Scarlet.Orleans.Reminders.Tests;

public sealed class SqliteReminderConformanceTests(SqliteDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlReminderConformanceTests(PostgreSqlDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderConformanceTests(SqlServerDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderConformanceTests(MySqlDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderConformanceTests(MariaDbDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteReminderSchemaManagementTests(SqliteDatabaseFixture fixture) : ReminderSchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlReminderSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : ReminderSchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderSchemaManagementTests(SqlServerDatabaseFixture fixture) : ReminderSchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderSchemaManagementTests(MySqlDatabaseFixture fixture) : ReminderSchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderSchemaManagementTests(MariaDbDatabaseFixture fixture) : ReminderSchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteReminderCancellationTests(SqliteDatabaseFixture fixture) : ReminderCancellationTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlReminderCancellationTests(PostgreSqlDatabaseFixture fixture) : ReminderCancellationTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderCancellationTests(SqlServerDatabaseFixture fixture) : ReminderCancellationTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderCancellationTests(MySqlDatabaseFixture fixture) : ReminderCancellationTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderCancellationTests(MariaDbDatabaseFixture fixture) : ReminderCancellationTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class PostgreSqlReminderConnectionFailureTests(PostgreSqlDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderConnectionFailureTests(SqlServerDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderConnectionFailureTests(MySqlDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderConnectionFailureTests(MariaDbDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteReminderDiagnosticsTests(SqliteDatabaseFixture fixture) : ReminderDiagnosticsTests(fixture), IClassFixture<SqliteDatabaseFixture>;
