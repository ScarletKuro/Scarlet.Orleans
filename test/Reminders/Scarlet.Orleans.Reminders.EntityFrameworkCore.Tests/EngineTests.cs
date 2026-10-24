namespace Scarlet.Orleans.Reminders.Tests;

public sealed class SqliteReminderTestKitTests(SqliteDatabaseFixture fixture) : ReminderTableTestKitTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlReminderTestKitTests(PostgreSqlDatabaseFixture fixture) : ReminderTableTestKitTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderTestKitTests(SqlServerDatabaseFixture fixture) : ReminderTableTestKitTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderTestKitTests(MySqlDatabaseFixture fixture) : ReminderTableTestKitTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderTestKitTests(MariaDbDatabaseFixture fixture) : ReminderTableTestKitTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataReminderTestKitTests(MySqlDataDatabaseFixture fixture) : ReminderTableTestKitTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteReminderConformanceTests(SqliteDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlReminderConformanceTests(PostgreSqlDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderConformanceTests(SqlServerDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderConformanceTests(MySqlDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderConformanceTests(MariaDbDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataReminderConformanceTests(MySqlDataDatabaseFixture fixture) : ReminderTableConformanceTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteReminderModelTests(SqliteDatabaseFixture fixture) : ReminderModelTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlReminderModelTests(PostgreSqlDatabaseFixture fixture) : ReminderModelTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderModelTests(SqlServerDatabaseFixture fixture) : ReminderModelTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderModelTests(MySqlDatabaseFixture fixture) : ReminderModelTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderModelTests(MariaDbDatabaseFixture fixture) : ReminderModelTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataReminderModelTests(MySqlDataDatabaseFixture fixture) : ReminderModelTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteReminderServiceFixture() : ReminderServiceFixture(new SqliteDatabaseFixture());

public sealed class SqliteReminderServiceTests(SqliteReminderServiceFixture fixture) : ReminderServiceTests(fixture), IClassFixture<SqliteReminderServiceFixture>;

public sealed class PostgreSqlReminderServiceFixture() : ReminderServiceFixture(new PostgreSqlDatabaseFixture());

public sealed class PostgreSqlReminderServiceTests(PostgreSqlReminderServiceFixture fixture) : ReminderServiceTests(fixture), IClassFixture<PostgreSqlReminderServiceFixture>;

public sealed class SqlServerReminderServiceFixture() : ReminderServiceFixture(new SqlServerDatabaseFixture());

public sealed class SqlServerReminderServiceTests(SqlServerReminderServiceFixture fixture) : ReminderServiceTests(fixture), IClassFixture<SqlServerReminderServiceFixture>;

public sealed class MySqlReminderServiceFixture() : ReminderServiceFixture(new MySqlDatabaseFixture());

public sealed class MySqlReminderServiceTests(MySqlReminderServiceFixture fixture) : ReminderServiceTests(fixture), IClassFixture<MySqlReminderServiceFixture>;

public sealed class MariaDbReminderServiceFixture() : ReminderServiceFixture(new MariaDbDatabaseFixture());

public sealed class MariaDbReminderServiceTests(MariaDbReminderServiceFixture fixture) : ReminderServiceTests(fixture), IClassFixture<MariaDbReminderServiceFixture>;

public sealed class MySqlDataReminderServiceFixture() : ReminderServiceFixture(new MySqlDataDatabaseFixture());

public sealed class MySqlDataReminderServiceTests(MySqlDataReminderServiceFixture fixture) : ReminderServiceTests(fixture), IClassFixture<MySqlDataReminderServiceFixture>;

public sealed class PostgreSqlReminderConnectionFailureTests(PostgreSqlDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerReminderConnectionFailureTests(SqlServerDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlReminderConnectionFailureTests(MySqlDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbReminderConnectionFailureTests(MariaDbDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataReminderConnectionFailureTests(MySqlDataDatabaseFixture fixture) : ReminderConnectionFailureTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;
