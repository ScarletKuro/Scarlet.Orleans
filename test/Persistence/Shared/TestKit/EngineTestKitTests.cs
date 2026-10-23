namespace Scarlet.Orleans.Persistence.Tests;

public sealed class SqliteTestKitFixture() : TestKitFixture(new SqliteDatabaseFixture(), deleteStateOnClear: false);

public sealed class SqliteTestKitTests(SqliteTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<SqliteTestKitFixture>;

public sealed class SqliteDeleteOnClearTestKitFixture() : TestKitFixture(new SqliteDatabaseFixture(), deleteStateOnClear: true);

public sealed class SqliteDeleteOnClearTestKitTests(SqliteDeleteOnClearTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<SqliteDeleteOnClearTestKitFixture>;

public sealed class PostgreSqlTestKitFixture() : TestKitFixture(new PostgreSqlDatabaseFixture(), deleteStateOnClear: false);

public sealed class PostgreSqlTestKitTests(PostgreSqlTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<PostgreSqlTestKitFixture>;

public sealed class PostgreSqlDeleteOnClearTestKitFixture() : TestKitFixture(new PostgreSqlDatabaseFixture(), deleteStateOnClear: true);

public sealed class PostgreSqlDeleteOnClearTestKitTests(PostgreSqlDeleteOnClearTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<PostgreSqlDeleteOnClearTestKitFixture>;

public sealed class SqlServerTestKitFixture() : TestKitFixture(new SqlServerDatabaseFixture(), deleteStateOnClear: false);

public sealed class SqlServerTestKitTests(SqlServerTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<SqlServerTestKitFixture>;

public sealed class SqlServerDeleteOnClearTestKitFixture() : TestKitFixture(new SqlServerDatabaseFixture(), deleteStateOnClear: true);

public sealed class SqlServerDeleteOnClearTestKitTests(SqlServerDeleteOnClearTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<SqlServerDeleteOnClearTestKitFixture>;

public sealed class MySqlTestKitFixture() : TestKitFixture(new MySqlDatabaseFixture(), deleteStateOnClear: false);

public sealed class MySqlTestKitTests(MySqlTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<MySqlTestKitFixture>;

public sealed class MySqlDeleteOnClearTestKitFixture() : TestKitFixture(new MySqlDatabaseFixture(), deleteStateOnClear: true);

public sealed class MySqlDeleteOnClearTestKitTests(MySqlDeleteOnClearTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<MySqlDeleteOnClearTestKitFixture>;

public sealed class MariaDbTestKitFixture() : TestKitFixture(new MariaDbDatabaseFixture(), deleteStateOnClear: false);

public sealed class MariaDbTestKitTests(MariaDbTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<MariaDbTestKitFixture>;

public sealed class MariaDbDeleteOnClearTestKitFixture() : TestKitFixture(new MariaDbDatabaseFixture(), deleteStateOnClear: true);

public sealed class MariaDbDeleteOnClearTestKitTests(MariaDbDeleteOnClearTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<MariaDbDeleteOnClearTestKitFixture>;

public sealed class MySqlDataTestKitFixture() : TestKitFixture(new MySqlDataDatabaseFixture(), deleteStateOnClear: false);

public sealed class MySqlDataTestKitTests(MySqlDataTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<MySqlDataTestKitFixture>;

public sealed class MySqlDataDeleteOnClearTestKitFixture() : TestKitFixture(new MySqlDataDatabaseFixture(), deleteStateOnClear: true);

public sealed class MySqlDataDeleteOnClearTestKitTests(MySqlDataDeleteOnClearTestKitFixture fixture) : TestKitTests(fixture), IClassFixture<MySqlDataDeleteOnClearTestKitFixture>;
