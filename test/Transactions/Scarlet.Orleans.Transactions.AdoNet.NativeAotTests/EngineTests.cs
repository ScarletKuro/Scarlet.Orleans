namespace Scarlet.Orleans.Transactions.Tests;

public sealed class SqliteTransactionalStateStoreTests(SqliteDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class SqliteTransactionsSchemaManagementTests(SqliteDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlTransactionalStateStoreTests(PostgreSqlDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class PostgreSqlTransactionsSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerTransactionalStateStoreTests(SqlServerDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class SqlServerTransactionsSchemaManagementTests(SqlServerDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlTransactionalStateStoreTests(MySqlDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MySqlTransactionsSchemaManagementTests(MySqlDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbTransactionalStateStoreTests(MariaDbDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MariaDbTransactionsSchemaManagementTests(MariaDbDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteTransactionalStateConcurrencyTests(SqliteDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlTransactionalStateConcurrencyTests(PostgreSqlDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerTransactionalStateConcurrencyTests(SqlServerDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlTransactionalStateConcurrencyTests(MySqlDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbTransactionalStateConcurrencyTests(MariaDbDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<MariaDbDatabaseFixture>;
