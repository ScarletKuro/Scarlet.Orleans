namespace Scarlet.Orleans.Transactions.Tests;

// One collection per engine: its clusters are shared by its test classes, and all its classes run one at a time,
// because classes running in parallel exceed PostgreSQL's default of 100 connections.

public sealed class SqliteTransactionsFixture() : TransactionsClusterFixture(new SqliteDatabaseFixture());

public sealed class SqliteSkewedClockTransactionsFixture() : TransactionsClusterFixture(new SqliteDatabaseFixture(), skewedClock: true);

public sealed class SqliteFaultInjectionFixture() : FaultInjectionFixture(new SqliteDatabaseFixture());

public sealed class SqliteGrainStorageWrapperFixture() : GrainStorageWrapperFixture(new SqliteDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class SqliteTransactions : ICollectionFixture<SqliteTransactionsFixture>, ICollectionFixture<SqliteSkewedClockTransactionsFixture>, ICollectionFixture<SqliteFaultInjectionFixture>, ICollectionFixture<SqliteGrainStorageWrapperFixture>
{
    public const string Name = "Sqlite transactions";
}

[Collection(SqliteTransactions.Name)]
public sealed class SqliteTransactionalStateStorageTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : TransactionalStateStorageTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteGoldenPathTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteGrainFaultTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteTocGoldenPathTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteTocFaultTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : TocFaultTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteConcurrencyTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteExclusiveLockTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : ExclusiveLockTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteScopedTransactionTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : ScopedTransactionTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteConsistencyTests(SqliteTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteSkewedClockGoldenPathTests(SqliteSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteSkewedClockConsistencyTests(SqliteSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteFaultInjectionTests(SqliteFaultInjectionFixture fixture, ITestOutputHelper output) : FaultInjectionTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteGrainStorageWrapperGoldenPathTests(SqliteGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteGrainStorageWrapperGrainFaultTests(SqliteGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteGrainStorageWrapperTocGoldenPathTests(SqliteGrainStorageWrapperFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteGrainStorageWrapperConcurrencyTests(SqliteGrainStorageWrapperFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(SqliteTransactions.Name)]
public sealed class SqliteTransactionalStateStoreTests(SqliteDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<SqliteDatabaseFixture>;

[Collection(SqliteTransactions.Name)]
public sealed class SqliteTransactionalStateConcurrencyTests(SqliteDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<SqliteDatabaseFixture>;

[Collection(SqliteTransactions.Name)]
public sealed class SqliteRecoveryTests(SqliteDatabaseFixture fixture, ITestOutputHelper output) : RecoveryTests(fixture, output), IClassFixture<SqliteDatabaseFixture>;

[Collection(SqliteTransactions.Name)]
public sealed class SqliteTransactionsSchemaManagementTests(SqliteDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlTransactionsFixture() : TransactionsClusterFixture(new PostgreSqlDatabaseFixture());

public sealed class PostgreSqlSkewedClockTransactionsFixture() : TransactionsClusterFixture(new PostgreSqlDatabaseFixture(), skewedClock: true);

public sealed class PostgreSqlFaultInjectionFixture() : FaultInjectionFixture(new PostgreSqlDatabaseFixture());

public sealed class PostgreSqlGrainStorageWrapperFixture() : GrainStorageWrapperFixture(new PostgreSqlDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class PostgreSqlTransactions : ICollectionFixture<PostgreSqlTransactionsFixture>, ICollectionFixture<PostgreSqlSkewedClockTransactionsFixture>, ICollectionFixture<PostgreSqlFaultInjectionFixture>, ICollectionFixture<PostgreSqlGrainStorageWrapperFixture>
{
    public const string Name = "PostgreSql transactions";
}

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlTransactionalStateStorageTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : TransactionalStateStorageTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlGoldenPathTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlGrainFaultTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlTocGoldenPathTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlTocFaultTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : TocFaultTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlConcurrencyTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlExclusiveLockTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : ExclusiveLockTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlScopedTransactionTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : ScopedTransactionTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlConsistencyTests(PostgreSqlTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlSkewedClockGoldenPathTests(PostgreSqlSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlSkewedClockConsistencyTests(PostgreSqlSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlFaultInjectionTests(PostgreSqlFaultInjectionFixture fixture, ITestOutputHelper output) : FaultInjectionTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlGrainStorageWrapperGoldenPathTests(PostgreSqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlGrainStorageWrapperGrainFaultTests(PostgreSqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlGrainStorageWrapperTocGoldenPathTests(PostgreSqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlGrainStorageWrapperConcurrencyTests(PostgreSqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlTransactionalStateStoreTests(PostgreSqlDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlTransactionalStateConcurrencyTests(PostgreSqlDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlRecoveryTests(PostgreSqlDatabaseFixture fixture, ITestOutputHelper output) : RecoveryTests(fixture, output), IClassFixture<PostgreSqlDatabaseFixture>;

[Collection(PostgreSqlTransactions.Name)]
public sealed class PostgreSqlTransactionsSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerTransactionsFixture() : TransactionsClusterFixture(new SqlServerDatabaseFixture());

public sealed class SqlServerSkewedClockTransactionsFixture() : TransactionsClusterFixture(new SqlServerDatabaseFixture(), skewedClock: true);

public sealed class SqlServerFaultInjectionFixture() : FaultInjectionFixture(new SqlServerDatabaseFixture());

public sealed class SqlServerGrainStorageWrapperFixture() : GrainStorageWrapperFixture(new SqlServerDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class SqlServerTransactions : ICollectionFixture<SqlServerTransactionsFixture>, ICollectionFixture<SqlServerSkewedClockTransactionsFixture>, ICollectionFixture<SqlServerFaultInjectionFixture>, ICollectionFixture<SqlServerGrainStorageWrapperFixture>
{
    public const string Name = "SqlServer transactions";
}

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerTransactionalStateStorageTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : TransactionalStateStorageTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerGoldenPathTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerGrainFaultTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerTocGoldenPathTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerTocFaultTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : TocFaultTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerConcurrencyTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerExclusiveLockTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : ExclusiveLockTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerScopedTransactionTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : ScopedTransactionTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerConsistencyTests(SqlServerTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerSkewedClockGoldenPathTests(SqlServerSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerSkewedClockConsistencyTests(SqlServerSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerFaultInjectionTests(SqlServerFaultInjectionFixture fixture, ITestOutputHelper output) : FaultInjectionTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerGrainStorageWrapperGoldenPathTests(SqlServerGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerGrainStorageWrapperGrainFaultTests(SqlServerGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerGrainStorageWrapperTocGoldenPathTests(SqlServerGrainStorageWrapperFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerGrainStorageWrapperConcurrencyTests(SqlServerGrainStorageWrapperFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerTransactionalStateStoreTests(SqlServerDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerTransactionalStateConcurrencyTests(SqlServerDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerRecoveryTests(SqlServerDatabaseFixture fixture, ITestOutputHelper output) : RecoveryTests(fixture, output), IClassFixture<SqlServerDatabaseFixture>;

[Collection(SqlServerTransactions.Name)]
public sealed class SqlServerTransactionsSchemaManagementTests(SqlServerDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlTransactionsFixture() : TransactionsClusterFixture(new MySqlDatabaseFixture());

public sealed class MySqlSkewedClockTransactionsFixture() : TransactionsClusterFixture(new MySqlDatabaseFixture(), skewedClock: true);

public sealed class MySqlFaultInjectionFixture() : FaultInjectionFixture(new MySqlDatabaseFixture());

public sealed class MySqlGrainStorageWrapperFixture() : GrainStorageWrapperFixture(new MySqlDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class MySqlTransactions : ICollectionFixture<MySqlTransactionsFixture>, ICollectionFixture<MySqlSkewedClockTransactionsFixture>, ICollectionFixture<MySqlFaultInjectionFixture>, ICollectionFixture<MySqlGrainStorageWrapperFixture>
{
    public const string Name = "MySql transactions";
}

[Collection(MySqlTransactions.Name)]
public sealed class MySqlTransactionalStateStorageTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : TransactionalStateStorageTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlGoldenPathTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlGrainFaultTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlTocGoldenPathTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlTocFaultTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : TocFaultTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlConcurrencyTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlExclusiveLockTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : ExclusiveLockTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlScopedTransactionTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : ScopedTransactionTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlConsistencyTests(MySqlTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlSkewedClockGoldenPathTests(MySqlSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlSkewedClockConsistencyTests(MySqlSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlFaultInjectionTests(MySqlFaultInjectionFixture fixture, ITestOutputHelper output) : FaultInjectionTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlGrainStorageWrapperGoldenPathTests(MySqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlGrainStorageWrapperGrainFaultTests(MySqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlGrainStorageWrapperTocGoldenPathTests(MySqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlGrainStorageWrapperConcurrencyTests(MySqlGrainStorageWrapperFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(MySqlTransactions.Name)]
public sealed class MySqlTransactionalStateStoreTests(MySqlDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<MySqlDatabaseFixture>;

[Collection(MySqlTransactions.Name)]
public sealed class MySqlTransactionalStateConcurrencyTests(MySqlDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<MySqlDatabaseFixture>;

[Collection(MySqlTransactions.Name)]
public sealed class MySqlRecoveryTests(MySqlDatabaseFixture fixture, ITestOutputHelper output) : RecoveryTests(fixture, output), IClassFixture<MySqlDatabaseFixture>;

[Collection(MySqlTransactions.Name)]
public sealed class MySqlTransactionsSchemaManagementTests(MySqlDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbTransactionsFixture() : TransactionsClusterFixture(new MariaDbDatabaseFixture());

public sealed class MariaDbSkewedClockTransactionsFixture() : TransactionsClusterFixture(new MariaDbDatabaseFixture(), skewedClock: true);

public sealed class MariaDbFaultInjectionFixture() : FaultInjectionFixture(new MariaDbDatabaseFixture());

public sealed class MariaDbGrainStorageWrapperFixture() : GrainStorageWrapperFixture(new MariaDbDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class MariaDbTransactions : ICollectionFixture<MariaDbTransactionsFixture>, ICollectionFixture<MariaDbSkewedClockTransactionsFixture>, ICollectionFixture<MariaDbFaultInjectionFixture>, ICollectionFixture<MariaDbGrainStorageWrapperFixture>
{
    public const string Name = "MariaDb transactions";
}

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbTransactionalStateStorageTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : TransactionalStateStorageTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbGoldenPathTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbGrainFaultTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbTocGoldenPathTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbTocFaultTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : TocFaultTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbConcurrencyTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbExclusiveLockTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : ExclusiveLockTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbScopedTransactionTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : ScopedTransactionTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbConsistencyTests(MariaDbTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbSkewedClockGoldenPathTests(MariaDbSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbSkewedClockConsistencyTests(MariaDbSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : ConsistencyTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbFaultInjectionTests(MariaDbFaultInjectionFixture fixture, ITestOutputHelper output) : FaultInjectionTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbGrainStorageWrapperGoldenPathTests(MariaDbGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbGrainStorageWrapperGrainFaultTests(MariaDbGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbGrainStorageWrapperTocGoldenPathTests(MariaDbGrainStorageWrapperFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbGrainStorageWrapperConcurrencyTests(MariaDbGrainStorageWrapperFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbTransactionalStateStoreTests(MariaDbDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbTransactionalStateConcurrencyTests(MariaDbDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbRecoveryTests(MariaDbDatabaseFixture fixture, ITestOutputHelper output) : RecoveryTests(fixture, output), IClassFixture<MariaDbDatabaseFixture>;

[Collection(MariaDbTransactions.Name)]
public sealed class MariaDbTransactionsSchemaManagementTests(MariaDbDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataTransactionsFixture() : TransactionsClusterFixture(new MySqlDataDatabaseFixture());

public sealed class MySqlDataSkewedClockTransactionsFixture() : TransactionsClusterFixture(new MySqlDataDatabaseFixture(), skewedClock: true);

public sealed class MySqlDataFaultInjectionFixture() : FaultInjectionFixture(new MySqlDataDatabaseFixture());

public sealed class MySqlDataGrainStorageWrapperFixture() : GrainStorageWrapperFixture(new MySqlDataDatabaseFixture());

[CollectionDefinition(Name)]
public sealed class MySqlDataTransactions : ICollectionFixture<MySqlDataTransactionsFixture>, ICollectionFixture<MySqlDataSkewedClockTransactionsFixture>, ICollectionFixture<MySqlDataFaultInjectionFixture>, ICollectionFixture<MySqlDataGrainStorageWrapperFixture>
{
    public const string Name = "MySqlData transactions";
}

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataTransactionalStateStorageTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : TransactionalStateStorageTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataGoldenPathTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataGrainFaultTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataTocGoldenPathTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataTocFaultTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : TocFaultTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataConcurrencyTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataExclusiveLockTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : ExclusiveLockTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataScopedTransactionTests(MySqlDataTransactionsFixture fixture, ITestOutputHelper output) : ScopedTransactionTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataSkewedClockGoldenPathTests(MySqlDataSkewedClockTransactionsFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataFaultInjectionTests(MySqlDataFaultInjectionFixture fixture, ITestOutputHelper output) : FaultInjectionTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataGrainStorageWrapperGoldenPathTests(MySqlDataGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GoldenPathTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataGrainStorageWrapperGrainFaultTests(MySqlDataGrainStorageWrapperFixture fixture, ITestOutputHelper output) : GrainFaultTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataGrainStorageWrapperTocGoldenPathTests(MySqlDataGrainStorageWrapperFixture fixture, ITestOutputHelper output) : TocGoldenPathTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataGrainStorageWrapperConcurrencyTests(MySqlDataGrainStorageWrapperFixture fixture, ITestOutputHelper output) : ConcurrencyTests(fixture, output);

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataTransactionalStateStoreTests(MySqlDataDatabaseFixture fixture) : TransactionalStateStoreTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataTransactionalStateConcurrencyTests(MySqlDataDatabaseFixture fixture) : TransactionalStateConcurrencyTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

[Collection(MySqlDataTransactions.Name)]
public sealed class MySqlDataTransactionsSchemaManagementTests(MySqlDataDatabaseFixture fixture) : TransactionsSchemaManagementTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;
