namespace Scarlet.Orleans.Clustering.Tests;

public sealed class SqliteMembershipTableTestKitTests(SqliteDatabaseFixture fixture) : MembershipTableTestKitTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlMembershipTableTestKitTests(PostgreSqlDatabaseFixture fixture) : MembershipTableTestKitTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerMembershipTableTestKitTests(SqlServerDatabaseFixture fixture) : MembershipTableTestKitTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlMembershipTableTestKitTests(MySqlDatabaseFixture fixture) : MembershipTableTestKitTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbMembershipTableTestKitTests(MariaDbDatabaseFixture fixture) : MembershipTableTestKitTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataMembershipTableTestKitTests(MySqlDataDatabaseFixture fixture) : MembershipTableTestKitTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteMembershipTableTests(SqliteDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlMembershipTableTests(PostgreSqlDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerMembershipTableTests(SqlServerDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlMembershipTableTests(MySqlDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbMembershipTableTests(MariaDbDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataMembershipTableTests(MySqlDataDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteClusteringSchemaManagementTests(SqliteDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlClusteringSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerClusteringSchemaManagementTests(SqlServerDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlClusteringSchemaManagementTests(MySqlDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbClusteringSchemaManagementTests(MariaDbDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataClusteringSchemaManagementTests(MySqlDataDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteClusteringHostTests() : ClusteringHostTests(new SqliteDatabaseFixture());

public sealed class PostgreSqlClusteringHostTests() : ClusteringHostTests(new PostgreSqlDatabaseFixture());

public sealed class SqlServerClusteringHostTests() : ClusteringHostTests(new SqlServerDatabaseFixture());

public sealed class MySqlClusteringHostTests() : ClusteringHostTests(new MySqlDatabaseFixture());

public sealed class MariaDbClusteringHostTests() : ClusteringHostTests(new MariaDbDatabaseFixture());

public sealed class MySqlDataClusteringHostTests() : ClusteringHostTests(new MySqlDataDatabaseFixture());

public sealed class SqliteLivenessTests(SqliteDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlLivenessTests(PostgreSqlDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerLivenessTests(SqlServerDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlLivenessTests(MySqlDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbLivenessTests(MariaDbDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataLivenessTests(MySqlDataDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteClusteringCancellationTests(SqliteDatabaseFixture fixture) : ClusteringCancellationTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlClusteringCancellationTests(PostgreSqlDatabaseFixture fixture) : ClusteringCancellationTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerClusteringCancellationTests(SqlServerDatabaseFixture fixture) : ClusteringCancellationTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlClusteringCancellationTests(MySqlDatabaseFixture fixture) : ClusteringCancellationTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbClusteringCancellationTests(MariaDbDatabaseFixture fixture) : ClusteringCancellationTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataClusteringCancellationTests(MySqlDataDatabaseFixture fixture) : ClusteringCancellationTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class PostgreSqlClusteringConnectionFailureTests(PostgreSqlDatabaseFixture fixture) : ClusteringConnectionFailureTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerClusteringConnectionFailureTests(SqlServerDatabaseFixture fixture) : ClusteringConnectionFailureTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlClusteringConnectionFailureTests(MySqlDatabaseFixture fixture) : ClusteringConnectionFailureTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbClusteringConnectionFailureTests(MariaDbDatabaseFixture fixture) : ClusteringConnectionFailureTests(fixture), IClassFixture<MariaDbDatabaseFixture>;
