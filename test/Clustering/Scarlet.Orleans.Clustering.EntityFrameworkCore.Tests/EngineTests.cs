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







public sealed class SqliteClusteringHostTests() : ClusteringHostTests(new SqliteDatabaseFixture());

public sealed class PostgreSqlClusteringHostTests() : ClusteringHostTests(new PostgreSqlDatabaseFixture());

public sealed class SqlServerClusteringHostTests() : ClusteringHostTests(new SqlServerDatabaseFixture());

public sealed class MySqlClusteringHostTests() : ClusteringHostTests(new MySqlDatabaseFixture());

public sealed class MariaDbClusteringHostTests() : ClusteringHostTests(new MariaDbDatabaseFixture());

public sealed class MySqlDataClusteringHostTests() : ClusteringHostTests(new MySqlDataDatabaseFixture());

public sealed class SqliteClusteringModelTests(SqliteDatabaseFixture fixture) : ClusteringModelTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlClusteringModelTests(PostgreSqlDatabaseFixture fixture) : ClusteringModelTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerClusteringModelTests(SqlServerDatabaseFixture fixture) : ClusteringModelTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlClusteringModelTests(MySqlDatabaseFixture fixture) : ClusteringModelTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbClusteringModelTests(MariaDbDatabaseFixture fixture) : ClusteringModelTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataClusteringModelTests(MySqlDataDatabaseFixture fixture) : ClusteringModelTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;

public sealed class SqliteLivenessTests(SqliteDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlLivenessTests(PostgreSqlDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerLivenessTests(SqlServerDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlLivenessTests(MySqlDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbLivenessTests(MariaDbDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class MySqlDataLivenessTests(MySqlDataDatabaseFixture fixture) : LivenessTests(fixture), IClassFixture<MySqlDataDatabaseFixture>;
