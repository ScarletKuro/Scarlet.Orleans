namespace Scarlet.Orleans.Clustering.Tests;

public sealed class SqliteMembershipTableTests(SqliteDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlMembershipTableTests(PostgreSqlDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerMembershipTableTests(SqlServerDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlMembershipTableTests(MySqlDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbMembershipTableTests(MariaDbDatabaseFixture fixture) : MembershipTableTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

public sealed class SqliteClusteringSchemaManagementTests(SqliteDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<SqliteDatabaseFixture>;

public sealed class PostgreSqlClusteringSchemaManagementTests(PostgreSqlDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<PostgreSqlDatabaseFixture>;

public sealed class SqlServerClusteringSchemaManagementTests(SqlServerDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<SqlServerDatabaseFixture>;

public sealed class MySqlClusteringSchemaManagementTests(MySqlDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<MySqlDatabaseFixture>;

public sealed class MariaDbClusteringSchemaManagementTests(MariaDbDatabaseFixture fixture) : ClusteringSchemaManagementTests(fixture), IClassFixture<MariaDbDatabaseFixture>;

