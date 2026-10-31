# Scarlet.Orleans.Clustering.AdoNet

Orleans clustering over plain ADO.NET, for SQL Server, PostgreSQL, MySQL, MariaDB and SQLite: the membership table
silos use to find each other and agree on who is alive, and the gateway list clients use to find silos. NativeAOT
compatible. Requires .NET 10 and Orleans 10.4 or later.

> **Preview.** Tested against every supported engine in CI with the Orleans clustering TestKit, including its
> model-based suite and concurrent snapshot reads, and with real silos and clients, but with no production deployments
> yet.

## Setup

Create a `DbDataSource` with your driver, and use it on silos and on clients:

```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);                       // PostgreSQL, Npgsql
// SqlClientFactory.Instance.CreateDataSource(connectionString)                  // SQL Server, Microsoft.Data.SqlClient
// new MySqlDataSource(connectionString)                                         // MySQL or MariaDB, MySqlConnector
// MySqlClientFactory.Instance.CreateDataSource(connectionString)                // MySQL, Oracle's MySql.Data
// SqliteFactory.Instance.CreateDataSource(connectionString)                     // SQLite, Microsoft.Data.Sqlite

builder.UseOrleans(silo => silo
    .UseScarletAdoNetClustering(options =>
    {
        options.DataSource = dataSource;
        options.SchemaMode = SchemaMode.Apply;
    }));

// On a client:
builder.UseOrleansClient(client => client
    .UseScarletAdoNetClustering(options => options.DataSource = dataSource));
```

Silos and clients of one cluster share `ClusterOptions.ClusterId`; several clusters can share the tables. Instead of
`options.DataSource`, the provider resolves a `DbDataSource` from dependency injection, keyed by
`options.DataSourceServiceKey` if set. The engine is inferred from the driver; set `options.Engine = "MariaDb"` for
MariaDB, which shares MySqlConnector with MySQL.

Oracle's MySql.Data has known bugs: concurrent TLS connection opens can corrupt its state, after which every open in
the process fails, and after a failover some calls can hang on dead connections. Prefer MySqlConnector; see the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

## Schema

`SchemaMode` decides what happens when the membership table starts:

| Mode | Behaviour |
|---|---|
| `Validate` (default) | Checks the `orleans_schema_history` table and fails startup, naming the scripts to run, if the schema is missing or outdated. |
| `Apply` | Creates the schema or applies pending scripts, under a database lock, so many silos can start at once. Needs DDL permissions. Silos only. |
| `None` | No checks. |

Clients never change the schema, so they accept only `Validate` and `None`. The scripts are embedded in the package and
also shipped under `schema/` in it, so a DBA can review and run them by hand, in version order. Each script records
itself in `orleans_schema_history`, so silos without DDL rights can then start in `Validate` mode. On SQLite, `Apply`
also switches the database to its write-ahead log, which concurrent writers need.

## Options

| Option | Default | Meaning |
|---|---|---|
| `DataSource`, `DataSourceServiceKey` | from DI | The `DbDataSource` to use. |
| `Engine` | inferred | `SqlServer`, `PostgreSql`, `MySql`, `MariaDb` or `Sqlite`. |
| `SchemaMode` | `Validate` | What to do with the schema at startup. |

## Behaviour

- **One version, one transaction.** Each cluster has a version row. Every membership change is a compare-and-swap of
  that row, made in the same transaction as the silo row it inserts or updates, so it commits exactly one new version
  or nothing. A silo that registers twice, or updates with stale data, gets `false` and no side effects, on every
  engine.
- **Consistent reads.** Reads return the version and the silo rows from one snapshot, which the membership protocol
  relies on. On SQL Server this needs no database setting such as `READ_COMMITTED_SNAPSHOT`.
- **Heartbeats** are one write to the silo's own row, with no read and no new version. A silo's liveness time never
  moves backwards, even when an update carries an older one.
- **Cleanup** removes dead silos whose start, heartbeat and suspicion times are all before Orleans' cutoff, leaving the
  version unchanged.
- **No deadlocks between clusters or silos.** Every write of a cluster's rows takes its version row first, and writes
  run at `READ COMMITTED`, so MySQL takes no gap locks that reach into other clusters' rows.
- **Identities.** Rows are keyed by 128-bit hashes of the cluster ID and the silo address with its generation, verified
  on every read and write. Times are stored as UTC ticks, so they come back exactly as written.
- A write is repeated only when the database reports that it was not applied, such as a deadlock; after a broken
  connection the error surfaces, and Orleans reads the table again before its next write. Operations run outside any
  ambient `TransactionScope`.

The tables can be shared with `Scarlet.Orleans.Clustering.EntityFrameworkCore`, and the database with the other
Scarlet.Orleans components. Diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
