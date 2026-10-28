# Scarlet.Orleans.GrainDirectory.AdoNet

An Orleans grain directory over plain ADO.NET, for SQL Server, PostgreSQL, MySQL, MariaDB and SQLite. NativeAOT
compatible. Requires .NET 10 and Orleans 10.4 or later.

> **Preview.** Tested against every supported engine in CI, including real connection failures and concurrent
> register/unregister load, but with no production deployments yet.

## Setup

Create a `DbDataSource` with your driver, and register the directory, as the default or under a name:

```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);                       // PostgreSQL, Npgsql
// SqlClientFactory.Instance.CreateDataSource(connectionString)                  // SQL Server, Microsoft.Data.SqlClient
// new MySqlDataSource(connectionString)                                         // MySQL or MariaDB, MySqlConnector
// MySqlClientFactory.Instance.CreateDataSource(connectionString)                // MySQL, Oracle's MySql.Data
// SqliteFactory.Instance.CreateDataSource(connectionString)                     // SQLite, Microsoft.Data.Sqlite

builder.UseOrleans(silo => silo
    .UseScarletAdoNetGrainDirectoryAsDefault(options =>
    {
        options.DataSource = dataSource;
        options.SchemaMode = SchemaMode.Apply;
    }));

// Or only for some grains, chosen with [GrainDirectory("durable")]:
// silo.AddScarletAdoNetGrainDirectory("durable", options => options.DataSource = dataSource);
```

Instead of `options.DataSource`, the provider resolves a `DbDataSource` from dependency injection, keyed by
`options.DataSourceServiceKey` if set. The engine is inferred from the driver; set `options.Engine = "MariaDb"` for
MariaDB, which shares MySqlConnector with MySQL.

Oracle's MySql.Data has known bugs: concurrent TLS connection opens can corrupt its state, after which every open in
the process fails, and after a failover some calls can hang on dead connections. Prefer MySqlConnector; see the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

## Schema

`SchemaMode` decides what happens at silo startup:

| Mode | Behaviour |
|---|---|
| `Validate` (default) | Checks the `orleans_schema_history` table and fails startup, naming the scripts to run, if the schema is missing or outdated. |
| `Apply` | Creates the schema or applies pending scripts, under a database lock, so many silos can start at once. Needs DDL permissions. |
| `None` | No checks. |

The scripts are embedded in the package and also shipped under `schema/` in it, so a DBA can review and run them by
hand, in version order. Each script records itself in `orleans_schema_history`, so silos without DDL rights can then
start in `Validate` mode. On SQLite, `Apply` also switches the database to its write-ahead log, which concurrent
writers need.

## Options

| Option | Default | Meaning |
|---|---|---|
| `DataSource`, `DataSourceServiceKey` | from DI | The `DbDataSource` to use. |
| `Engine` | inferred | `SqlServer`, `PostgreSql`, `MySql`, `MariaDb` or `Sqlite`. |
| `SchemaMode` | `Validate` | What to do with the schema at startup. |
| `InitStage` | `RuntimeInitialize` | Silo lifecycle stage of the schema check. |

## Behaviour

- **First writer wins.** Registering a grain that already has an entry returns that entry and changes nothing, unless
  the caller names it as the previous activation, in which case it is replaced in one statement.
- **Unregistering** removes an entry only if its activation ID matches. A failed silo's entries are removed in batches
  of 500, so the cleanup never holds locks on a large part of the table.
- **No upserts.** Registration is a plain insert, so on MySQL concurrent registrations do not deadlock on gap locks, as
  `INSERT … ON DUPLICATE KEY UPDATE` does. Deadlocks that do occur are retried, since the server rolled them back.
- **Identities.** Entries are keyed by a 128-bit hash of the cluster ID, directory name and grain ID, verified on every
  read and write, so grain IDs of any length work. Each directory name and cluster has its own entries.
- A write is repeated only when the database reports that it was not applied; after a broken connection the error
  surfaces to Orleans, which retries. Operations run outside any ambient `TransactionScope`.

The table can be shared with `Scarlet.Orleans.GrainDirectory.EntityFrameworkCore`, and the database with the other
Scarlet.Orleans components. Diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
