# Scarlet.Orleans.Reminders.AdoNet

The Orleans reminder table over plain ADO.NET, for SQL Server, PostgreSQL, MySQL, MariaDB and SQLite. NativeAOT
compatible. Requires .NET 10 and Orleans 10.4 or later.

> **Preview.** Tested against every supported engine in CI, including real connection failures and the Orleans
> reminders TestKit, but with no production deployments yet.

## Setup

Create a `DbDataSource` with your driver, and point the reminder service at it:

```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);                       // PostgreSQL, Npgsql
// SqlClientFactory.Instance.CreateDataSource(connectionString)                  // SQL Server, Microsoft.Data.SqlClient
// new MySqlDataSource(connectionString)                                         // MySQL or MariaDB, MySqlConnector
// MySqlClientFactory.Instance.CreateDataSource(connectionString)                // MySQL, Oracle's MySql.Data
// SqliteFactory.Instance.CreateDataSource(connectionString)                     // SQLite, Microsoft.Data.Sqlite

builder.UseOrleans(silo => silo
    .UseScarletAdoNetReminderService(options =>
    {
        options.DataSource = dataSource;
        options.SchemaMode = SchemaMode.Apply;
    }));
```

Instead of `options.DataSource`, the provider resolves a `DbDataSource` from dependency injection, keyed by
`options.DataSourceServiceKey` if set. The engine is inferred from the driver; set `options.Engine = "MariaDb"` for
MariaDB, which shares MySqlConnector with MySQL.

Oracle's MySql.Data has known bugs: concurrent TLS connection opens can corrupt its state, after which every open in
the process fails, and after a failover some calls can hang on dead connections. Prefer MySqlConnector; see
the [repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

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

## Behaviour

- **Upserts** replace a reminder's schedule whatever its ETag, as Orleans requires, and issue a new ETag. **Removes**
  check the ETag.
- **Identities.** Reminders are keyed by a 128-bit hash of the service, grain and reminder name, verified on every read
  and write, so long grain IDs and names work.
- **Schedules** are stored as UTC ticks, exact on every engine. A local start time is converted to UTC; an unspecified
  one is taken as UTC, as Orleans creates them.
- A write is repeated only when the database reports that it was not applied; after a broken connection the error
  surfaces to the reminder service. Operations run outside any ambient `TransactionScope`.

The table can be shared with `Scarlet.Orleans.Reminders.EntityFrameworkCore`, and the database with the other
Scarlet.Orleans components. Diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
