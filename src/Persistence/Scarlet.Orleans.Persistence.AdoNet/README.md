# Scarlet.Orleans.Persistence.AdoNet

Orleans grain persistence over plain ADO.NET, for SQL Server, PostgreSQL, MySQL, MariaDB and SQLite. NativeAOT
compatible. Requires .NET 10 and Orleans 10.4 or later.

> **Preview.** Tested against every supported engine in CI, including real connection failures and the Orleans
> persistence TestKit, but with no production deployments yet.

## Setup

Create a `DbDataSource` with your driver, and point the provider at it:

```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);                       // PostgreSQL, Npgsql
// SqlClientFactory.Instance.CreateDataSource(connectionString)                  // SQL Server, Microsoft.Data.SqlClient
// new MySqlDataSource(connectionString)                                         // MySQL or MariaDB, MySqlConnector
// MySqlClientFactory.Instance.CreateDataSource(connectionString)                // MySQL, Oracle's MySql.Data
// SqliteFactory.Instance.CreateDataSource(connectionString)                     // SQLite, Microsoft.Data.Sqlite

builder.UseOrleans(silo => silo
    .AddScarletAdoNetGrainStorageAsDefault(options =>
    {
        options.DataSource = dataSource;
        options.SchemaMode = SchemaMode.Apply;
    }));
```

Instead of `options.DataSource`, the provider resolves a `DbDataSource` from dependency injection, keyed by
`options.DataSourceServiceKey` if set. The engine is inferred from the driver; set `options.Engine = "MariaDb"` for
MariaDB, which shares MySqlConnector with MySQL.

Oracle's MySql.Data has a known bug: concurrent TLS connection opens can corrupt its state, after which every open in
the process fails. Prefer MySqlConnector; see the
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
start in `Validate` mode. When the provider applies a script itself, it also records the script's checksum, and fails
startup if a shipped script was ever edited.

**SQLite.** Concurrent writers need SQLite's write-ahead log, because in its default journal readers block writers and
writes fail with "database is locked". `Apply` switches the database to WAL; otherwise run `PRAGMA journal_mode = WAL`
once, and the provider warns until you do.

## Options

| Option | Default | Meaning |
|---|---|---|
| `DataSource`, `DataSourceServiceKey` | from DI | The `DbDataSource` to use. |
| `Engine` | inferred | `SqlServer`, `PostgreSql`, `MySql`, `MariaDb` or `Sqlite`. |
| `SchemaMode` | `Validate` | What to do with the schema at startup. |
| `DeleteStateOnClear` | `false` | Clearing state deletes the row, instead of keeping it with no payload and a new ETag. |
| `MaxPayloadSize` | none | Largest serialized state a write accepts, in bytes. A larger one fails before reaching the database, with an error naming the grain. Set it below your database's limit, for example MySQL's `max_allowed_packet` (64 MiB by default). |
| `GrainStorageSerializer` | Orleans | The serializer of grain state. Under NativeAOT, use a source-generated one. |
| `InitStage` | `ApplicationServices` | Silo lifecycle stage of the schema check. |

## Behaviour

Every write checks the grain's ETag and fails with `InconsistentStateException` if another activation wrote first.
States are keyed by a 128-bit hash of their full identity, verified on every read and write, so grain keys of any
length work. A write is repeated only when the database reports that it was not applied; after a broken connection the
error surfaces to Orleans. Operations run outside any ambient `TransactionScope`.

The tables can be shared with `Scarlet.Orleans.Persistence.EntityFrameworkCore` and the other Scarlet.Orleans
components. Diagnostics, supported versions and the full behaviour are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
