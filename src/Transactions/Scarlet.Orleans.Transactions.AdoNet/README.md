# Scarlet.Orleans.Transactions.AdoNet

Storage for Orleans transactions over plain ADO.NET, for SQL Server, PostgreSQL, MySQL, MariaDB and SQLite: the
`ITransactionalStateStorage` behind every `[TransactionalState]`. NativeAOT compatible. Requires .NET 10 and Orleans
10.4 or later.

> **Preview.** Tested against every supported engine with the Orleans transactions TestKit, its storage runner and its
> grain runners in a silo, but with no production deployments yet.

## Setup

Create a `DbDataSource` with your driver, and add the storage under the name your grains use:

```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);                       // PostgreSQL, Npgsql
// SqlClientFactory.Instance.CreateDataSource(connectionString)                  // SQL Server, Microsoft.Data.SqlClient
// new MySqlDataSource(connectionString)                                         // MySQL or MariaDB, MySqlConnector
// MySqlClientFactory.Instance.CreateDataSource(connectionString)                // MySQL, Oracle's MySql.Data
// SqliteFactory.Instance.CreateDataSource(connectionString)                     // SQLite, Microsoft.Data.Sqlite

builder.UseOrleans(silo => silo
    .UseTransactions()
    .AddScarletAdoNetTransactionalStateStorage("TransactionStore", options =>
    {
        options.DataSource = dataSource;
        options.SchemaMode = SchemaMode.Apply;
    }));

public sealed class AccountGrain(
    [TransactionalState("balance", "TransactionStore")] ITransactionalState<Balance> balance) : Grain, IAccountGrain
```

`AddScarletAdoNetTransactionalStateStorageAsDefault` serves `[TransactionalState("balance")]` without a storage name.
Instead of `options.DataSource`, the provider resolves a `DbDataSource` from dependency injection, keyed by
`options.DataSourceServiceKey` if set. The engine is inferred from the driver; set `options.Engine = "MariaDb"` for
MariaDB, which shares MySqlConnector with MySQL.

Without this package, Orleans stores transactional state through the grain storage of the same name, rewriting every
pending version on each store. This package writes only what changed.

## Schema

`SchemaMode` decides what happens when the silo starts:

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
| `GrainStorageSerializer` | Orleans' default | Serializes the states. The transaction metadata always uses the Orleans serializer, because it holds grain references. |

## Behaviour

- **One store, one transaction.** Each store replaces the state row, checking its ETag, and writes, commits and
  removes the pending versions in the same database transaction, so a load sees the state before or after a store,
  never in between. A stale ETag fails with `InconsistentStateException`, which Orleans treats as a storage conflict.
- **Commits copy nothing.** The committed state is the pending version it committed: a commit moves the committed
  sequence number and deletes the versions it made obsolete.
- **Consistent loads.** A load reads the state row and its versions from one snapshot. On SQL Server this needs no
  database setting such as `READ_COMMITTED_SNAPSHOT`.
- **Identities.** Rows are keyed by a 128-bit hash of the service ID, state name and grain ID, verified on every read
  and write.
- A store is repeated only when the database reports that it was not applied, such as a deadlock; after a broken
  connection the error surfaces, and Orleans loads the state again before its next store. Operations run outside any
  ambient `TransactionScope`.

## Storage errors

When a store fails, Orleans aborts the transaction and returns the driver's exception to the caller as its inner
exception. Orleans serializes exceptions only from the `Microsoft`, `System` and `Azure` namespaces by default, so
callers on another silo or a client receive an Npgsql, MySqlConnector or MySql.Data error as a `CodecNotFoundException`
instead. To get the real error, allow the driver's namespace on silos and clients:

```csharp
services.Configure<ExceptionSerializationOptions>(options =>
{
    options.SupportedNamespacePrefixes.Add("Npgsql");
    options.SupportedNamespacePrefixes.Add("MySql"); // MySqlConnector and MySql.Data
});
```

The tables can be shared with `Scarlet.Orleans.Transactions.EntityFrameworkCore`, and the database with the other
Scarlet.Orleans components. Diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
