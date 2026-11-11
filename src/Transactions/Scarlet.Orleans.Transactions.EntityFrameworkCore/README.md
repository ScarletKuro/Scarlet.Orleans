# Scarlet.Orleans.Transactions.EntityFrameworkCore

Storage for Orleans transactions over EF Core, with the Orleans tables in your own `DbContext` and migrations: the
`ITransactionalStateStorage` behind every `[TransactionalState]`. Uses the same schema as
`Scarlet.Orleans.Transactions.AdoNet`, so either can use a database created by the other. Requires .NET 10 and Orleans
10.4 or later.

> **Preview.** Tested against every supported engine with the Orleans transactions TestKit, its storage runner and its
> grain runners in a silo, but with no production deployments yet.

## Setup

Add the Orleans tables to a context, register its factory, and add the storage under the name your grains use:

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansTransactionsModel();
}

builder.Services.AddDbContextFactory<AppDbContext>(options => options.UseNpgsql(connectionString));
builder.UseOrleans(silo => silo
    .UseTransactions()
    .AddScarletEntityFrameworkCoreTransactionalStateStorage<AppDbContext>("TransactionStore"));
```

`AddScarletEntityFrameworkCoreTransactionalStateStorageAsDefault<AppDbContext>()` serves `[TransactionalState("state")]`
without a storage name. A context can map other Scarlet.Orleans components too, such as persistence with
`ApplyOrleansPersistenceModel()`; they share the `orleans_schema_history` table.

## Schema

Create the tables with your migrations: run `dotnet ef migrations add AddOrleansTransactions`, then record the schema
version at the end of the generated `Up` method:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    // ... tables EF generated from the model
    migrationBuilder.ApplyOrleansTransactionsSchema(1);
}
```

At startup the provider checks the recorded version (`SchemaMode.Validate`, the default) and never changes the schema.
When a later package version needs a newer schema, its release notes give the version to record in your next migration.

## Options

| Option | Default | Meaning |
|---|---|---|
| `SchemaMode` | `Validate` | `Validate` checks the recorded schema version at startup; `None` skips the check. |
| `IsDuplicateKeyError` | built in | Recognises duplicate-key errors of a driver the provider does not know, when a state is first stored. |
| `GrainStorageSerializer` | Orleans' default | Serializes the states. The transaction metadata always uses the Orleans serializer, because it holds grain references. |

## Behaviour

- **One store, one transaction.** Each store replaces the state row, checking its ETag, and writes, commits and
  removes the pending versions in the same database transaction, so a load sees the state before or after a store,
  never in between. A stale ETag fails with `InconsistentStateException`, which Orleans treats as a storage conflict.
- **Commits copy nothing.** The committed state is the pending version it committed: a commit moves the committed
  sequence number and deletes the versions it made obsolete.
- **Consistent loads.** A load reads the state row and its versions in one `REPEATABLE READ` transaction. No
  provider-specific SQL is involved, so any relational provider works.
- **Identities.** Rows are keyed by a 128-bit hash of the service ID, state name and grain ID, verified on every read
  and write.
- Stores ignore the context's execution strategy, such as `EnableRetryOnFailure`, and are repeated only when the
  database reports that they were not applied; after a broken connection Orleans loads the state again. Loads use the
  strategy.

## Performance

A store is several statements in one transaction, each its own round trip, because EF Core has no way to send them as
one batch without provider-specific SQL. `Scarlet.Orleans.Transactions.AdoNet` sends a store as one batch or command
per engine, and on the same server database it is two to four times faster: in local benchmarks the EF Core flavour
handled about a quarter of the ADO.NET flavour's transfers per second on PostgreSQL, and about half on SQL Server and
MySQL. On SQLite, whose single writer limits both, they are close. Both write
only what changed, a fifteenth to a twentieth of the bytes Orleans writes through grain storage. Choose this package
when your migrations and `DbContext` should own the Orleans tables, and the ADO.NET flavour when throughput matters;
both use the same tables.

## Storage errors

When a store fails, Orleans aborts the transaction and returns the provider's exception to the caller as its inner
exception. Orleans serializes exceptions only from the `Microsoft`, `System` and `Azure` namespaces by default, so
callers on another silo or a client receive an error from Npgsql, MySqlConnector or MySql.Data as a
`CodecNotFoundException` instead. To get the real error, allow the driver's namespace on silos and clients:

```csharp
services.Configure<ExceptionSerializationOptions>(options =>
{
    options.SupportedNamespacePrefixes.Add("Npgsql");
    options.SupportedNamespacePrefixes.Add("MySql"); // MySqlConnector and MySql.Data
});
```

`MySql.EntityFrameworkCore` runs on Oracle's MySql.Data, which has known bugs: concurrent TLS connection opens can
corrupt its state, after which every open in the process fails, and after a failover some calls can hang on dead
connections. Prefer Microting's provider for MySQL; see the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

Tested providers, diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
