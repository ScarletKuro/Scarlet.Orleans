# Scarlet.Orleans.Clustering.EntityFrameworkCore

Orleans clustering over EF Core, with the Orleans tables in your own `DbContext` and migrations: the membership table
silos use to find each other, and the gateway list clients use to find silos. Uses the same schema as
`Scarlet.Orleans.Clustering.AdoNet`, so either can use a database created by the other. Requires .NET 10 and Orleans
10.4 or later.

> **Preview.** Tested against every supported engine in CI with the Orleans clustering TestKit, including its
> model-based suite and concurrent snapshot reads, and with real silos and clients, but with no production deployments
> yet.

## Setup

Add the Orleans tables to a context, register its factory, and use it on silos and on clients:

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansClusteringModel();
}

builder.Services.AddDbContextFactory<AppDbContext>(options => options.UseNpgsql(connectionString));
builder.UseOrleans(silo => silo.UseScarletEntityFrameworkCoreClustering<AppDbContext>());

// On a client, with the same context factory registered:
builder.UseOrleansClient(client => client.UseScarletEntityFrameworkCoreClustering<AppDbContext>());
```

A context can map other Scarlet.Orleans components too, such as persistence with `ApplyOrleansPersistenceModel()`; they
share the `orleans_schema_history` table.

## Schema

Create the tables with your migrations: run `dotnet ef migrations add AddOrleansClustering`, then record the schema
version at the end of the generated `Up` method:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    // ... tables EF generated from the model
    migrationBuilder.ApplyOrleansClusteringSchema(1);
}
```

At startup the provider checks the recorded version (`SchemaMode.Validate`, the default) and never changes the schema.
When a later package version needs a newer schema, its release notes give the version to record in your next migration.

## Options

| Option | Default | Meaning |
|---|---|---|
| `SchemaMode` | `Validate` | `Validate` checks the recorded schema version at startup; `None` skips the check. |
| `IsDuplicateKeyError` | built in | Recognises duplicate-key errors of a driver the provider does not know, when silos create the cluster's version row. |

## Behaviour

- **One version, one transaction.** Every membership change is a compare-and-swap of the cluster's version row, made
  in the same transaction as the silo row it inserts or updates, so it commits exactly one new version or nothing.
- **Consistent reads.** Reads return the version and the silo rows from one snapshot, in a `REPEATABLE READ`
  transaction that reads the version row first. No provider-specific SQL is involved, so any relational provider works.
- **Heartbeats** are one `ExecuteUpdate` of the silo's own row, with no read and no new version, and never move its
  liveness time backwards.
- **Identities.** Rows are keyed by 128-bit hashes of the cluster ID and the silo address with its generation, verified
  on every read and write.
- Writes ignore the context's execution strategy, such as `EnableRetryOnFailure`, and are repeated only when the
  database reports that they were not applied; after a broken connection Orleans reads the table again. Reads use the
  strategy.

`MySql.EntityFrameworkCore` runs on Oracle's MySql.Data, which has known bugs: concurrent TLS connection opens can
corrupt its state, after which every open in the process fails, and after a failover some calls can hang on dead
connections. Prefer Microting's provider for MySQL; see the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

Tested providers, diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
