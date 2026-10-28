# Scarlet.Orleans.GrainDirectory.EntityFrameworkCore

An Orleans grain directory over EF Core, with the Orleans tables in your own `DbContext` and migrations. Uses the same
schema as `Scarlet.Orleans.GrainDirectory.AdoNet`, so either can use a database created by the other. Requires .NET 10
and Orleans 10.4 or later.

> **Preview.** Tested against every supported engine in CI, including real connection failures and concurrent
> register/unregister load, but with no production deployments yet.

## Setup

Add the Orleans tables to a context, register its factory, and register the directory, as the default or under a name:

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansGrainDirectoryModel();
}

builder.Services.AddDbContextFactory<AppDbContext>(options => options.UseNpgsql(connectionString));
builder.UseOrleans(silo => silo.UseScarletEntityFrameworkCoreGrainDirectoryAsDefault<AppDbContext>());

// Or only for some grains, chosen with [GrainDirectory("durable")]:
// silo.AddScarletEntityFrameworkCoreGrainDirectory<AppDbContext>("durable");
```

A context can map other Scarlet.Orleans components too, such as persistence with `ApplyOrleansPersistenceModel()`; they
share the `orleans_schema_history` table.

## Schema

Create the tables with your migrations: run `dotnet ef migrations add AddOrleansGrainDirectory`, then record the schema
version at the end of the generated `Up` method:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    // ... tables EF generated from the model
    migrationBuilder.ApplyOrleansGrainDirectorySchema(1);
}
```

At startup the provider checks the recorded version (`SchemaMode.Validate`, the default) and never changes the schema.
When a later package version needs a newer schema, its release notes give the version to record in your next migration.

## Options

| Option | Default | Meaning |
|---|---|---|
| `SchemaMode` | `Validate` | `Validate` checks the recorded schema version at startup; `None` skips the check. |
| `IsDuplicateKeyError` | built in | Recognises duplicate-key errors of a driver the provider does not know. |
| `InitStage` | `RuntimeInitialize` | Silo lifecycle stage of the schema check. |

## Behaviour

- **First writer wins.** Registering a grain that already has an entry returns that entry and changes nothing, unless
  the caller names it as the previous activation, in which case it is replaced in one statement.
- **Unregistering** removes an entry only if its activation ID matches. A failed silo's entries are removed in batches
  of 500.
- **Identities.** Entries are keyed by a 128-bit hash of the cluster ID, directory name and grain ID, verified on every
  read and write, so grain IDs of any length work.
- Writes ignore the context's execution strategy, such as `EnableRetryOnFailure`, and are repeated only when the
  database reports that they were not applied. Reads use the strategy.

`MySql.EntityFrameworkCore` runs on Oracle's MySql.Data, which has known bugs: concurrent TLS connection opens can
corrupt its state, after which every open in the process fails, and after a failover some calls can hang on dead
connections. Prefer Microting's provider for MySQL; see the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

Tested providers, diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
