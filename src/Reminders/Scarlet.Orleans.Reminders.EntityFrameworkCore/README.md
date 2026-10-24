# Scarlet.Orleans.Reminders.EntityFrameworkCore

The Orleans reminder table over EF Core, with the Orleans tables in your own `DbContext` and migrations. Uses the same
schema as `Scarlet.Orleans.Reminders.AdoNet`, so either can use a database created by the other. Requires .NET 10 and
Orleans 10.4 or later.

> **Preview.** Tested against every supported engine in CI, including real connection failures and the Orleans
> reminders TestKit, but with no production deployments yet.

## Setup

Add the Orleans tables to a context, register its factory, and point the reminder service at it:

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansRemindersModel();
}

builder.Services.AddDbContextFactory<AppDbContext>(options => options.UseNpgsql(connectionString));
builder.UseOrleans(silo => silo.UseScarletEntityFrameworkCoreReminderService<AppDbContext>());
```

A context can map other Scarlet.Orleans components too, such as persistence with `ApplyOrleansPersistenceModel()`; they
share the `orleans_schema_history` table.

## Schema

Create the tables with your migrations: run `dotnet ef migrations add AddOrleansReminders`, then record the schema
version at the end of the generated `Up` method:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    // ... tables EF generated from the model
    migrationBuilder.ApplyOrleansRemindersSchema(1);
}
```

At startup the provider checks the recorded version (`SchemaMode.Validate`, the default) and never changes the schema.
When a later package version needs a newer schema, its release notes give the version to record in your next migration.

## Options

| Option | Default | Meaning |
|---|---|---|
| `SchemaMode` | `Validate` | `Validate` checks the recorded schema version at startup; `None` skips the check. |
| `IsDuplicateKeyError` | built in | Recognises duplicate-key errors of a driver the provider does not know. |

## Behaviour

- **Upserts** replace a reminder's schedule whatever its ETag, as Orleans requires, and issue a new ETag. **Removes**
  check the ETag.
- **Identities.** Reminders are keyed by a 128-bit hash of the service, grain and reminder name, verified on every read
  and write, so long grain IDs and names work.
- **Schedules** are stored as UTC ticks, exact on every engine.
- Writes ignore the context's execution strategy, such as `EnableRetryOnFailure`, and are repeated only when the
  database reports that they were not applied. Reads use the strategy.

`MySql.EntityFrameworkCore` runs on Oracle's MySql.Data, which has known bugs: concurrent TLS connection opens can
corrupt its state, after which every open in the process fails, and after a failover some calls can hang on dead
connections. Prefer Microting's provider for MySQL; see the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

Tested providers, diagnostics and supported versions are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
