# Scarlet.Orleans.Persistence.EntityFrameworkCore

Orleans grain persistence over EF Core, with the Orleans tables in your own `DbContext` and migrations. Uses the same
schema as `Scarlet.Orleans.Persistence.AdoNet`, so either can use a database created by the other. Requires .NET 10
and Orleans 10.4 or later.

> **Preview.** Tested against every supported engine in CI, including real connection failures and the Orleans
> persistence TestKit, but with no production deployments yet.

## Setup

Add the Orleans tables to a context, register its factory, and point the provider at it:

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansPersistenceModel();
}

builder.Services.AddDbContextFactory<AppDbContext>(options => options.UseNpgsql(connectionString));
builder.UseOrleans(silo => silo.AddScarletEntityFrameworkCoreGrainStorageAsDefault<AppDbContext>());
```

Contexts are created per operation from `IDbContextFactory<TContext>`, because Orleans calls storage concurrently.

## Schema

Create the tables with your migrations: run `dotnet ef migrations add AddOrleansPersistence`, then record the schema
version at the end of the generated `Up` method:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    // ... tables EF generated from the model
    migrationBuilder.ApplyOrleansPersistenceSchema(1);
}
```

At startup the provider checks the recorded version (`SchemaMode.Validate`, the default) and never changes the schema.
When a later package version needs a newer schema, its release notes give the version to record in your next migration.

## Options

| Option | Default | Meaning |
|---|---|---|
| `SchemaMode` | `Validate` | `Validate` checks the recorded schema version at startup; `None` skips the check. |
| `DeleteStateOnClear` | `false` | Clearing state deletes the row, instead of keeping it with no payload and a new ETag. |
| `MaxPayloadSize` | none | Largest serialized state a write accepts, in bytes. A larger one fails before reaching the database, with an error naming the grain. |
| `IsDuplicateKeyError` | built in | Recognises duplicate-key errors of a driver the provider does not know. |
| `GrainStorageSerializer` | Orleans | The serializer of grain state. |
| `InitStage` | `ApplicationServices` | Silo lifecycle stage of the schema check. |

## Providers

Tested: SQL Server, Npgsql, SQLite, and for MySQL [Microting](https://www.nuget.org/packages/Microting.EntityFrameworkCore.MySql)
(the maintained fork of Pomelo) or Oracle's `MySql.EntityFrameworkCore`. Other relational providers work on a
best-effort basis: their duplicate-key errors are recognised by the standard SQLSTATE `23505`, or by your own
`IsDuplicateKeyError` rule; otherwise the provider checks for the conflicting row after a failed insert.

`MySql.EntityFrameworkCore` runs on Oracle's MySql.Data, which has a known bug: concurrent TLS connection opens can
corrupt its state, after which every open in the process fails. Prefer Microting's provider; see the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#supported-databases).

## Behaviour

Every write checks the grain's ETag and fails with `InconsistentStateException` if another activation wrote first.
Writes ignore the context's execution strategy, such as `EnableRetryOnFailure`: after a broken connection a write's
outcome is unknown, so it is repeated only when the database reports that it was not applied. Reads use the strategy.
Operations run outside any ambient `TransactionScope`.

Diagnostics, supported versions and the full behaviour are described in the
[repository](https://github.com/ScarletKuro/Scarlet.Orleans#readme).
