# Scarlet.Orleans

Relational storage providers for [Microsoft Orleans](https://github.com/dotnet/orleans) 10, in two flavours over one
schema: plain **ADO.NET** (NativeAOT compatible) and **EF Core**. Persistence and reminders are available today;
clustering, the grain directory and streaming are planned ([design](community-relational-providers-design.md)).

> **Status: preview.** The packages are tested against every supported engine in CI, including real connection failures
> and the Orleans persistence and reminders TestKits, but have no production deployments yet. Try them where a lost write would be
> annoying rather than catastrophic, and report what you find.

| Package | For |
|---|---|
| `Scarlet.Orleans.Persistence.AdoNet` | Grain persistence over ADO.NET: SQL Server, PostgreSQL, MySQL, MariaDB, SQLite. NativeAOT compatible. |
| `Scarlet.Orleans.Persistence.EntityFrameworkCore` | Grain persistence over EF Core, with the Orleans tables in your own `DbContext` and migrations. |
| `Scarlet.Orleans.Reminders.AdoNet` | The reminder table over ADO.NET, on the same engines. NativeAOT compatible. |
| `Scarlet.Orleans.Reminders.EntityFrameworkCore` | The reminder table over EF Core, in your own `DbContext` and migrations. |

All require .NET 10 and Orleans 10.4 or later. Either flavour can use a database created by the other, and the
components can share one database. The sections below use persistence; [Reminders](#reminders) shows the differences.

## ADO.NET

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

**Schema.** `SchemaMode` decides what happens at silo startup:

| Mode | Behaviour |
|---|---|
| `Validate` (default) | Checks the `orleans_schema_history` table and fails startup, naming the scripts to run, if the schema is missing or outdated. |
| `Apply` | Creates the schema or applies pending scripts, under a database lock, so many silos can start at once. Needs DDL permissions. |
| `None` | No checks. |

The scripts are embedded in the package and also shipped under `schema/` in the NuGet package, so a DBA can review
and run them by hand, in version order. Each script records itself in `orleans_schema_history`, which `Validate`
checks, so silos without DDL rights can then start in `Validate` mode. When the provider applies a script itself, it
also records the script's checksum, and fails startup if a shipped script was ever edited.

**SQLite.** Concurrent writers need SQLite's write-ahead log, because in its default journal readers block writers and
writes fail with "database is locked". `Apply` switches the database to WAL; otherwise run `PRAGMA journal_mode = WAL`
once, and the provider warns until you do.

## EF Core

Add the Orleans tables to a context, register its factory, and point the provider at it:

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansPersistenceModel();
}

builder.Services.AddDbContextFactory<AppDbContext>(options => options.UseNpgsql(connectionString));
builder.UseOrleans(silo => silo.AddScarletEntityFrameworkCoreGrainStorageAsDefault<AppDbContext>());
```

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

Contexts are created per operation from `IDbContextFactory<TContext>`, because Orleans calls storage concurrently.
Tested providers: SQL Server, Npgsql, SQLite, and for MySQL [Microting](https://www.nuget.org/packages/Microting.EntityFrameworkCore.MySql)
(the maintained fork of Pomelo) or Oracle's `MySql.EntityFrameworkCore`. Other relational providers work; see
[Supported databases](#supported-databases).

## Reminders

Reminders are set up like persistence, with the same data sources, contexts and schema modes:

```csharp
builder.UseOrleans(silo => silo
    .UseScarletAdoNetReminderService(options =>
    {
        options.DataSource = dataSource;
        options.SchemaMode = SchemaMode.Apply;
    }));

// EF Core: modelBuilder.ApplyOrleansRemindersModel() in OnModelCreating, and
// migrationBuilder.ApplyOrleansRemindersSchema(1) in the migration that adds the tables.
builder.UseOrleans(silo => silo.UseScarletEntityFrameworkCoreReminderService<AppDbContext>());
```

A context can map both components: call `ApplyOrleansPersistenceModel()` and `ApplyOrleansRemindersModel()`, and record
each component's version in the migration. Options: `DataSource`, `DataSourceServiceKey`, `Engine` and `SchemaMode`
for ADO.NET; `SchemaMode` and `IsDuplicateKeyError` for EF Core, with the meanings in [Options](#options).

- **Upserts** replace a reminder's schedule whatever its ETag, as Orleans requires, and issue a new ETag. **Removes**
  check the ETag.
- **Identities.** Reminders are keyed by a 128-bit hash of the service, grain and reminder name, verified on every
  read and write, so long grain IDs and names work.
- **Schedules** are stored as UTC ticks, exact on every engine. A local start time is converted to UTC; an unspecified
  one is taken as UTC, as Orleans creates them.

## Options

| Option | Flavour | Default | Meaning |
|---|---|---|---|
| `DataSource`, `DataSourceServiceKey` | ADO.NET | from DI | The `DbDataSource` to use. |
| `Engine` | ADO.NET | inferred | `SqlServer`, `PostgreSql`, `MySql`, `MariaDb` or `Sqlite`. |
| `SchemaMode` | both | `Validate` | What to do with the schema at startup. EF Core has `None` and `Validate` only. |
| `DeleteStateOnClear` | both | `false` | Clearing state deletes the row, instead of keeping it with no payload and a new ETag. |
| `MaxPayloadSize` | both | none | Largest serialized state a write accepts, in bytes. A larger one fails before reaching the database, with an error naming the grain. Set it below your database's limit, for example MySQL's `max_allowed_packet` (64 MiB by default). |
| `IsDuplicateKeyError` | EF Core | built in | Recognises duplicate-key errors of a driver the provider does not know (see below). |
| `GrainStorageSerializer` | both | Orleans | The serializer of grain state. Under NativeAOT, use a source-generated one. |
| `InitStage` | both | `ApplicationServices` | Silo lifecycle stage of the schema check. |

## Behaviour

- **Optimistic concurrency.** Every write checks the grain's ETag, a GUID generated by the provider, and fails with
  `InconsistentStateException` if another activation wrote first. ETags cannot overflow, and an old one matching
  again is practically impossible.
- **Keys.** Each state is stored under a 128-bit hash of its full identity (service, state name, grain type and key, as
  raw bytes), and every read and write verifies the full identity, so grain keys of any length and content work.
- **Retries.** A write is repeated only when the database reports that the attempt was not applied, such as a deadlock.
  After a broken connection a write's outcome is unknown, so the error surfaces to Orleans instead of being retried into
  a false conflict. In the EF Core flavour, writes ignore the context's execution strategy for the same reason; reads
  use it.
- **Transactions.** Storage operations run outside any ambient `TransactionScope`, so a caller's rollback cannot undo a
  write whose new ETag the grain already holds.
- **Cancellation.** A canceled call does nothing and leaves the grain state as it was; a write that already committed
  is still reported.

## Diagnostics

Everything is published under the name `Scarlet.Orleans`:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("Scarlet.Orleans"))
    .WithTracing(tracing => tracing.AddSource("Scarlet.Orleans"));
```

| Instrument | Type | Tags |
|---|---|---|
| `scarlet.orleans.persistence.duration` | histogram, seconds | `orleans.storage.name`, `orleans.storage.operation` (`read`, `write`, `clear`), `outcome` (`success`, `conflict`, `collision`, `payload_too_large`, `canceled`, `error`) |
| `scarlet.orleans.persistence.payload.size` | histogram, bytes | `orleans.storage.name` |
| `scarlet.orleans.reminders.duration` | histogram, seconds | `orleans.reminders.operation` (`read`, `read_grain`, `read_range`, `upsert`, `remove`, `clear`), `outcome` (`success`, `collision`, `canceled`, `error`) |
| `scarlet.orleans.retries` | counter | `error` |

Each operation is also a trace span, such as `persistence write` with the grain ID, state name and outcome, or
`reminders upsert`; retries appear as span events. Conflicts are logged at debug level, since concurrent
activations make them normal. Other failures propagate to Orleans, which logs them.

## Supported databases

| Engine | Versions in CI | ADO.NET drivers | EF Core providers |
|---|---|---|---|
| PostgreSQL | 14, 16, 17 | Npgsql | Npgsql.EntityFrameworkCore.PostgreSQL |
| SQL Server | 2019, 2022 | Microsoft.Data.SqlClient | Microsoft.EntityFrameworkCore.SqlServer |
| MySQL | 8.4 | MySqlConnector, MySql.Data | Microting.EntityFrameworkCore.MySql, MySql.EntityFrameworkCore |
| MariaDB | 10.6, 11.4 | MySqlConnector | Microting.EntityFrameworkCore.MySql |
| SQLite | bundled | Microsoft.Data.Sqlite | Microsoft.EntityFrameworkCore.Sqlite |

The EF Core flavour also accepts any other relational provider, on a best-effort basis: its duplicate-key errors are
recognised by their standard SQLSTATE `23505`, or by your own `IsDuplicateKeyError` rule; otherwise the provider checks
for the conflicting row after a failed insert, which reports a conflicting row deleted in between as the driver's
error rather than as a conflict.

**NativeAOT.** The ADO.NET flavour is trimming- and AOT-compatible, and its test suites run as a NativeAOT executable
against every engine in CI. An Orleans 10.4 silo itself cannot start under NativeAOT yet, so this guarantee matters once
Orleans supports it. MySql.Data relies on reflection and works under JIT only; use MySqlConnector for AOT.

## Performance

On the same machine and databases, the ADO.NET flavour matches or beats the in-box Orleans ADO.NET provider in latency
and concurrent throughput on PostgreSQL and SQL Server, and is within run-to-run noise of it on MySQL. The EF Core
flavour costs about 5 to 15% over ADO.NET. See [bench](bench/README.md) to
run the comparison on your own infrastructure.

## Not yet supported

- **Moving from the in-box Orleans ADO.NET providers.** The schemas differ, and there is no import tool yet; start new
  deployments on these providers.
- Table name and schema options; the tables are `orleans_grain_state`, `orleans_reminder` and `orleans_schema_history`
  in the connection's default schema.

## Building and testing

```sh
dotnet build Scarlet.Orleans.slnx
dotnet test --solution Scarlet.Orleans.slnx
```

SQLite tests always run. Each server engine runs when its administrative connection string is set:
`SCARLET_TEST_POSTGRESQL`, `SCARLET_TEST_SQLSERVER`, `SCARLET_TEST_MYSQL`, `SCARLET_TEST_MARIADB`. The tests create and
drop their own databases.

## License

MIT
