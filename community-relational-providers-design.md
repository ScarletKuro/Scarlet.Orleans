# Scarlet relational providers for Orleans: ADO.NET and EF Core

A design for a community set of Orleans providers, published as `Scarlet.Orleans.*`, backed by relational databases, offered in two flavours over **one shared schema and one shared core**:

- **EF Core**, for applications that already use EF and want generated migrations and one data-access stack.
- **ADO.NET**, for lean hosts (console apps, devices, containers) and NativeAOT, with the fewest dependencies.

It covers clustering, grain directory, persistence, reminders and streaming. It is informed by the problems found and fixed in the in-box `Orleans.*.AdoNet` providers, and by the EF Core providers proposed in dotnet/orleans#8654.

Status: design draft. Nothing here is implemented yet.

---

## 1. Goals and non-goals

### Goals

1. **Same behaviour in both flavours.** ADO.NET and EF Core share the schema, the concurrency rules and the provider logic. A deployment can switch flavour without migrating data.
2. **Correct before fast.** Every safety property (membership conditional writes, single activation registration, optimistic concurrency on state, at-least-once streaming) is enforced by the database and verified by conformance tests on every supported database version.
3. **No silent failure modes.** No counters that overflow, no tokens that repeat, no message that can become unreachable.
4. **NativeAOT-capable ADO.NET flavour.** No reflection-based driver loading on the main path, trimming-safe code, `DbDataSource` first.
5. **Schema evolution as a first-class feature.** Every schema change ships with a migration that is idempotent, tested from a frozen older schema, and runnable through the database's own client.
6. **Testable time.** All provider time comes from `TimeProvider`, so tests can drive visibility timeouts, expiry and eviction deterministically.

### Non-goals

- **Drop-in compatibility with the in-box ADO.NET tables.** The schema is new. An import tool moves existing data across (section 11).
- **User-editable SQL stored in the database.** The in-box providers keep their queries in an `OrleansQuery` table so users can edit them. That made fixes hard to ship (stale queries survive upgrades) and hid bugs. Here, SQL lives in code. Extension points are explicit and typed (section 4.9).
- **Every database under the sun.** The supported set is listed and tested (section 9). Anything else is best effort.

---

## 2. Lessons from the in-box ADO.NET providers

Each row is a real problem found in `Orleans.*.AdoNet`, and the rule this design adopts so it cannot recur.

| Problem in the in-box provider | Where | Rule adopted here |
|---|---|---|
| Grain state `Version` is a 32-bit `INT` incremented in SQL. Writes fail with an overflow at `int.MaxValue` (dotnet/orleans#8379). A wrap-around fix still lets a stale ETag match again after 2³¹ writes. | Persistence | GUID ETags generated in C# (section 4.2). |
| Grain type was not part of the storage key, so grains of different types with the same ID shared state (dotnet/orleans#8746, fixed opt-in by `KeyFormat = GrainTypeQualified`). | Persistence | The key always includes service, grain type, state name and grain ID (section 4.3). |
| Reminders `GrainId VARCHAR(150)`, so long grain IDs (generic grains) fail (dotnet/orleans#9653). | Reminders | Keys are fixed-size hashes. Full values are stored unbounded and verified (section 4.3). |
| MySQL 8.4 does not evaluate `ROW_COUNT()` inside a `WHERE` clause as the previous statement's count, so a duplicate membership insert reported success and bumped the version. | Clustering (MySQL) | Never compute a guard from a previous statement's row count. Each conditional write is a single statement, or a transaction whose outcome the client checks (section 5.1). |
| MySQL 8.4 rejects `CREATE TEMPORARY TABLE … SELECT … FOR UPDATE`, so streaming consumers never received messages. MariaDB accepted it, and CI only ran MariaDB. | Streaming (MySQL) | No temp-table batch selection. Test every supported version of every engine, MariaDB and MySQL separately (section 9). |
| Positional arguments passed in the wrong order to `EvictStreamMessages`. Dead letters deleted after ~17 minutes instead of 7 days, poisoned messages never evicted. | Streaming (MySQL) | No positional cross-routine calls. All SQL is parameterised by name in C# (section 4.8). |
| The C# `EvictStreamMessagesAsync` binds `MaxCount` while the stored query expects `@BatchSize`. Fails on every database if called. | Streaming | Every query has a typed C# wrapper, and every wrapper is exercised against every engine (section 9). |
| Stream message IDs used as cache sequence tokens. Redelivered and late-committed messages carry lower IDs, so the cache treats them as missed and a purge confirms them undelivered. | Streaming | Cache positions are assigned by the receiver and only ever increase. Database IDs are never cache tokens (section 5.5). |
| Two cached attempts of the same message produced a confirmation batch with a duplicate key, which every engine rejected. | Streaming | Confirmations are coalesced per message, keeping the latest receipt (section 5.5). |
| Grain directory `INSERT … ON DUPLICATE KEY UPDATE` deadlocks under concurrent register/unregister on MySQL 8.4 (InnoDB gap locks). | Grain directory (MySQL) | Registration avoids upserts that take gap locks, classifies deadlocks as retryable, and is chaos-tested on every engine (section 5.2). |
| `FailStreamMessage` takes only the message ID, not the dequeue count, so a failure reported for an old attempt can move or redeliver a message that has since been dequeued again by another receiver. | Streaming | Every operation on a dequeued message carries its receipt, `Fail` included (section 5.5). |
| Clustering joins an ambient `TransactionScope` (dotnet/orleans#7765). | All | Provider operations suppress ambient transactions by default (section 4.6). |
| The MySQL streaming migration only worked through the test harness's script splitter, not through the `mysql` client. | Migrations | Migrations are tested through the real client semantics (section 8). |
| SQLite stored versions past `int.MaxValue` that C# could not read back, leaving grains unreadable after upgrade. | Persistence (SQLite, Oracle) | Column types and C# types match exactly, enforced by schema tests (section 8). |

The pattern across almost all of these: **dialect-specific SQL, written by hand per engine, with behaviour that diverged silently between engines.** The design below shrinks that surface and tests what remains on every engine.

---

## 3. Architecture

```
Orleans contracts      IMembershipTable  IGatewayListProvider  IGrainDirectory
                       IGrainStorage     IReminderTable        IQueueAdapterFactory ...
                                   │
Component logic        src/{Component}/Shared: linked into both packages of the component
                       - Orleans contract implementation for that component
                       - component rules: membership versions, first-writer-wins registration,
                         state ETag checks, reminder ranges, streaming receiver (positions,
                         receipts, confirmation coalescing), batch container, failure handler
                       - raw SQL that both flavours run (section 7.1)
                                   │
Store contracts        IMembershipStore  IGrainDirectoryStore  IGrainStateStore
(narrow, internal)     IReminderStore    IStreamQueueStore
                                   │
Backends               Scarlet.Orleans.{Component}.AdoNet       Scarlet.Orleans.{Component}.EntityFrameworkCore
                       - per-dialect SQL text in C#             - DbContext model, no shipped migrations
                       - DbDataSource, no reflection            - ExecuteUpdate / ExecuteDelete / FromSql
                       - SQL Server, PostgreSQL, MySQL,         - SQL Server, PostgreSQL, MySQL, SQLite
                         MariaDB, SQLite, Oracle (optional)
                                   │
Shared infrastructure  src/Shared: linked into every package, not a package itself
                       - dialect abstraction, DbDataSource execution
                       - key hashing, ETag generation, error classification and retry policy
                       - schema ledger, migration runner and version checks
                       - TimeProvider plumbing, options base types, diagnostics
```

### 3.1 Linked source instead of shared packages

Shared code is compiled into each package from linked source files (`<Compile Include="..\..\Shared\**\*.cs" LinkBase="Shared" />`), the same technique the in-box Orleans ADO.NET providers use for `src/AdoNet/Shared`. There is no core package.

- **Shared infrastructure** (`src/Shared`) is linked into all ten packages.
- **Component logic** (`src/{Component}/Shared`) is linked into that component's ADO.NET and EF Core packages, and nowhere else.

Why linked source rather than a shared package:

- **No public API to maintain.** For one package to consume another package's types, those types must be public, and every public helper becomes a compatibility commitment. Linked code stays `internal` to each assembly, so it can be refactored freely. The public surface of each package is only its hosting extensions, its options and a few component-specific interfaces (section 4.9).
- **No version coupling.** Each package is self-contained. A user who installs `Scarlet.Orleans.Clustering.AdoNet` 1.2 next to `Scarlet.Orleans.Persistence.AdoNet` 1.3 cannot end up with a mismatched shared assembly.
- **No drift.** One source file is compiled into every assembly that uses it, so a fix reaches every package on the next build. All packages are released together from one repository, with one version.
- **Components stay independent.** A package only links the shared infrastructure and its own component's logic. The grain directory package contains no clustering code, and no package pulls in another component's Orleans dependencies.

Rules that keep linked source manageable:

- **Every linked type is `internal`.** Public types are declared once per package, never in linked files. Two assemblies each exposing a public type with the same name would be a different type in each, and a user referencing both would get ambiguous-type errors. For that reason no public type is shared between packages at all: options use only .NET types (section 3.5).
- **No `#if` per component in shared files.** The in-box providers switch namespaces per assembly with `#if CLUSTERING_ADONET` and similar. Here, shared infrastructure is genuinely component-agnostic, and component code lives in component folders, so no conditional compilation is needed.
- **One component per test project, or tests link the source themselves.** A test project with `InternalsVisibleTo` access to several component assemblies would see the same internal type in each and fail with ambiguity errors (`CS0433`).
- **Shared tables are stable across versions.** Several assemblies, possibly at different versions, use `orleans_schema_history` (section 8.1). Its own layout only ever changes in backward-compatible ways.

### 3.2 What belongs where

- **Shared infrastructure is cross-cutting only.** Keys, ETags, error classification, retries, schema versioning and migrations, time and diagnostics. It contains no component types.
- **Each component owns its semantics, once.** The component's linked logic implements its Orleans interface and its rules. Both of the component's packages compile that same code, and add only their store.
- **Store contracts are narrow, atomic and internal.** Each method is one atomic database operation with a precisely specified outcome, for example `TryInsertMemberAsync(member, expectedVersion) → Inserted | VersionConflict | AlreadyExists`. The atomicity guarantee is part of the contract and is verified by the store conformance suite (section 9).
- **Backends own execution.** ADO.NET backends hold SQL per dialect. The EF backend uses the EF model and EF's set-based operations, plus the component's shared raw SQL where EF cannot express an operation (section 7.1). Neither contains Orleans logic.

### 3.3 Why this split

- Fixes like confirmation coalescing, monotonic positions or key derivation exist **once** in source.
- The backend-specific code is exactly the code that has to be correct per engine, so that is where testing concentrates.
- Both flavours run the same Orleans-level conformance tests (the Orleans TestKits) **and** the same store-level conformance tests.

### 3.4 Packages

Packages are split **by component and flavour**, like the in-box Orleans providers (`Microsoft.Orleans.Clustering.AdoNet` and so on), but **not by database**. All IDs start with `Scarlet.Orleans.`. Reserve the `Scarlet.*` ID prefix on nuget.org before the first release.

**Why split by component.** A single package for all five components would force every user to take `Microsoft.Orleans.Streaming` and `Microsoft.Orleans.Reminders`, even when they only use clustering and persistence. Splitting keeps dependencies, trimmed size and AOT surface to what is actually used.

**Why not by database.** Each package contains every supported engine:

- **ADO.NET:** the dialects are SQL text plus error mapping, with no reference to any driver. The user supplies the driver through the `DbDataSource` they pass in, which the AOT design already requires (section 4.11).
- **EF Core:** the packages ship the model only, configured with provider-neutral APIs so that every provider generates the same tables, and no EF provider package is referenced. A component uses EF Core APIs for everything EF Core can express, and knows the engine only if it has raw statements that need engine-specific syntax (section 7.1). Persistence has none, so it works with any relational EF Core provider; the tested ones are SQL Server, Npgsql, SQLite and, for MySQL, Microting (`Microting.EntityFrameworkCore.MySql`, the maintained fork of Pomelo, over MySqlConnector) and Oracle's `MySql.EntityFrameworkCore` (over MySql.Data). The ADO.NET flavour likewise accepts both MySqlConnector and MySql.Data. The application's own EF migrations create the Orleans tables, after `ApplyOrleansModel()` adds them to its model. No generated EF migrations are shipped. A per-engine EF extension package is added only if a future feature genuinely needs a provider-specific API.

| Package | Contents | Orleans dependencies | NativeAOT |
|---|---|---|---|
| **ADO.NET flavour** (all engines in each package) | | | |
| `Scarlet.Orleans.Clustering.AdoNet` | Clustering, schema scripts and migrations | Orleans runtime | Yes |
| `Scarlet.Orleans.GrainDirectory.AdoNet` | Grain directory, schema scripts and migrations | Orleans runtime | Yes |
| `Scarlet.Orleans.Persistence.AdoNet` | Persistence, schema scripts and migrations | Orleans runtime | Yes |
| `Scarlet.Orleans.Reminders.AdoNet` | Reminders, schema scripts and migrations | Orleans reminders | Yes |
| `Scarlet.Orleans.Streaming.AdoNet` | Streaming, schema scripts and migrations | Orleans streaming | Yes |
| **EF Core flavour** (all engines in each package) | | | |
| `Scarlet.Orleans.Clustering.EntityFrameworkCore` | Clustering EF model and store, `ApplyOrleansModel()` | Orleans runtime, EF Core | No |
| `Scarlet.Orleans.GrainDirectory.EntityFrameworkCore` | Grain directory EF model and store | Orleans runtime, EF Core | No |
| `Scarlet.Orleans.Persistence.EntityFrameworkCore` | Persistence EF model and store | Orleans runtime, EF Core | No |
| `Scarlet.Orleans.Reminders.EntityFrameworkCore` | Reminders EF model and store | Orleans reminders, EF Core | No |
| `Scarlet.Orleans.Streaming.EntityFrameworkCore` | Streaming EF model and store, running the shared raw SQL for dequeue and eviction | Orleans streaming, EF Core | No |
| **Tools** | | | |
| `Scarlet.Orleans.Relational.Tool` | .NET tool `orleans-db`: `migrate`, `status`, `script` (section 8.1) and `import` from the in-box Orleans tables (section 11) | The ADO.NET packages | Not applicable |

That is 11 packages: ten runtime packages and a tool. No runtime package references another package of this set, and there is no shared assembly between them. Users install only the components and flavour they use, and the names mirror the in-box Orleans packages.

The store conformance kit is not published. It lives in the repository's test projects, which link the store contracts. Store contracts are internal, so writing a third-party backend against them is not supported. If that need appears, a deliberately designed public package can expose them later.

**Trade-off: error classification without driver references.** Deciding whether an error is a duplicate key, a deadlock or a transient failure (section 4.7) cannot use driver exception types. Npgsql and MySqlConnector populate `DbException.SqlState`, but SQL Server's client reports numeric error codes instead. Each engine's dialect therefore includes a classifier that reads what `DbException` exposes, and the classifier is pluggable (section 3.5) for drivers where that is not enough. Verify each supported driver before relying on this. Microsoft.Data.Sqlite turned out to expose its result code only on `SqliteException` (`DbException.ErrorCode` is the generic `E_FAIL`), so SQLite statements are written to need no classification: inserts use `ON CONFLICT DO NOTHING` and check the row count, and the driver retries a busy database itself.

### 3.5 Choosing the engine and driver

Drivers are never discovered. The application creates the `DbDataSource` with its own driver, which makes the driver an ordinary compile-time reference in the application. The packages only use `DbDataSource`, `DbConnection` and `DbCommand`, so any ADO.NET driver works, including drivers that do not exist yet, and nothing is loaded by name or by reflection.

The SQL still differs per database, so a component needs two pieces of configuration: the data source (how to connect) and the engine (which SQL to speak).

```csharp
silo.AddScarletAdoNetGrainStorage("Default", options =>
{
    options.DataSource = dataSource;                            // any driver
    options.Engine = "PostgreSql";                              // optional when inferable; validated at startup
    options.IsTransientError = ex => ex.SqlState == "40001";    // optional classifier overrides
    options.IsConflictError = ex => ex.SqlState == "23505";
});
```

**The public surface uses only .NET types.** Engine names are strings and error overrides are `Func<DbException, bool>` predicates, so no package needs a shared public type, and there is no shared assembly between packages at all (section 3.1). Each package declares its own options class, and options were never shared anyway.

**Engine selection is a closed choice.** `Engine` accepts only the engines that ship with a dialect: `SqlServer`, `PostgreSql`, `MySql`, `MariaDb`, `Sqlite`, `Oracle`. Any other value fails at startup with the list of supported names (for example: unknown engine `Postgres`; supported: `SqlServer`, `PostgreSql`, …). The property's XML documentation lists the names for IntelliSense. There is deliberately no public dialect interface:

- A dialect is every SQL statement of every component plus its error mapping. Made public, every new statement or refactor in any component would break third-party implementations.
- A third-party dialect would not run this project's conformance suites, yet its bugs would look like bugs in these packages.
- Adding a database therefore means contributing a dialect, with its tests, to the repository.

**Inference is a convenience.** When `Engine` is not set, it is inferred from the connection type name for well-known drivers (for example `Npgsql.NpgsqlConnection` → `PostgreSql`), with MySQL and MariaDB told apart from the server version. That uses a string comparison, not reflection. An unrecognised driver requires `Engine` to be set, and startup fails with a clear message otherwise.

**Compatible databases.** Databases that speak a supported engine's protocol and SQL (for example CockroachDB or YugabyteDB with `PostgreSql`, TiDB with `MySql`) can be used by choosing that engine, at the user's risk. Connecting always works, but features such as skip-locked reads must also be supported there. The documentation lists which compatible databases have been run through the conformance suites.

**Error classification is the one extension point.** Each engine has a built-in classifier that maps a `DbException` to `Conflict`, `Transient` or `Fatal` (section 4.7). For a driver whose errors the built-in classifier cannot read, `IsTransientError` and `IsConflictError` override it. Each is consulted first, and the built-in classifier decides when it returns `false`; anything neither recognises is `Fatal`.

**Why strings and predicates instead of an enum and an interface.** A `RelationalEngine` enum or an `IRelationalErrorClassifier` interface would have to be declared once and shared, because a public type in linked source is a different type in each assembly (section 3.1). That means a shared public package that every package depends on, whose types can never change, and which reintroduces version coupling between packages. Plain .NET types avoid all of that. The cost is that a misspelled engine name is caught at startup rather than at compile time.

---

## 4. Cross-cutting decisions

### 4.1 One schema, versioned

- A single schema definition per engine, identical between flavours. The EF model is configured to produce exactly that schema, and a schema-equality test enforces it (section 8).
- The `orleans_schema_history` ledger (section 8.1) records the applied schema version per component. Each package release declares the **range** of schema versions it supports, and each schema version declares the range of package versions it supports. Startup fails fast with a clear message when the database is outside that range. Ranges, rather than a single expected version, are what make rolling upgrades work in both directions: a new silo runs against the old schema until the expand migration is applied, and old silos keep running against the expanded schema until they are replaced (section 8.1).
- Table and column names are lower-case snake case on every engine, so no quoting is needed. This avoids the PostgreSQL case-folding problem seen in dotnet/orleans#9578.
- Schema-qualified names are supported through an option, not through connection-string tricks.

### 4.2 ETags: GUIDs generated in C#

- Every row with optimistic concurrency has an `etag` column holding a GUID stored as 16 bytes on every engine (`binary(16)`, `bytea`, `raw(16)`, or a blob on SQLite). Native GUID types are avoided because drivers disagree on their byte order (`uniqueidentifier`, MySQL GUID formats).
- The shared infrastructure generates the new ETag with `Guid.CreateVersion7()` and passes it as a parameter. Writes are `UPDATE … SET etag = @new … WHERE … AND etag = @expected`.
- Consequences: it cannot overflow, a stale ETag matching again has negligible probability (a UUIDv7 carries a millisecond timestamp plus 74 random bits, so a repeat needs two writes in the same millisecond to draw the same 74 bits, compared with a certainty after 2³¹ writes for a wrapping 32-bit counter), the SQL is identical on every engine, and the ETag is opaque to callers, which is all Orleans requires.
- Because the client generates the new ETag before writing, a read-back can often settle an uncertain write: if the row holds the ETag it generated, the write committed; if it still holds the expected ETag, it did not. Any other value leaves the outcome unknown, because another writer may have changed the row after this write committed (section 4.7).
- The only counter that stays a counter is the **membership table version**, because Orleans' membership protocol defines it as an ordered `int` (`TableVersion.Version`). It only changes when silos join, leave or change status, so overflow is not a practical concern. It is checked and incremented atomically together with the row it guards (section 5.1).

### 4.3 Keys: fixed-size hashes plus verified full values

- Primary keys are 128-bit hashes (`XxHash128`) of the full identity, stored as `binary(16)`. This keeps indexes narrow (the original in-box design goal) without limiting identity length.
- Full identity values are stored losslessly in unbounded columns and compared on every read, so a hash collision can never return the wrong row.
- Writes verify identity too. Every update and upsert includes the full identity columns in its `WHERE` clause, and an insert that hits an existing key whose identity differs is reported as a hash collision, never treated as "already exists" or overwritten. A collision is astronomically unlikely with 128-bit hashes, but its behaviour is defined: the operation fails with a dedicated error naming both identities, and nothing is written. This matters most for reminder upserts, which would otherwise silently replace another grain's reminder (section 5.4).
- Identity always includes every distinguishing part:
  - persistence: service ID, grain type, state name, grain ID;
  - reminders: service ID, grain ID, reminder name;
  - grain directory: cluster ID, grain ID;
  - membership: cluster ID, silo address and generation.
- **Identities are bytes, not text.** An Orleans `GrainId` is a `GrainType` and an `IdSpan` key, both arbitrary byte sequences; they are usually UTF-8 but nothing guarantees it, and decoding invalid UTF-8 replaces bytes with U+FFFD, which would collapse distinct identities. So:
  - the hash input is the original bytes of each part (`GrainType.AsSpan()`, `IdSpan.AsSpan()`, and the UTF-8 of parts that are .NET strings, such as service ID and state name), each with a length prefix, in a documented order, so it never depends on runtime string hashing or text decoding;
  - one `grain_identity` column stores exactly the hashed bytes (`varbinary(max)`, `bytea`, `longblob`, `blob`), and comparisons are byte comparisons, which also avoids collation and case-folding differences between engines;
  - plain text columns (`service_id`, `state_name`, `grain_id` holding `GrainId.ToString()`) are for humans and queries, and are never used for identity.
- A test round-trips identities with invalid UTF-8, embedded NUL and bytes that differ only after decoding, and asserts each stays distinct.

### 4.4 Serialization

- Grain state and reminder data use the configured `IGrainStorageSerializer`.
- Payloads are stored in a binary column plus a `content_type` column (for example `application/json`, `application/x-orleans`).
- Optional: on engines with a native JSON type, a JSON payload can be stored in a JSON column instead, so it is queryable and readable (dotnet/orleans#8417). That is a schema option chosen at creation time, not a runtime toggle.
- **NativeAOT.** The Orleans serializer is source-generated and works under AOT. A JSON serializer based on `System.Text.Json` needs a source-generated `JsonSerializerContext` covering every state type. The JSON storage serializer accepts a `JsonSerializerContext` (or `JsonTypeInfo` per type) in its options. Without one it uses reflection-based serialization, and its constructor carries `[RequiresUnreferencedCode]` and `[RequiresDynamicCode]`, so AOT builds get a warning at the call site.

### 4.5 Time

- All "now" values come from `TimeProvider`, passed into SQL as parameters. This keeps tests deterministic and avoids each engine's clock function differences (for example MySQL's whole-second `UTC_TIMESTAMP()` against `UTC_TIMESTAMP(6)`).
- Timestamps are stored as UTC with microsecond precision (`datetime2(6)`, `timestamptz`, `datetime(6)`).
- Clock skew between silos only shifts visibility and expiry by the skew. The design document for each component states which comparisons tolerate skew.

### 4.6 Transactions and isolation

- Provider operations run inside `TransactionScope(TransactionScopeOption.Suppress)` by default, so an application's ambient transaction never captures Orleans infrastructure writes (dotnet/orleans#7765). An option allows opting in, for users who want persistence writes in their own transaction.
- Every multi-statement operation uses an explicit transaction with a documented isolation level. Default is READ COMMITTED; any operation that needs more says so in code.
- Lock ordering is fixed and documented per component (for example "version row, then member row"), so concurrent operations cannot deadlock by acquiring locks in opposite orders.

### 4.7 Errors and retries

- Each backend maps engine errors to a small set of outcomes: `Conflict` (duplicate key, ETag mismatch), `Transient` (deadlock victim, serialization failure, connection reset, lock timeout) and `Fatal`.
- **Retry the whole operation, after an explicit rollback.** Engines differ in what an error undoes. PostgreSQL aborts the whole transaction on any error. MySQL and MariaDB roll back only the failed statement on a lock wait timeout (unless `innodb_rollback_on_timeout` is set), keeping earlier statements and their locks; a deadlock rolls back the whole transaction. SQL Server keeps the transaction open after a lock timeout (error 1222) unless `XACT_ABORT` is on. So the shared infrastructure never retries a single statement inside a transaction. On any `Transient` error it rolls the transaction back explicitly (discarding the connection if the rollback fails), then retries the whole operation from its first statement, with bounded exponential backoff and jitter.
- **Which errors are retried.** An error the server returned before `COMMIT` was sent, followed by that explicit rollback, leaves the database unchanged, so the operation is retried. A connection failure before the command was sent is the same. A connection failure after `COMMIT`, or after an autocommit statement, was sent is ambiguous and is handled below.
- **Commit ambiguity.** Such a failure leaves the outcome unknown. A blind retry is wrong there: a conditional write would see its own committed ETag and report a false conflict, and an insert would see its own row and report "already exists". Unknown outcomes are resolved where that is possible, and reported as unknown where it is not:
  - **ETag-guarded writes** (membership, grain state, reminders) re-read the row. If it holds the ETag the client generated for this write (section 4.2), the write committed and the call succeeds; if it still holds the expected ETag, the write did not commit and is retried. Any other value, or a missing row, does **not** prove a conflict: this write may have committed and another writer may have changed or deleted the row since. Persistence avoids reaching this state rather than reporting it: it never repeats a write after a connection failure, so an unmatched write is only ever settled after a repeat by a driver, and the original failure, or a cancellation, surfaces instead (section 7). When it does occur, persistence reports `InconsistentStateException`, which has the effect an "outcome unknown" error would have, since the grain re-reads its state either way; for membership, Orleans re-reads the table. No extra operation log is kept to settle it.
  - **Deletes by key and ETag** follow the same rule: a row still holding the expected ETag means the delete did not commit; a missing or changed row is "outcome unknown".
  - **Stream enqueue** carries a client-generated `message_key` with a unique constraint, so a retry that hits its own key while the message still exists is recognised as the same message (section 5.5).
  - **Operations that are idempotent by nature** (confirm by receipt, release by receipt, evict) are retried as is; repeating them changes nothing.
  - Operations that fit none of these surface the error to Orleans rather than guess.
- The conformance suite injects connection failures before send, after send and after commit for every store operation and asserts the outcome (section 9).
- Retries are visible: each retry emits a metric and a debug log with the engine error code.

### 4.8 SQL in code, parameterised by name

- ADO.NET SQL lives in C# constants per dialect, one per store method. No stored procedures. That removes the `DELIMITER` problems and the cross-routine positional calls, and makes every statement visible in code review.
- Parameters are always bound by name through typed helpers. No positional parameters.
- Each statement has exactly one C# call site (the store method), so there is no text and binding to drift apart.

### 4.9 Extension points

Instead of editable stored queries, a small, deliberate public surface per package:

- `IGrainStorageSerializer`, already part of Orleans.
- Table name and schema options.
- `IsTransientError` and `IsConflictError` (section 3.5), for drivers whose errors the built-in classifier cannot read. Engine selection is a closed choice, not an extension point.

Store contracts and SQL are internal (section 3.1), so changing a component's SQL means forking it. That is deliberate: it keeps every shipped statement covered by the conformance suites.

### 4.10 Hosting

Each package adds its own entry points, in the style of the in-box Orleans providers. Linked shared types are internal and differ per assembly (section 3.1), so there is no single builder spanning all components. Instead, the connection is registered once in dependency injection and every component resolves it:

```csharp
// ADO.NET flavour: one DbDataSource, shared by every component.
builder.Services.AddNpgsqlDataSource(connectionString);

builder.UseOrleans(silo => silo
    .UseScarletAdoNetClustering()
    .UseScarletAdoNetGrainDirectory()
    .AddScarletAdoNetGrainStorage("Default")
    .UseScarletAdoNetReminders()
    .AddScarletAdoNetStreams("Events", streams => streams.Queues = 8));
```

```csharp
// EF Core flavour: the application's own DbContext, with the Orleans tables added through ApplyOrleansModel().
builder.UseOrleans(silo => silo
    .UseScarletEntityFrameworkCoreClustering<AppDbContext>()
    .AddScarletEntityFrameworkCoreGrainStorage<AppDbContext>("Default"));
```

- Each entry point resolves `DbDataSource` (ADO.NET) or `IDbContextFactory<TContext>` (EF Core, section 7) from dependency injection by default. An optional service key, or an explicit instance in the options, selects a different database per component.
- The ADO.NET flavour takes a `DbDataSource`, which is trimming- and AOT-friendly and pools connections the right way.
- The EF flavour can either use its own `DbContext`, or add the Orleans model to the application's existing `DbContext` through `modelBuilder.ApplyOrleansModel()`, so Orleans tables appear in the application's own migrations. That second option is the main reason an EF user would pick this flavour. Each EF package contributes its own `ApplyOrleans…Model()` extension, so only the tables of installed components are added.

### 4.11 NativeAOT and trimming

The ADO.NET flavour aims to be fully NativeAOT- and trimming-compatible from the first release. Compatibility is enforced by the build, not checked at the end.

- **Build enforcement.** The ADO.NET packages set `<IsAotCompatible>true</IsAotCompatible>`, which enables the trimming, single-file and AOT analyzers, and treat their warnings (`IL2xxx`, `IL3xxx`) as errors. A change that is not AOT-safe fails the build.
- **No dynamic driver loading.** No `DbProviderFactories` lookup by invariant name, no reflection, no dynamic code. The driver comes from the supplied `DbDataSource`.
- **Conformance tests run as NativeAOT.** Rather than a separate smoke-test app, the store conformance kit and the Orleans TestKit suites are compiled into an xUnit v3 test project that is published as NativeAOT and run directly, per engine, in CI from the foundation milestone onwards (section 13). Trimming or AOT problems then surface in the paths that matter, including conflict and error paths, not only startup.
  - The same test sources compile into the regular (JIT) test project and the AOT one, so there is one copy of each test.
  - The AOT project contains only AOT-safe dependencies. Tests that need runtime-proxy mocking (NSubstitute, Moq) stay in the JIT project. The conformance suites need no mocks, because they run against real databases.
  - Database containers are started by CI and passed in as connection strings, unless the container library used is itself AOT-compatible.
  - At least one test builds a real silo through the public entry points (`UseOrleans` with every `UseScarletAdoNet…` / `AddScarletAdoNet…` extension), so hosting, dependency injection and options binding are trimmed and exercised too. **Blocked on Orleans:** an Orleans 10.4 silo cannot start under NativeAOT, because it resolves serializer codecs as open generics over value types from dependency injection (`IFieldCodec<ImmutableArray<GrainManifest>>`), which DI refuses without dynamic code. Orleans itself also reports trim and AOT warnings. Until Orleans supports it, the silo tests run under JIT only, and the AOT run covers everything below the silo: the storage contract, the stores on every engine, schema management and cancellation.
  - **Implemented** as `Scarlet.Orleans.Persistence.AdoNet.NativeAotTests` (`xunit.v3.aot`), published for `linux-x64` and run against PostgreSQL, SQL Server, MySQL and MariaDB in one CI job. Trim and AOT warnings in this repository's assemblies are reported individually and fail the publish; warnings that dependencies report about themselves do not. The Orleans TestKit is not in the AOT run yet: its Accordant model-based runner has not been checked for AOT.
- **Drivers.** Npgsql (good AOT support, `NpgsqlSlimDataSourceBuilder`), MySqlConnector (fully asynchronous, preferred over MySql.Data), Microsoft.Data.SqlClient, Microsoft.Data.Sqlite. MySql.Data relies on reflection and is supported under JIT only. Microsoft.Data.SqlClient 6.1 reports its own trim and AOT warnings. Each driver's AOT status is verified by the AOT test runs rather than assumed, and documented per engine.
- **Serialization.** See section 4.4: payload serializers must be source-generated under AOT.
- **The EF flavour and the tool are exempt.** They are not part of the AOT guarantee, and document their own status. The migration runner is linked into the ADO.NET packages (section 8.1), so it is covered by the guarantee.
- **EF flavour:** documented as not AOT-compatible until EF Core's AOT support (precompiled queries, still experimental) covers what the stores need.

### 4.12 Diagnostics

- `ActivitySource` and `Meter` per component: operation latency, outcome (`ok`, `conflict`, `transient`, `fatal`), retries, rows affected.
- Streaming adds queue depth, dequeue batch size, redeliveries, dead letters and eviction counts.

---

## 5. Components

### 5.1 Clustering

**Tables**

- `orleans_cluster_version`: one row per cluster with `cluster_id`, `version` (`int`), `timestamp`.
- `orleans_member`: one row per silo with `cluster_id`, `address`, `port`, `generation`, `silo_name`, `host_name`, `status`, `proxy_port`, `suspect_times`, `start_time`, `i_am_alive_time` and an `etag` (GUID).

**Guarantees** (the `IMembershipTable` contract)

- `InsertRow` succeeds only if the member does not exist **and** the table version still equals the expected version. Both conditions hold atomically, and the version is incremented in the same transaction.
- `UpdateRow` succeeds only if the member's ETag and the table version both match, atomically, with the version incremented.
- `UpdateIAmAlive` never moves `i_am_alive_time` backwards (`GREATEST`, or the engine's equivalent), and does not touch the version.
- `ReadAll` and `ReadRow` return a consistent snapshot: the table version and the member rows come from the same point in time. Orleans uses the returned version as the expected version of its next conditional write, and it reasons about the returned rows as the state at that version. A version read before a concurrent write, combined with rows read after it, would let a silo act on rows that do not match the version it holds.

**Implementation rules**

- Conditional writes are one transaction in a fixed lock order: lock the version row first (`UPDATE … WHERE cluster_id = @c AND version = @expected`), then insert or update the member row. The client checks the affected-row count of each statement itself. **No guard depends on `ROW_COUNT()`, `@@ROWCOUNT` or `FOUND` inside another statement.**
- A duplicate insert surfaces as a unique-key violation, which the backend classifies as `Conflict`. The transaction rolls back, so the version is unchanged.
- Snapshot reads need a per-engine guarantee, because "one statement" alone is not enough everywhere:
  - **PostgreSQL**: one statement joining the version row with the member rows. Every statement reads one snapshot, even at `READ COMMITTED`.
  - **MySQL, MariaDB**: the same statement; InnoDB's non-locking consistent read uses one snapshot per statement at `READ COMMITTED`, and per transaction at `REPEATABLE READ`.
  - **SQLite**: the same statement; a read transaction sees one snapshot.
  - **SQL Server**: default (locking) `READ COMMITTED` gives no point-in-time guarantee within a statement, so a joined read is not enough. The read runs in a transaction that first reads the version row `WITH (HOLDLOCK)` and then the member rows. Every versioned write locks the version row first (rule above), so it cannot commit between the two reads, and no database setting is required. When the database has `READ_COMMITTED_SNAPSHOT` or `ALLOW_SNAPSHOT_ISOLATION` on, the dialect uses a row-versioned read instead and takes no lock; it detects the setting at startup.
- Writes that do not change the version (`UpdateIAmAlive`, `CleanupDefunctSiloEntries`) do not affect the snapshot contract: Orleans does not tie `i_am_alive_time` to the version, and cleanup only removes rows of dead silos. Cleanup still locks the version row first, to keep a single lock order.
- Gateway listing reads active members with a proxy port in one indexed query.
- `CleanupDefunctSiloEntries` deletes dead members older than a cutoff, in bounded batches.

**Tests**

- The Orleans clustering TestKit and its model-based conformance suite, on every engine and version.
- An explicit test that a duplicate insert returns `false` with **no** side effects (version unchanged, rows unchanged). That is the test that caught the MySQL 8.4 bug.
- A snapshot test: concurrent writers insert and update members while readers call `ReadAll` in a loop, and every result must be internally consistent (the member rows are exactly those committed at the returned version).

### 5.2 Grain directory

**Table**

- `orleans_grain_directory`: `cluster_id`, `grain_id_hash` (key), `grain_id` (bytes, section 4.3), `grain_id_display`, `silo_address`, `activation_id`, `membership_version`, `created_on`.

**Guarantees** (the `IGrainDirectory` contract)

- `Register` is first-writer-wins: it returns the existing registration if one exists, otherwise inserts and returns the new one. It never overwrites another activation.
- `Unregister` removes the entry only if the activation ID matches.
- `UnregisterSilos` removes every entry for the given silos.

**Implementation rules**

- `Register` is `INSERT` with a plain duplicate-key failure handled as "already registered, read it back". No `INSERT … ON DUPLICATE KEY UPDATE`, `MERGE` or upsert that takes gap or range locks on the index.
- Run at READ COMMITTED to minimise gap locking where the engine supports it.
- Deadlock victims are `Transient` and retried. Because `Register` is idempotent from the caller's view, a retry is always safe.
- `UnregisterSilos` deletes in bounded batches ordered by key, so large silo failures do not hold locks for long.

**Tests**

- A conformance suite for first-writer-wins under concurrency.
- The concurrent register/lookup/unregister chaos test on every engine, including MySQL 8.4. It must run in CI for this component, not only as an excluded stress test, because it is the only test that exposes the gap-lock deadlock.

### 5.3 Persistence

**Table**

- `orleans_grain_state`: `key_hash` (key), `grain_identity` (bytes, section 4.3), `service_id`, `state_name`, `grain_id` (display text), `payload` (null once cleared), `etag`, `modified_on`. There is no `content_type`, since `IGrainStorageSerializer` does not report one.

**Guarantees** (the `IGrainStorage` contract)

- Read returns the payload and ETag, or "no record" with a null ETag.
- Write with a null ETag inserts and fails with `InconsistentStateException` if a row already exists.
- Write with an ETag updates only if the ETag matches, otherwise `InconsistentStateException`.
- Clear either nulls the payload and rotates the ETag, or deletes the row when `DeleteStateOnClear` is set. Both check the ETag.

**Implementation rules**

- Each operation is a single statement: `INSERT`, or `UPDATE … WHERE key_hash = @k AND etag = @expected`. The client checks the affected-row count.
- Every read verifies the full identity columns against the request, so a hash collision cannot return another grain's state.
- Payload size limits are checked in the component logic with a clear error, before the database rejects the write.

**Tests**

- The Orleans persistence TestKit, model-based conformance.
- Key isolation tests (types sharing a key, states of one grain, keys parsing to the same value, keys of different kinds).
- ETag tests: stale ETag rejected, concurrent writers, clear and delete paths.

### 5.4 Reminders

**Table**

- `orleans_reminder`: `service_id`, `grain_id_hash`, `reminder_name_hash` (key), `grain_id` (bytes, section 4.3), `grain_id_display`, `reminder_name`, `grain_hash` (`uint`, for range queries), `start_at`, `period`, `etag`.

**Guarantees** (the `IReminderTable` contract)

- Read by grain, read by name, read by hash range, upsert, remove with ETag, clear table.
- Range reads handle the wrap-around ranges Orleans produces (begin greater than end).

**Implementation rules**

- Upsert is insert-or-update keyed by the hash key, rotating the ETag. The update branch matches the full `grain_id` and `reminder_name` as well as the hash key, so a hash collision fails with the collision error (section 4.3) instead of replacing another grain's reminder. Remove checks the ETag.
- `grain_hash` is stored in a type that can hold the full `uint` range on every engine (`bigint` where there is no unsigned type).
- Index on `(service_id, grain_hash)` for range reads.
- `grain_id` and `reminder_name` are unbounded, which removes the 150-character limit (dotnet/orleans#9653).

**Tests**

- The Orleans reminders TestKit.
- Long grain IDs (generic grains), wrap-around ranges, ETag conflicts.
- A forced collision (a test-only hash that maps two reminders to one key) proves that upsert fails and leaves the existing reminder untouched.

### 5.5 Streaming

This is the component with the most logic, and where the in-box provider had the most bugs. The component logic (linked into both streaming packages) holds all the queue semantics; the backend only implements atomic queue operations.

**Tables**

- `orleans_stream_message`: `service_id`, `provider_id`, `queue_id`, `message_id` (`bigint`, from a sequence or identity), `message_key` (a client-generated GUID, unique, for idempotent enqueue), `dequeued`, `visible_on`, `expires_on`, `created_on`, `modified_on`, `payload`.
- `orleans_stream_dead_letter`: the same columns plus `dead_on` and `remove_on`.
- `orleans_stream_control`: per queue `evict_on`, for scheduling eviction.

**Store operations** (each atomic)

| Operation | Semantics |
|---|---|
| `Enqueue(key)` | Insert a message visible now, expiring at `now + expiry`. A unique-key violation on `message_key` means an earlier attempt of the same call committed, and is reported as success (section 4.7). Deduplication lasts only while the message row exists: if the first attempt was already consumed and confirmed, a retry inserts it again, with a new `message_id` but the same `message_key`. |
| `Dequeue(max, maxAttempts, visibility)` | Select up to `max` visible, unexpired messages with `dequeued < maxAttempts`, in `message_id` order, skipping locked rows. Increment `dequeued`, set `visible_on = now + visibility`, return them **ordered by `message_id`**. |
| `Confirm(receipts)` | Delete each message whose `dequeued` equals its receipt. Return the IDs actually deleted. |
| `Release(receipts)` | Make each message whose receipt matches visible now. |
| `Fail(receipt, maxAttempts, removal)` | If the message's `dequeued` still equals the receipt, make it visible again, or move it to dead letters if attempts are exhausted. A stale receipt changes nothing. |
| `Evict(batch, maxAttempts, removal)` | Move up to `batch` messages that are expired, or exhausted and no longer visible, to dead letters. |
| `EvictDeadLetters(batch)` | Delete up to `batch` dead letters past `remove_on`. |

**Implementation rules**

- Batch selection is atomic, but the shape is per engine, because no single statement is valid everywhere. MySQL in particular rejects an `UPDATE` whose subquery reads the target table (error 1093), and does not allow `LIMIT` in an `IN` subquery, so `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED)` does not work there.

  | Engine | `Dequeue` and `Evict` shape |
  |---|---|
  | PostgreSQL | One statement: `UPDATE … FROM (SELECT … ORDER BY message_id LIMIT @max FOR UPDATE SKIP LOCKED) … RETURNING …`. |
  | SQL Server | One statement: `UPDATE TOP (@max) … WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK) … OUTPUT inserted.…`, selecting through an ordered CTE, in a session explicitly at `READ COMMITTED`. `READPAST` is rejected under `READ_COMMITTED_SNAPSHOT` unless the query also has `READCOMMITTEDLOCK`, and it is only allowed at `READ COMMITTED` or `REPEATABLE READ`. `READCOMMITTEDLOCK` is in the same hint group as `ROWLOCK`, so `ROWLOCK` is dropped; `READPAST` already skips row locks. With RCSI off, `READCOMMITTEDLOCK` is what `READ COMMITTED` does anyway, so this one hint set is valid in both configurations. |
  | MySQL, MariaDB | One transaction, two statements: `SELECT message_id … ORDER BY message_id LIMIT @max FOR UPDATE SKIP LOCKED`, then `UPDATE … WHERE message_id IN (…)` with the IDs just read, then `COMMIT`. The rows stay locked between the two statements, so the pair is atomic. |
  | SQLite | `BEGIN IMMEDIATE`, then the select and update. SQLite has a single writer, so there is nothing to skip; the immediate transaction serialises dequeuers. |

  Each shape is verified by the same concurrent-dequeuer conformance test. **No temporary tables built with `CREATE … SELECT … FOR UPDATE`.**
- Result order is never assumed from `OUTPUT` or `RETURNING`. The component logic sorts by `message_id`.
- Eviction runs on a schedule owned by `orleans_stream_control`. Exactly one silo wins each slot with a conditional update of `evict_on`, and the parameters are passed by name.

**Core rules** (the lessons from the in-box receiver)

- **Monotonic cache positions.** The receiver assigns every dequeued message a position from a counter that only increases, and uses it as the `StreamSequenceToken`. Database message IDs are never cache tokens, because redelivered and late-committed messages carry lower IDs.
- **Receipts.** The receiver tracks the latest dequeue count per message as its receipt. Confirmation, release and failure all send receipts, so a stale attempt can never confirm, release or fail a newer one.
- **Confirmation coalescing.** When several attempts of one message leave the cache together, the receiver confirms that message once with the highest receipt.
- **Position to message mapping.** The failure handler resolves a position to its message ID. Positions that no longer resolve (released, redelivered, expired) are skipped, never mapped to an unrelated row.
- **Release on shutdown.** On shutdown the receiver releases every pending message it holds, so the next owner picks them up immediately instead of waiting for the visibility timeout.
- **Cache contract.** The component logic documents that its batches are ordered by position, which is what `SimpleQueueCache` requires.

**Delivery semantics**

At-least-once. Consumers must tolerate duplicates, from redelivery and from an enqueue retried after its first attempt was already confirmed. The `message_key` is exposed through a provider-specific API as the deduplication key, since it is the same in both cases, while `message_id` is not. Keeping deduplication records beyond the message's lifetime would make enqueue exactly-once only within some retry window, at the cost of another table and its eviction, without removing the need for consumer deduplication after redelivery; it is not planned.

**Tests**

- Store conformance for every operation, including concurrent dequeuers on one queue (no message dequeued twice before its visibility timeout), receipt rules for confirm, release and fail, enqueue retried after a commit-time connection failure (one message while the first is pending; the same `message_key` if the first was already confirmed), and eviction bounds.
- Receiver tests: out-of-order dequeue within a read, redelivery after newer messages are cached, release on shutdown, duplicate attempts confirmed once.
- End-to-end streaming tests (the Orleans streaming test suites) on every engine.
- A cluster test where a silo shuts down while holding unconfirmed messages, and every message is delivered.

**EF flavour note**

EF Core cannot express row locks or skip-locked reads, and the main EF providers (Npgsql, Pomelo, SqlClient) do not add them. So `Dequeue` and the eviction operations cannot be generated by EF, and the EF streaming store runs the same per-engine statements as the ADO.NET store, on the context's connection and transaction. The rules for this raw SQL are in section 7.1.

---

## 6. ADO.NET backend specifics

- One `IRelationalDialect` per engine: SQL text per store method, parameter naming, error classification, type mapping, skip-locked syntax.
- Connections come from `DbDataSource`. Commands are prepared where the driver benefits.
- Bulk paths (unregistering many silos, evicting) use bounded batches, never unbounded `IN` lists.
- MariaDB and MySQL are **separate dialects** sharing most text, because they diverge in exactly the places this design relies on: locking reads, `ROW_COUNT()` semantics, temporary table rules.
- Oracle is optional and community-maintained, since no CI image is as practical as the others. It ships only when a maintainer commits to running its conformance suite.

## 7. EF Core backend specifics

- The model maps exactly the shared schema, keys as configured in section 4.3, and the `orleans_schema_history` ledger as a shared-type entity, so that every component's package can map the same ledger table in one model.
- Contexts come from `IDbContextFactory<TContext>`, one short-lived context per store operation. Orleans calls providers concurrently, and a `DbContext` is not thread-safe, so a scoped or shared context would break under load. The application registers the factory with `AddDbContextFactory`; using the application's own context type does not mean sharing its instances.
- Read paths use `AsNoTracking()`. Inserts use `SaveChanges`; conditional updates and deletes use `ExecuteUpdate` and `ExecuteDelete`, so the change tracker never turns an atomic operation into several.
- **Duplicate keys are recognised from the failed insert's own exception**, by each driver's signature (`DbErrors`, plus SQLite's extended result code, which only its exception type exposes), with no provider list. The database's later contents cannot identify the cause of an earlier failure, because the conflicting row may be deleted in between. Any other driver is classified by its standard SQLSTATE (`23505`), or by the application's own rule (`IsDuplicateKeyError`). Only when neither decides does the store check for a row under the key: if one exists the insert could not have succeeded, and if none does the driver's error surfaces, as it would without the check. A conflicting row deleted in between therefore surfaces as the driver's error rather than as a conflict; the check never gives a wrong answer.
- **Writes run exactly once per attempt**, in a single-attempt execution strategy that suspends any retrying strategy configured on the context. A retry after a connection failure would follow an attempt of unknown outcome, and once another writer had moved the row on, nothing could tell whether the first attempt committed. Writes are repeated only when the server reported that the attempt was not applied, as in the ADO.NET flavour; reads keep the context's strategy.
- No EF migrations are shipped. EF users add the Orleans tables to a `DbContext` of theirs, the application's own or a dedicated one, with `ApplyOrleansModel()`, and create them through their own migrations (section 8.1). The provider never changes the schema.
- A CI test generates the schema from the EF model on each engine and compares it with the ADO.NET schema scripts. They must be identical, so both paths produce the same tables.
- `ApplyOrleansModel()` lets the application include Orleans tables in its own `DbContext` and migrations.

### 7.1 Raw SQL in the EF flavour

EF translation is used wherever it can express an operation, because EF and its providers keep the generated SQL correct across database releases. Only operations that EF cannot express drop down to raw SQL: mainly streaming `Dequeue` and eviction, which need a skip-locked locking read inside an update, and probably the SQL Server snapshot read of clustering, which needs a table hint. Working around a provider's error reporting is not a reason for raw SQL: persistence once used a raw insert for that, and replaced it with an existence check (section 7). Only a component with raw statements needs to know the engine. For those statements:

- **One copy of the SQL.** The statements live in the component's linked source (`src/Streaming/Shared`, section 3.1), which is compiled into both `Scarlet.Orleans.Streaming.AdoNet` and `Scarlet.Orleans.Streaming.EntityFrameworkCore`. Neither package references the other, and both execute the same text. When a database release breaks a statement, CI catches it through either flavour, and one fix repairs both. Separate copies per flavour would drift apart, as the in-box scripts did.
- **Only documented, standard constructs**, in the per-engine shapes listed in section 5.5 (for example `FOR UPDATE SKIP LOCKED` in a select whose IDs a second statement updates, inside one transaction, on MySQL). No behaviour that merely happens to work on one engine version. The MySQL 8.4 breakages (`ROW_COUNT()` inside a `WHERE` clause, `CREATE TEMPORARY TABLE … SELECT … FOR UPDATE`) were exactly that kind of trick.
- **As few statements as possible.** Everything EF can generate, it generates. The raw set is listed in one place and reviewed as a unit.
- **New database releases join the CI matrix early** (section 9.3), so a release that breaks a raw statement is found before users upgrade, not after.

EF translation is not immune to upgrades either: provider bugs and translation changes between EF versions happen, and EF upgrades follow .NET releases. The CI matrix covers both paths.

---

## 8. Schema and migrations

- Each component's schema has a version. Each version change ships:
  - an updated install script per engine,
  - a migration script per engine from the previous version,
  - the matching change to the component's EF model (no EF migrations are shipped, see section 7).
- **Migration tests** install a frozen, hash-checked copy of the previous schema, run the migration **twice** the way users run it (through each engine's own client: `sqlcmd`, `psql`, `mysql`, `sqlite3`), then run the component's conformance tests.
- **Fresh equals migrated.** A test compares a fresh install with an old install plus all migrations, at the schema level (columns, types, indexes) and the behaviour level.
- **Type checks.** A test asserts that every column type can hold every value the C# side writes and reads (the SQLite version problem).
- Migrations are idempotent and safe to rerun. Data backfills are batched.
- **Rules for every new schema version**, because schema changes outlive code changes:
  - a shipped script never changes; a fix is a new version;
  - each version states its requirements in one place: the SQL migration per engine, the EF model change, the data steps of its EF migration helper (section 8.1), and the minimum version the code then needs;
  - each version gets an upgrade test from a populated database of the previous version through both paths, SQL migration and EF migration, compared row by row;
  - the runner stays a ledger, a lock and ordered scripts. Anything a version needs beyond that is written into that version's scripts, not added to the runner.

### 8.1 Opt-in schema management for the ADO.NET flavour

Manual scripts are the most common complaint about the in-box providers. The ADO.NET flavour therefore offers automatic schema creation and migration as an opt-in, while still shipping the bare `.sql` files.

**One source of truth.** Migration scripts are embedded in each backend assembly as resources and also published as `.sql` files. The runner executes exactly the scripts a DBA would run by hand. Since the design has no stored procedures, scripts are plain statements, with no `DELIMITER` handling needed.

**Ledger.** An `orleans_schema_history` table records, per component: schema version, script name, script checksum, applied time and applying principal. On startup the runner compares embedded scripts with the ledger:

- pending versions are applied in order;
- a checksum mismatch on an applied script fails (a shipped script was edited);
- a database outside the schema range this code supports fails, naming both versions.

**Concurrency.** Many silos start at once, but only one migrates. The runner holds a database-level lock while it reads the ledger and applies scripts: `pg_advisory_lock` (PostgreSQL), `sp_getapplock` (SQL Server), `GET_LOCK` (MySQL, MariaDB), `BEGIN EXCLUSIVE` (SQLite). The others wait, then find the ledger current.

**Atomicity per engine.**

- PostgreSQL and SQL Server have transactional DDL, so a migration and its ledger row commit or roll back together.
- MySQL and MariaDB commit implicitly on DDL, so a migration is a sequence of idempotent steps (`IF NOT EXISTS`, guarded `ALTER`) with a ledger row per step. A crash midway resumes from the last recorded step.

**Modes.**

| Mode | Behaviour |
|---|---|
| `None` | No checks. |
| `Validate` (default) | Check the ledger and fail fast, listing the scripts that need to run. |
| `Apply` | Create the schema or apply pending migrations at startup. |

**Placement.** Each component runs its own schema check once, before its first database operation, early in the silo lifecycle, so the clustering tables exist before membership initialises. Because the runner is linked into each assembly (section 3.1), components do not share an in-memory gate; the database lock and the ledger keep concurrent checks from different components and silos consistent. Cluster clients only validate and never run DDL.

**Rolling upgrades.** Migrations follow expand/contract. A release only adds what the previous release tolerates, and a later release removes what became obsolete. Compatibility is declared as ranges on both sides (section 4.1):

- each package version declares the schema versions it can run against, for example `[3, 4]`: it works on schema 3 and on schema 4 once the expand migration is applied;
- each schema version records, in the ledger, the oldest package version that can still use it, so an older silo refuses to start once a contract migration has removed something it needs.

A rolling upgrade is then: deploy code that supports `[n, n+1]`, apply migration `n+1`, and only after every silo runs that code apply a later contract migration. Contract migrations are never applied automatically in `Apply` mode; they run only through the tool, as a deliberate step.

**Permissions and tooling.**

- An optional separate `DbDataSource` for migrations, so the runtime principal needs no DDL rights.
- A `dotnet orleans-db` tool: `migrate` (apply), `status` (show the ledger) and `script` (print the exact SQL that would run, for teams whose DBAs review or apply changes themselves).

**EF flavour.** The application's EF migrations create and evolve the Orleans tables, whether they share the application's `DbContext` or use a dedicated one. The provider has no `Apply` mode and never runs scripts. The ledger still has to know what was applied, or `Validate` would always fail:

- **A model diff only covers structure.** EF generates the DDL from the difference between two models. It cannot know about data backfills, key transformations or any other data step in the matching SQL migration, so a migration generated by EF alone can produce the right tables with the wrong data.
- **Each schema version therefore ships an EF migration step**, a `MigrationBuilder` extension that the application calls from the `Up()` method of the migration EF generated, as a [custom migration operation](https://learn.microsoft.com/ef/core/managing-schemas/migrations/managing#customize-migration-code), for example `migrationBuilder.ApplyOrleansClusteringSchema(4)`. It emits the data steps of that version, as provider-neutral migration operations where possible and per `migrationBuilder.ActiveProvider` only where a step needs engine-specific SQL, and then the ledger row for that version through `InsertData`, with `source = 'ef'` instead of a script checksum. The ledger table itself is part of the model, so EF creates it. The release notes of each version state where in `Up()` the call must go when a data step depends on DDL ordering (for example after a column is added, before an old one is dropped).
- **The ledger row comes from that call, not from `HasData`.** A seeded row would be generated whether or not the data step was added, and would record a version whose data was never migrated. With the row written by the step, forgetting the call leaves the ledger behind, and `Validate` fails at startup naming the missing step.
- `Validate` reads the ledger through EF and checks the version and range, whichever flavour recorded it, so either flavour runs on a database the other created. The schema-equality test (section 7) guarantees that the EF-generated tables, ledger included, match the scripts for the same version.
- **Upgrade tests run both paths from populated data.** For every schema version, a database at the previous version is filled with representative data (including every identity kind and edge case from section 4.3), then upgraded once with the SQL migration and once with an EF migration generated from the model diff plus the shipped step. Both results must match at the schema level and row by row, and the component conformance tests then run on each.
- The ADO.NET runner refuses to apply scripts to a component whose ledger has EF rows, so one component's tables are never managed by both.

Both flavours produce the same schema, and a test keeps the EF model and the SQL scripts in agreement.

#### Implementation: a built-in runner, linked into every package

The runner is part of the shared infrastructure (`src/Shared`, section 3.1) and is compiled into every ADO.NET and EF Core package. Each component applies only its own scripts, embedded in its own assembly, under the shared lock and ledger. Clustering never migrates the persistence tables.

It is a small amount of code: read the embedded scripts, take the database lock, read the ledger, apply pending scripts (in a transaction, or step by step on MySQL and MariaDB), record them with checksums. Everything else in this section is what it implements.

**Why not a third-party migration library by default.** [DbUp](https://dbup.github.io/) is the best fit on features: plain `.sql` scripts, a journal table, a package per engine, actively maintained. Building the runner on DbUp *inside* the component packages would make every package depend on DbUp, and add an unverified dependency to the AOT guarantee (section 4.11), so the default runner is built in.

The built-in runner is also most of the work regardless. The `Validate` mode (the default) needs the ledger, the checksums and the "schema newer than this code" check, and both modes need the lock across concurrently starting silos, which DbUp does not provide. DbUp would only add the execution of pending scripts in order, each in a transaction, which is a small part of the runner.

**DbUp as an optional companion, if wanted.** Because every package is isolated (section 3.1), DbUp can still be offered without touching the main packages, through one optional companion package per component, for example `Scarlet.Orleans.Clustering.AdoNet.DbUp`:

- The companion links the component's scripts and the shared migration glue, so the main package exposes no extra public API for it.
- It plugs into DbUp through a custom journal (`IJournal`) that writes to `orleans_schema_history`, so there is still one ledger, with the same checksums, lock and modes.
- One companion per component serves both flavours, since DbUp only needs a `DbDataSource` and the scripts.
- The main packages, and the AOT guarantee, are unaffected.

Companions are not planned for the first release. They are worth adding if users ask, for example teams that already run every database change through DbUp and want Orleans migrations in the same pipeline. Adding them later needs no change to the main packages.

Other libraries fit worse: grate (CLI-oriented, folder conventions), FluentMigrator (C# migrations, which conflicts with `.sql` files as the source of truth) and Evolve (no stable release since development slowed).

The tool (`orleans-db`) calls each installed component package's small public schema API (for example `ScarletAdoNetClusteringSchema.MigrateAsync(dataSource, mode, cancellationToken)`), so command-line and startup migrations run the same code.

---

## 9. Testing and CI

### 9.1 Test layers

1. **Store conformance kit.** Black-box tests of each store contract: atomicity, conflict outcomes, ordering, limits. Every backend and engine runs it. This is the most important layer, because it verifies exactly the code that differs between engines.
2. **Orleans TestKits.** The existing clustering, persistence and reminders TestKits, including the model-based suites.
3. **Component tests.** Streaming receiver behaviour, key isolation, ETag rules.
4. **Chaos tests.** Concurrent registration, membership churn, concurrent dequeuers. These run in CI on every engine, not only as excluded stress tests.
5. **Migration tests** (section 8).
6. **End-to-end cluster tests.** Multi-silo scenarios: rolling restart, silo crash with unconfirmed stream messages, gateway failover.
7. **NativeAOT test runs** for the ADO.NET flavour: the conformance suites and a hosting test, published and run as NativeAOT per engine (section 4.11), plus the build-time analyzers as errors.

### 9.2 Mutation checks

For every bug class in section 2 there is a test that fails when that bug is reintroduced. When a test is added for a fix, it is first run against the unfixed code to prove it fails.

### 9.3 Database matrix

| Engine | Versions in CI |
|---|---|
| SQL Server | 2019, 2022, each with `READ_COMMITTED_SNAPSHOT` off and on (the on/off choice changes the locking reads, sections 5.1 and 5.5) |
| PostgreSQL | 14, 16, 17 |
| MySQL | 8.0 (while supported), 8.4 |
| MariaDB | 10.6, 11.4 |
| SQLite | bundled |
| Oracle | optional, community-run |

Images are pinned by digest to releases at least a few weeks old, and updated deliberately.

### 9.4 Rules

- One test process per database server at a time (tests that recreate databases must not run in parallel across target frameworks).
- Real clients for anything users run by hand (migrations).
- Fake time through `TimeProvider` for every timeout and expiry.

---

## 10. Performance notes

- Narrow primary keys (16-byte hashes) and few secondary indexes: one for reminder ranges, one for stream queue selection `(service_id, provider_id, queue_id, visible_on)`, one for dead letter removal.
- Single-statement writes on the hot paths (persistence write, `IAmAlive`, directory lookup).
- Streaming dequeue and confirm are batched. Confirmation sends all receipts in one statement through a table-valued parameter, an array parameter or a `VALUES` list, depending on the engine.
- Measure with the Orleans benchmarks and per-engine load tests before claiming numbers. Compare against the in-box providers.

---

## 11. Coming from the in-box ADO.NET providers

The schemas differ (GUID ETags, hash keys, snake case, no `OrleansQuery`), so there is no in-place upgrade. Instead:

- **Import tool.** `orleans-db import` reads the in-box tables and writes the new ones:
  - persistence: rebuilds each full identity, derives the new key, keeps the payload, assigns a new ETag;
  - reminders: rebuilds the grain ID, derives the new key, copies the schedule;
  - clustering and grain directory: not imported; the cluster rebuilds them on start.
- **The legacy rows do not carry the identity directly**, so the import needs input from the user:
  - `GrainIdN0`, `GrainIdN1` and `GrainIdExtensionString` encode the grain key in a form that depends on the key kind; the importer reassembles it the same way the in-box provider split it.
  - `GrainTypeString` holds whatever the in-box key format put there. Under the default format it is often the state name or a type string that no longer maps to a grain type, so the user supplies a mapping (a table from legacy values to grain type and state name), and the importer refuses rows it cannot map.
  - The payload's serializer and content type must be known and configured on the new provider; the importer does not re-serialize.
  - A dry run lists unmapped and ambiguous rows before anything is written.
- **Cutover.** Stop the cluster, import, start on the new providers. Streaming queues are drained before cutover, since in-flight messages are not imported. There is no online transition: the new provider does not read the in-box tables, because supporting the in-box key formats would bring back the problems listed in section 2.

---

## 12. Open questions

1. **Hash function.** `XxHash128` is fast and stable but not cryptographic. That is fine for keys, since full values are always verified. Confirm there is no need for a keyed hash against deliberate collision attacks (grain IDs are usually not attacker-controlled, but sometimes are).
2. **Membership version type.** Keep `int` as Orleans defines it, or store `bigint` and narrow at the boundary for headroom.
3. **JSON columns.** Worth supporting at all, given the schema-option complexity, or leave JSON readability to `content_type` plus a view?
4. **Oracle.** Ship at all without a maintainer and a CI image?
5. **Stream message IDs.** Per-queue sequences or one global sequence. Global is simpler; per queue avoids a hot sequence under very high enqueue rates.
6. **Default schema mode.** `Validate` is the safe default (section 8.1). Should development environments default to `Apply`, for example when `IHostEnvironment.IsDevelopment()`?
7. **Upstreaming.** If this proves itself, which parts are worth proposing to dotnet/orleans: GUID ETags, the store conformance kit, the streaming receiver rules?

---

## 13. Roadmap

1. **Foundation:** the linked shared infrastructure (`src/Shared`), the store contracts and their conformance suite in the test projects, CI matrix with all engines, `IsAotCompatible` with analyzer warnings as errors, and the NativeAOT test project. Every later milestone runs its component's conformance tests under NativeAOT as well as JIT.
2. **Persistence and clustering** on ADO.NET (PostgreSQL, SQL Server, MySQL, MariaDB, SQLite). These are the most used components and the easiest to verify.
3. **Reminders and grain directory** on ADO.NET, including the directory chaos test on every engine.
4. **Streaming** on ADO.NET, with the receiver rules from section 5.5 and the end-to-end cluster tests.
5. **EF Core flavour** for all components over the same schema, with the schema-equality test.
6. **The import tool**, and benchmarks against the in-box providers, including NativeAOT startup time and memory.
7. First preview release.
