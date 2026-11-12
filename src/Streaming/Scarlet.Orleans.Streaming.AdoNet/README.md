# Scarlet.Orleans.Streaming.AdoNet

Orleans persistent streams over relational databases with plain ADO.NET: SQL Server, PostgreSQL, MySQL, MariaDB and
SQLite. NativeAOT compatible. Requires .NET 10 and Orleans 10.4 or later.

> **In development.** The queue store and the provider are tested against every supported engine, but not yet with
> Orleans' end-to-end streaming tests in a cluster. Do not use it in production yet.

## Setup

Create a `DbDataSource` with your driver, and add the provider under the name your streams use, on the silos and on
any client that produces events:

```csharp
var dataSource = NpgsqlDataSource.Create(connectionString);

builder.UseOrleans(silo => silo
    .AddMemoryGrainStorage("PubSubStore")
    .AddScarletAdoNetStreams("StreamProvider", options =>
    {
        options.DataSource = dataSource;
        options.SchemaMode = SchemaMode.Apply;
    }));
```

Instead of `options.DataSource`, the provider resolves a `DbDataSource` from dependency injection, keyed by
`options.DataSourceServiceKey` if set. The engine is inferred from the driver; set `options.Engine = "MariaDb"` for
MariaDB. The last argument configures Orleans' side of the provider, such as its cache size, its pulling agents and its
queue count (`HashRingStreamQueueMapperOptions`, 8 by default), which must match on silos and clients.

## Options

| Option | Default | Meaning |
|---|---|---|
| `DataSource`, `DataSourceServiceKey` | from DI | The `DbDataSource` to use. |
| `Engine` | inferred | `SqlServer`, `PostgreSql`, `MySql`, `MariaDb` or `Sqlite`. |
| `SchemaMode` | `Validate` | What to do with the schema at startup: `Validate`, `Apply` or `None`. |
| `MaxAttempts` | 5 | Dequeues before a message moves to the dead letters. |
| `VisibilityTimeout` | 1 minute | How long a dequeued message stays hidden; an unconfirmed message is delivered again after it. |
| `ExpiryTimeout` | 10 minutes | How long a message can wait before it expires to the dead letters. |
| `DeadLetterRetention` | 7 days | How long dead letters are kept. |
| `EvictionInterval` | 1 minute | How often each queue moves expired and exhausted messages, and deletes old dead letters. |
| `EvictionBatchSize` | 1,000 | The most rows one eviction moves or deletes. |

## Behaviour

- **At least once.** A message is delivered again if it is not confirmed within its visibility timeout, so consumers
  must tolerate duplicates.
- **Receipts.** Every confirmation, release and failure names the attempt it is for, so a late one for an attempt
  that has since been delivered again changes nothing.
- **Hand-over.** A silo that stops, or loses a queue to another silo, releases the messages it holds, and the next owner
  reads them at once.
- **Dead letters.** A message out of attempts or expired is moved to
  `orleans_stream_dead_letter`, and deleted after `DeadLetterRetention`. A payload that cannot be deserialized counts as
  a failed attempt, so a message read while its silo stops is not lost.
- Queues are not rewindable.
