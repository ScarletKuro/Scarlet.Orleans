# Provider investigation, 2026-11-15

Follow-up to the [full benchmark campaign](2026-11-14-local.md), based on `cf32c03` plus the changes accompanying
this report. The SQL Server streaming tail has a confirmed provider cause and a targeted fix. SQLite EF transactional
loads have a confirmed contention problem; the two caught exceptions from the original campaign remain unidentified.

## SQL Server streaming

Nine diagnostic runs selected **only SQL Server and Scarlet**, with four queues, 16 producers, the usual unpaced
workload followed by 800 scheduled events/s for ten seconds. No in-box receivers were registered. Four runs had
paced delivery p99 above three seconds, so competition from another benchmark provider was not necessary.

The slowest run had paced delivery p99 **9,153.6 ms**, while send-to-enqueue-acknowledgement p99 was about **40 ms**.
Scarlet's confirmation operation reached **9,273.83 ms** and recorded two SqlException retries. Enqueue and dequeue
durations remained in the tens of milliseconds. The queue's pulling agent waits for confirmation, so delayed
confirmation stalls subsequent delivery even when enqueue succeeds quickly.

SQL Server Extended Events supplied the missing explanation:

- Confirmation DELETE RPCs took four to nine seconds with approximately zero to six milliseconds of CPU time.
- Wait events included `LCK_M_U`/`LCK_M_X` waits of approximately 4.2 seconds, followed by waits near five seconds.
- A retained `system_health` deadlock report at **2026-11-14 22:37:54 UTC** showed two confirmation DELETEs, with
  32 and 64 receipt parameters, deadlocking on `ix_orleans_stream_message_key`.
- One process owned an X key lock needed by the other's U request; the other owned a U lock needed by the first's X
  request. Both resource entries named the global unique message-key index. Earlier captured runs had the same pattern.

The DELETE predicate already includes `message_id IN (...)`, queue identity and matching receipt conditions. That ID
prefilter allows a message-ID seek but does **not** constrain SQL Server's chosen read index. The deadlock's U locks on
the global message-key index identify the unsafe access path; that index has no predicate on its leading message key.

### Change and regression coverage

SQL Server confirmation and release now supply a FROM-clause `INDEX(pk_orleans_stream_message)` hint. This restricts
the target read to the shipped schema's clustered message-ID index. Queue and receipt checks, bounded batches,
idempotency and normal secondary-index maintenance are preserved. Other engines retain their original SQL. The hint
depends on the primary-key index name in the shipped SQL Server schema; renamed custom indexes need to retain that name.

Six integration cases inspect actual execution plans for confirm/release batches of 32, 64 and 500 receipts. Three
cases failed the message-ID-index requirement before the fix; all six pass afterward. These cases protect the access
path rather than requiring a timing-sensitive deadlock to occur during a test.

Five additional isolated diagnostic runs after the fix:

| Run | Unpaced delivered/s | Unpaced delivery p99, ms | Paced sent/s | Paced delivery p99, ms | Duplicates |
|---|---:|---:|---:|---:|---:|
| 1 | 1,247 | 348.3 | 801 | 188.6 | 0 |
| 2 | 1,280 | 354.7 | 800 | 157.1 | 0 |
| 3 | 1,280 | 378.9 | 800 | 182.1 | 0 |
| 4 | 1,272 | 334.3 | 800 | 177.2 | 0 |
| 5 | 1,178 | 354.0 | 800 | 180.8 | 0 |

All expected distinct events were delivered. There were **zero observed retries**. Whole-run confirmation maxima
were 303, 311, 459, 278 and 337 ms respectively; paced-phase confirmation maxima were 98, 68, 97, 91 and 94 ms.
The repeated multi-second tail disappeared in these runs. Five short runs cannot establish that SQL Server will never
deadlock under other workloads, and this does not revise the earlier in-box provider failures or establish a new
cross-provider performance ranking.

## SQLite EF transactional state

Six runs with the normal 16 callers and two stress runs with 32 callers completed with **zero caught transfer
exceptions**, and every transfer-total verification passed. The original two exceptions therefore cannot be assigned
a cause from the available evidence. Previously the benchmark counted every caught transfer exception as `Aborted`
without retaining its type or stack; it now reports grouped counts and the first full exception for each group.

The contention is real even without an exception. At 16 callers the shared-account workload managed only about
3–4 transfers/s, with p99 around 5.0–6.4 seconds. At 32 callers, p99 rose to approximately 10.8–11.8 seconds.

`EntityFrameworkCoreTransactionalStateStore.LoadAsync` reads the entry and pending versions inside an EF
`RepeatableRead` transaction. With Microsoft.Data.Sqlite, the generic begin-transaction call acquires the writer lock
even though this operation only reads. A separate WAL-file probe held an uncommitted writer and compared:

| Operation while the writer was held | Observation |
|---|---|
| EF `BeginTransactionAsync(RepeatableRead)` followed by SELECT | Still blocked when checked after 300 ms |
| Plain SELECT on a separate connection | Returned the last committed value in approximately 2.4 ms |
| Driver-level deferred `Serializable` transaction and SELECT | Returned the last committed value while the writer remained held |
| Original EF read after writer rollback | Returned the last committed value; approximately 621 ms total |

This establishes an unnecessary source of read/write serialization. It does **not** establish that it caused the two
original exceptions. It also does not justify weakening isolation: the provider needs one consistent view of both the
entry and pending versions. A deferred read snapshot is a candidate for a subsequent SQLite-specific implementation;
the generic EF transaction API used here does not expose that option. No SQLite production behavior was changed in
this investigation. For latency-sensitive concurrent SQLite transactions, the dedicated ADO.NET flavor remains the
more defensible choice from these measurements; neither flavor removes SQLite's single-writer limit.

## Reproduction and validation

Same host and database versions as the full campaign: Windows, .NET 10.0.15, EF Core 10.0.14, SqlClient 7.1.2,
SQL Server 2022 16.0.4305.5 in local Docker, and SQLite WAL. Diagnostic instrumentation was enabled for both the
before and after runs. Builds and tests did not run concurrently with the measured workloads.

Set the usual `SCARLET_TEST_SQLSERVER` administrative connection string, then run these in Release with
`SCARLET_BENCH_DIAGNOSTICS=1`:

```sh
dotnet run -c Release --project bench/Scarlet.Orleans.Streaming.Benchmarks -- --engine SqlServer --provider scarlet streaming-focused.md
dotnet run -c Release --project bench/Scarlet.Orleans.Transactions.Benchmarks -- --engine Sqlite --provider "Scarlet EF Core" transactions-focused.md
```

For repeated measurements, build once and invoke the resulting executable to exclude build activity. Set
`SCARLET_BENCH_CALLERS=32` for the SQLite stress variant. The default remains 16. See [benchmark instructions](../README.md)
for diagnostic fields and provider names.

Validation: **102 queue-store contract cases passed**, with no skips, across SQLite, PostgreSQL, SQL Server, MySQL
(MySqlConnector and MySql.Data) and MariaDB; **six SQL Server execution-plan cases passed**. The Release solution
build passed with zero warnings and errors. The temporary Extended Events session was stopped and removed.

Local raw evidence is retained under `artifacts/investigation/2026-11-15/` (ignored by Git): `streaming-*`,
`confirmation-*` and `wait-tail-1` are pre-fix runs; `fixed-1` through `fixed-5` are post-fix runs;
`transactions-*` and `sqlite-*` are SQLite runs. `stream-deadlock.xml`, `with-waits.xml` and the standalone
`read-probe` hold the database evidence. The published table above preserves every post-fix result.

Relevant primary documentation: [SQL Server deadlocks and system_health capture](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-deadlocks-guide?view=sql-server-ver15),
[SQL Server table hints](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver17),
[Microsoft.Data.Sqlite deferred transactions](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/transactions),
and [SQLite transaction locking](https://www.sqlite.org/lang_transaction.html).
