# Local transactional state results, 2026-11-14

Two consecutive runs on Windows 11, a Ryzen 7 5800X3D (16 logical processors), .NET 10.0.15, and local Docker databases through `127.0.0.1`. One benchmark process ran at a time. See the [all-suite report](2026-11-14-local.md) for versions, harness corrections, scope, and comparison limits.

Measurements: 2026-11-14 23:20 to 2026-11-14 23:34, Europe/Tallinn. Filenames use the campaign start date.

16 logical processors, .NET 10.0.15, 1024 B account state. Single account: 300 sequential transactions on one account, each prepared and committed in one store. Hot transfers: 16 callers each moving money from its own account into one shared account for 10 s; each transfer is one transaction over both, so on the shared account prepared versions wait for their transaction managers and pile up. Spread transfers: each caller between its own two accounts. Callers start transfers for the stated time; throughput counts those that commit over the time until the last one finished. Latencies in milliseconds. Bytes are the serialized bytes the storage writes; state copies per store are those bytes over the state size: the dedicated stores write each prepared version once, the wrapper rewrites the committed state and every pending version. The in-box provider does not report its bytes.

## Reading the results

- **Payload efficiency is the clearest repeatable advantage.** Dedicated stores write about 3.36–3.43 KB per hot transfer versus 54.59–64.83 KB through the wrapper on Scarlet persistence, roughly 16–19 times less serialized payload. This is not a measurement of network traffic, WAL, or physical disk writes; the in-box persistence provider exposes no comparable payload metric.
- **ADO.NET versus EF Core:** dedicated ADO.NET spread throughput is 443–478 versus 82–85 transfers/s on PostgreSQL, 229–239 versus 74–76 on SQL Server, and 126–148 versus 58–63 on MySQL. The paired gap is about 2.2–5.6 times, depending on the engine.
- **Wrappers remain competitive.** On PostgreSQL the dedicated ADO.NET store wins hot-transfer throughput in both runs, but spread throughput changes order against the wrappers between runs. On SQL Server it has lower single-account latency than the Scarlet persistence wrapper, while its spread throughput is 4–7% lower.
- **MySQL does not support a universal dedicated-store speed claim.** The Scarlet persistence wrapper reaches 237 versus 148 dedicated ADO.NET spread transfers/s in run 1, but 128 versus 126 in run 2. Its single-account p50 is lower in both runs: 7.19–12.54 ms versus 12.69–15.83 ms. The payload reduction survives this throughput trade-off.
- **SQLite remains sensitive to contention.** Hot transfers reach only 3–4/s, with p99 of roughly 5–7 seconds. Dedicated ADO.NET spread throughput varies from 72 to 147/s; EF Core from 77 to 130/s; the wrapper from 116 to 138/s. EF Core reports two caught transfer exceptions in run 2. These results do not establish a stable SQLite throughput ranking.
- **Completion and checks:** the in-box SQL Server wrapper's read times out after 30 seconds in run 1; run 2 completes. The incomplete row has no usable performance numbers. Every completed provider passes the single-account value check and both hot and spread total-conservation checks. The Aborted column counts all exceptions caught during transfers, rather than distinguishing expected aborts from other failures.

## Run 1

| Engine | Provider | Single p50 | Single p99 | Single bytes/tx | Hot transfers/s | Hot p99 | Hot bytes/transfer | Hot state copies/store | Spread transfers/s | Spread p99 | Aborted |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.53 | 3.20 | 1580 | 4 | 5131.32 | 3396 | 0.8 | 147 | 3116.83 | 0 |
| Sqlite | Scarlet EF Core | 2.06 | 6.13 | 1580 | 3 | 6399.59 | 3422 | 0.8 | 130 | 2661.37 | 0 |
| Sqlite | Wrapper on Scarlet persistence | 1.03 | 1.28 | 2432 | 4 | 5300.13 | 61429 | 15.1 | 116 | 4202.30 | 0 |
| PostgreSql | Scarlet ADO.NET | 3.17 | 6.05 | 1580 | 162 | 127.52 | 3394 | 0.9 | 478 | 49.96 | 0 |
| PostgreSql | Scarlet EF Core | 13.05 | 52.47 | 1580 | 42 | 633.25 | 3409 | 0.9 | 85 | 241.19 | 0 |
| PostgreSql | Wrapper on Scarlet persistence | 2.57 | 3.22 | 2432 | 143 | 136.15 | 63185 | 16.6 | 495 | 47.63 | 0 |
| PostgreSql | Wrapper on Orleans ADO.NET persistence | 2.62 | 6.02 | n/a | 147 | 136.20 | n/a | n/a | 492 | 47.56 | 0 |
| SqlServer | Scarlet ADO.NET | 4.51 | 10.55 | 1580 | 76 | 329.94 | 3403 | 0.9 | 229 | 143.79 | 0 |
| SqlServer | Scarlet EF Core | 19.07 | 33.31 | 1580 | 23 | 796.06 | 3409 | 0.9 | 74 | 330.84 | 0 |
| SqlServer | Wrapper on Scarlet persistence | 8.55 | 17.49 | 2432 | 55 | 422.36 | 56467 | 15.6 | 247 | 97.04 | 0 |
| SqlServer | Wrapper on Orleans ADO.NET persistence | failed, see below |  |  |  |  |  |  |  |  |  |
| MySql | Scarlet ADO.NET | 12.69 | 18.14 | 1580 | 50 | 541.05 | 3377 | 1.0 | 148 | 146.48 | 0 |
| MySql | Scarlet EF Core | 18.65 | 29.38 | 1580 | 27 | 618.88 | 3422 | 0.9 | 63 | 305.11 | 0 |
| MySql | Wrapper on Scarlet persistence | 7.19 | 17.22 | 2432 | 49 | 453.29 | 56389 | 15.4 | 237 | 144.15 | 0 |
| MySql | Wrapper on Orleans ADO.NET persistence | 7.32 | 13.77 | n/a | 38 | 612.80 | n/a | n/a | 139 | 309.81 | 0 |

### Incomplete runs

- SqlServer / Wrapper on Orleans ADO.NET persistence: TimeoutException: Response did not arrive on time in 00:00:30 for message: ReadOnly Request [ sys.client/868f6be034c5474bab0684c8bbb64f8b]->[S127.0.0.1:13273:0 inboxwrapperaccount/2] Scarlet.Orleans.Transactions.Benchmarks.IAccountGrain.GetAsync() #0B95186F4102A840. Last known status is IsExecuting: True, IsWaiting: False, Diagnostics: [[Activation: S127.0.0.1:25316:153696224/inboxwrapperaccount/2@0f3429e56de64c8eaf853c6d6f32c47f #GrainType=Scarlet.Orleans.Transactions.Benchmarks.InBoxWrapperAccount,Scarlet.Orleans.Transactions.Benchmarks Placement=ResourceOptimizedPlacement State=Valid NonReentrancyQueueSize=0 NumRunning=2 IdlenessTimeSpan=00:00:27.4850000 CollectionAgeLimit=00:15:00], TaskScheduler status: WorkItemGroup:Name=[Activation: S127.0.0.1:25316:153696224/inboxwrapperaccount/2@0f3429e56de64c8eaf853c6d6f32c47f#GrainType=Scarlet.Orleans.Transactions.Benchmarks.InBoxWrapperAccount,Scarlet.Orleans.Transactions.Benchmarks Placement=ResourceOptimizedPlacement State=Valid],WorkGroupStatus=Waiting. Currently QueuedWorkItems=0; Total Enqueued=1536; Total processed=1536; TaskRunner=ActivationTaskScheduler:Queued=0; Detailed context=<[Activation: S127.0.0.1:25316:153696224/inboxwrapperaccount/2@0f3429e56de64c8eaf853c6d6f32c47f #GrainType=Scarlet.Orleans.Transactions.Benchmarks.InBoxWrapperAccount,Scarlet.Orleans.Transactions.Benchmarks Placement=ResourceOptimizedPlacement State=Valid NonReentrancyQueueSize=0 NumRunning=2 IdlenessTimeSpan=00:00:27.4850000 CollectionAgeLimit=00:15:00 CurrentlyExecuting=ReadOnly Request [S127.0.0.1:25316:153696224 sys.client/868f6be034c5474bab0684c8bbb64f8b]->[S127.0.0.1:25316:153696224 inboxwrapperaccount/2] Scarlet.Orleans.Transactions.Benchmarks.IAccountGrain[(Scarlet.Orleans.Transactions.Benchmarks.IAccountGrain)Scarlet.Orleans.Transactions.Benchmarks.InBoxWrapperAccount].GetAsync() #0B95186F4102A840]>, Message ReadOnly Request [S127.0.0.1:25316:153696224 sys.client/868f6be034c5474bab0684c8bbb64f8b]->[S127.0.0.1:25316:153696224 inboxwrapperaccount/2] Scarlet.Orleans.Transactions.Benchmarks.IAccountGrain[(Scarlet.Orleans.Transactions.Benchmarks.IAccountGrain)Scarlet.Orleans.Transactions.Benchmarks.InBoxWrapperAccount].GetAsync() #0B95186F4102A840 was enqueued 00:00:27.4850000 ago and has now been executing for 00:00:27.4850000.].

## Run 2

| Engine | Provider | Single p50 | Single p99 | Single bytes/tx | Hot transfers/s | Hot p99 | Hot bytes/transfer | Hot state copies/store | Spread transfers/s | Spread p99 | Aborted |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 3.20 | 8.97 | 1580 | 4 | 5349.13 | 3399 | 0.8 | 72 | 5612.36 | 0 |
| Sqlite | Scarlet EF Core | 3.85 | 8.48 | 1580 | 3 | 7078.09 | 3422 | 0.9 | 77 | 5002.08 | 2 |
| Sqlite | Wrapper on Scarlet persistence | 1.04 | 2.73 | 2432 | 4 | 5170.20 | 64829 | 15.9 | 138 | 3737.40 | 0 |
| PostgreSql | Scarlet ADO.NET | 3.17 | 3.84 | 1580 | 163 | 136.00 | 3400 | 0.9 | 443 | 55.07 | 0 |
| PostgreSql | Scarlet EF Core | 13.97 | 17.21 | 1580 | 40 | 422.47 | 3421 | 0.8 | 82 | 255.17 | 0 |
| PostgreSql | Wrapper on Scarlet persistence | 4.35 | 4.71 | 2432 | 97 | 195.84 | 62931 | 16.6 | 389 | 53.13 | 0 |
| PostgreSql | Wrapper on Orleans ADO.NET persistence | 4.32 | 5.87 | n/a | 96 | 195.25 | n/a | n/a | 363 | 55.25 | 0 |
| SqlServer | Scarlet ADO.NET | 8.00 | 15.26 | 1580 | 59 | 460.23 | 3403 | 0.9 | 239 | 101.56 | 0 |
| SqlServer | Scarlet EF Core | 19.40 | 30.11 | 1580 | 24 | 815.48 | 3427 | 0.9 | 76 | 254.82 | 0 |
| SqlServer | Wrapper on Scarlet persistence | 11.04 | 18.01 | 2432 | 59 | 470.97 | 54590 | 15.1 | 250 | 92.47 | 0 |
| SqlServer | Wrapper on Orleans ADO.NET persistence | 7.76 | 18.55 | n/a | 51 | 426.80 | n/a | n/a | 231 | 96.19 | 0 |
| MySql | Scarlet ADO.NET | 15.83 | 22.77 | 1580 | 28 | 692.81 | 3361 | 1.0 | 126 | 203.20 | 0 |
| MySql | Scarlet EF Core | 25.32 | 50.24 | 1580 | 15 | 1454.22 | 3428 | 0.9 | 58 | 356.91 | 0 |
| MySql | Wrapper on Scarlet persistence | 12.54 | 21.42 | 2432 | 26 | 778.69 | 56779 | 15.4 | 128 | 425.82 | 0 |
| MySql | Wrapper on Orleans ADO.NET persistence | 13.16 | 23.93 | n/a | 30 | 700.14 | n/a | n/a | 138 | 304.29 | 0 |
