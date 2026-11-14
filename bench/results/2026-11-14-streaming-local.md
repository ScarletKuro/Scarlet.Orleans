# Local streaming results, 2026-11-15

Two consecutive runs on Windows 11, a Ryzen 7 5800X3D (16 logical processors), .NET 10.0.15, and local Docker databases through `127.0.0.1`. One benchmark process ran at a time. See the [all-suite report](2026-11-14-local.md) for versions, harness corrections, scope, and comparison limits.

Measurements: 2026-11-15 00:05 to 2026-11-15 00:13, Europe/Tallinn. Filenames use the campaign start date.

16 logical processors, .NET 10.0.15, 4 queues per provider. Enqueue: 1000 events sent one at a time from a client. Delivery: 16 client producers, each sending to its own stream for 10 s as fast as they can, read by one implicitly subscribed consumer grain per stream; throughput counts deliveries over the time from the first send to the last delivery, and latency runs from send to consumer. Faster enqueues mean more load, so the paced run offers one event every 20 ms per producer (800 events per second), sending without waiting for earlier sends, and compares latency at that load, measured from when each event was due; its sent rate, the events over the time until the last send completed, shows whether the provider took it. Each event is counted once; further deliveries of it are duplicates. Latencies in milliseconds. Every event must be delivered within 120 s of the last send, or the run is incomplete; deliveries beyond the events sent are duplicates.

## Reading the results

- **PostgreSQL:** after budgeting both pools at 40 connections, both providers complete both runs. Scarlet delivers 1,199–1,543 events/s versus 1,197–1,489 in-box, within 4% in the paired runs. Both accept the paced 800/s schedule, with paced p99 of 134.0–145.6 ms for Scarlet and 136.5–142.6 ms in-box. These runs support broadly comparable performance.
- **SQL Server:** Scarlet completes both runs at 782–1,254 delivered events/s. The in-box provider reaches 265/s in the only complete pair, then fails in run 2 while waiting for a pooled connection. The paced workload exposes large tails: Scarlet p99 ranges from 304.5 to 9,564.0 ms, and the completed in-box run reaches 18,494.5 ms while completing sends at only 335/s. There is insufficient complete baseline data for a repeatable two-run ranking, and Scarlet's throughput does not establish consistently low paced latency.
- **MySQL:** Scarlet delivers 446–452 events/s with the 16 closed-loop producers. The open-loop 800/s schedule completes sends at 798/s, with paced p99 around 1,485 ms. The load-generation modes have different outstanding concurrency, so the closed-loop rate is not a measurement of maximum sustainable capacity. The in-box schema script fails to install in both runs; no MySQL baseline performance comparison is available.
- **SQLite:** delivery throughput varies from 316 to 911 events/s. The fixed 800/s schedule completes sends at only 179–296/s, with paced p99 of 23.66–37.12 seconds. This offered load creates a substantial backlog; a fast isolated enqueue median does not describe delivery latency.
- **Completion and comparison limits:** all events are received within the drain deadline for every completed provider row, with zero duplicates observed during those measurement phases. This does not prove exactly-once behavior. Paced latency includes waiting from the scheduled due time, so its values are not directly comparable with the November 12 report. The original unbudgeted PostgreSQL attempts are excluded as a connection-budgeting error in the harness, while the retained SQL Server pool timeout remains visible.

## Run 1

| Engine | Provider | Enqueue p50 | Enqueue p99 | Sent | Delivered/s | Delivery p50 | Delivery p99 | Paced sent/s | Paced p50 | Paced p99 | Duplicates |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.97 | 2.71 | 10123 | 911 | 4509.0 | 9154.9 | 296 | 12581.5 | 23659.1 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.60 | 3.73 | 15489 | 1543 | 20.0 | 132.6 | 800 | 70.7 | 134.0 | 0 |
| PostgreSql | Orleans ADO.NET | 2.65 | 5.50 | 14953 | 1489 | 18.3 | 129.5 | 800 | 73.7 | 136.5 | 0 |
| SqlServer | Scarlet ADO.NET | 4.24 | 9.01 | 12607 | 1254 | 30.0 | 362.6 | 799 | 4922.2 | 9564.0 | 0 |
| SqlServer | Orleans ADO.NET | 7.88 | 17.51 | 2675 | 265 | 79.9 | 175.9 | 335 | 13420.8 | 18494.5 | 0 |
| MySql | Orleans ADO.NET | failed, see below |  |  |  |  |  |  |  |  |  |
| MySql | Scarlet ADO.NET | 21.03 | 35.08 | 4629 | 452 | 82.5 | 532.7 | 798 | 189.2 | 1485.8 | 0 |

### Incomplete runs

- MySql / Orleans ADO.NET: its schema scripts failed to install: MySqlException: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'CREATE PROCEDURE GetStreamMessages

## Run 2

| Engine | Provider | Enqueue p50 | Enqueue p99 | Sent | Delivered/s | Delivery p50 | Delivery p99 | Paced sent/s | Paced p50 | Paced p99 | Duplicates |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 2.75 | 7.31 | 3552 | 316 | 3347.4 | 7594.1 | 179 | 25217.1 | 37115.6 | 0 |
| PostgreSql | Scarlet ADO.NET | 5.53 | 8.01 | 12054 | 1199 | 26.1 | 136.9 | 800 | 72.8 | 145.6 | 0 |
| PostgreSql | Orleans ADO.NET | 5.48 | 7.83 | 12039 | 1197 | 25.9 | 122.6 | 800 | 70.6 | 142.6 | 0 |
| SqlServer | Scarlet ADO.NET | 7.64 | 16.33 | 7848 | 782 | 45.7 | 341.6 | 798 | 89.8 | 304.5 | 0 |
| SqlServer | Orleans ADO.NET | failed, see below |  |  |  |  |  |  |  |  |  |
| MySql | Orleans ADO.NET | failed, see below |  |  |  |  |  |  |  |  |  |
| MySql | Scarlet ADO.NET | 20.37 | 35.42 | 4600 | 446 | 83.2 | 362.6 | 798 | 129.0 | 1485.0 | 0 |

### Incomplete runs

- SqlServer / Orleans ADO.NET: InvalidOperationException: Timeout expired.  The timeout period elapsed prior to obtaining a connection from the pool.  This may have occurred because all pooled connections were in use and max pool size was reached.
- MySql / Orleans ADO.NET: its schema scripts failed to install: MySqlException: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'CREATE PROCEDURE GetStreamMessages
