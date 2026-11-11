# Local streaming results, 2026-11-12

Two consecutive complete runs on one developer machine: Windows (build 26300), 16 logical processors, .NET 10.0.15,
local Docker containers (PostgreSQL 17, SQL Server 2022, MySQL 8.4), and a SQLite file in WAL mode, connected through
`127.0.0.1`. Client and databases share the machine, so the numbers compare providers within a run. The second run ran
on a busier machine: every enqueue latency, for both providers, is about twice the first run's.

The in-box provider is `Microsoft.Orleans.Streaming.AdoNet` 10.4.0-alpha.1, from its v10.4.0 scripts. Its MySQL script
does not install (a syntax error at `CREATE PROCEDURE GetStreamMessages`, the script problem listed in section 2 of the
design), so it is not measured on MySQL. SQLite runs Scarlet's provider alone.

Each provider runs in one silo per engine with four queues. Enqueue: 1,000 events sent one at a time from a client.
Delivery: 16 client producers, each sending to its own stream for 10 seconds, read by one implicitly subscribed consumer
grain per stream; throughput counts deliveries over the time from the first send to the last delivery, and latency runs
from send to consumer. Latencies are in milliseconds. Every event was delivered in every run, none twice.

## Run 1

| Engine | Provider | Enqueue p50 | Enqueue p99 | Sent | Delivered/s | Delivery p50 | Delivery p99 | Duplicates |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.00 | 4.16 | 9781 | 836 | 5367.2 | 8838.0 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.36 | 5.63 | 22425 | 2234 | 14.0 | 66.6 | 0 |
| PostgreSql | Orleans ADO.NET | 2.49 | 5.44 | 21567 | 2143 | 13.6 | 114.4 | 0 |
| SqlServer | Scarlet ADO.NET | 3.94 | 9.79 | 15284 | 1495 | 25.3 | 267.7 | 0 |
| SqlServer | Orleans ADO.NET | 3.95 | 7.90 | 10884 | 1082 | 24.4 | 157.9 | 0 |
| MySql | Orleans ADO.NET | failed, see below |  |  |  |  |  |  |
| MySql | Scarlet ADO.NET | 9.26 | 29.50 | 5139 | 507 | 1160.6 | 4654.8 | 0 |

## Run 2

| Engine | Provider | Enqueue p50 | Enqueue p99 | Sent | Delivered/s | Delivery p50 | Delivery p99 | Duplicates |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 2.72 | 6.06 | 3577 | 304 | 6056.2 | 11072.4 | 0 |
| PostgreSql | Scarlet ADO.NET | 5.55 | 9.91 | 19480 | 1939 | 17.7 | 121.6 | 0 |
| PostgreSql | Orleans ADO.NET | 5.52 | 9.03 | 20223 | 2015 | 17.4 | 120.3 | 0 |
| SqlServer | Scarlet ADO.NET | 7.30 | 13.65 | 8853 | 874 | 60.9 | 4038.8 | 0 |
| SqlServer | Orleans ADO.NET | 7.60 | 19.92 | 2756 | 273 | 73.6 | 179.6 | 0 |
| MySql | Orleans ADO.NET | failed, see below |  |  |  |  |  |  |
| MySql | Scarlet ADO.NET | 19.41 | 34.75 | 5138 | 501 | 602.1 | 2262.4 | 0 |

## Reading the results

- **PostgreSQL**: the two providers are level, on enqueue latency, throughput and delivery latency.
- **SQL Server**: Scarlet's provider delivered more in both runs (1,495 and 874 events per second, against 1,082 and
  273). Its delivery p99 was higher in both runs (268 ms against 158, and 4 seconds against 180 on the busier
  machine), so its tail latency on SQL Server needs a closer look.
- **MySQL**: Scarlet's provider keeps up with what the producers send (about 500 events per second), but delivery
  latency reaches one to five seconds. Every write commits slowly on this setup (enqueue p50 of 9 to 19 ms), and each
  read is two round trips and a commit; the in-box provider cannot be compared.
- **SQLite**: its single writer serializes enqueues, dequeues and confirmations, so delivery falls behind sending and
  latency grows to seconds over the run.
