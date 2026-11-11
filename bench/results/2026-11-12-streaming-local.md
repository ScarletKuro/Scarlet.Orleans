# Local streaming results, 2026-11-12

Two consecutive complete runs on one developer machine: Windows (build 26300), 16 logical processors, .NET 10.0.15,
local Docker containers (PostgreSQL 17, SQL Server 2022, MySQL 8.4), and a SQLite file in WAL mode, connected through
`127.0.0.1`. Client and databases share the machine, so the numbers compare providers within a run, and they move
between runs: the second ran on a busier machine, with enqueue latencies about twice the first's.

The in-box provider is `Microsoft.Orleans.Streaming.AdoNet` 10.4.0-alpha.1, from its v10.4.0 scripts. Its MySQL script
does not install (a syntax error at `CREATE PROCEDURE GetStreamMessages`, the script problem listed in section 2 of the
design), so it is not measured on MySQL. SQLite runs Scarlet's provider alone.

Each provider runs in one silo per engine with four queues. Enqueue: 1,000 events sent one at a time from a client.
Delivery: 16 client producers, each sending to its own stream for 10 seconds as fast as they can, read by one implicitly
subscribed consumer grain per stream; throughput counts deliveries over the time from the first send to the last
delivery, and latency runs from send to consumer. A provider that enqueues faster carries more load there, so the paced
run sends one event every 20 ms per producer (800 per second in all) and compares latency at the same load. Latencies
are in milliseconds. Every event was delivered in every run, none twice.

## Run 1

| Engine | Provider | Enqueue p50 | Enqueue p99 | Sent | Delivered/s | Delivery p50 | Delivery p99 | Paced p50 | Paced p99 | Duplicates |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.97 | 4.11 | 9735 | 859 | 3513.4 | 8921.4 | 377.2 | 3539.7 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.33 | 5.33 | 21507 | 2141 | 13.8 | 106.9 | 58.1 | 118.7 | 0 |
| PostgreSql | Orleans ADO.NET | 2.40 | 5.51 | 20693 | 2062 | 14.2 | 109.9 | 60.8 | 124.7 | 0 |
| SqlServer | Scarlet ADO.NET | 6.57 | 13.68 | 9946 | 980 | 49.3 | 2679.3 | 59.1 | 1416.3 | 0 |
| SqlServer | Orleans ADO.NET | 14.58 | 37.26 | 2782 | 276 | 74.7 | 171.6 | 80.2 | 199.6 | 0 |
| MySql | Orleans ADO.NET | failed, see below |  |  |  |  |  |  |  |  |
| MySql | Scarlet ADO.NET | 21.95 | 37.40 | 5139 | 510 | 70.9 | 211.3 | 70.7 | 237.4 | 0 |

## Run 2

| Engine | Provider | Enqueue p50 | Enqueue p99 | Sent | Delivered/s | Delivery p50 | Delivery p99 | Paced p50 | Paced p99 | Duplicates |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 5.03 | 11.81 | 2308 | 201 | 3730.4 | 8519.9 | 850.5 | 4185.5 | 0 |
| PostgreSql | Scarlet ADO.NET | 5.57 | 9.13 | 19668 | 1946 | 18.1 | 123.5 | 66.4 | 126.6 | 0 |
| PostgreSql | Orleans ADO.NET | 5.57 | 12.27 | 20099 | 2004 | 17.6 | 121.7 | 65.4 | 126.6 | 0 |
| SqlServer | Scarlet ADO.NET | 7.53 | 16.52 | 8398 | 835 | 197.1 | 3371.7 | 50.6 | 304.9 | 0 |
| SqlServer | Orleans ADO.NET | 7.64 | 18.21 | 3302 | 328 | 66.4 | 186.7 | 58.7 | 168.0 | 0 |
| MySql | Orleans ADO.NET | failed, see below |  |  |  |  |  |  |  |  |
| MySql | Scarlet ADO.NET | 19.27 | 32.50 | 5209 | 517 | 69.1 | 233.0 | 1472.3 | 6004.8 | 0 |

## Reading the results

- **PostgreSQL**: the two providers are level on enqueue latency, throughput and delivery latency, paced or not.
- **MySQL**: Scarlet's provider delivers about 510 events per second, what the producers send on this commit-bound
  setup (enqueue p50 of about 20 ms), with a delivery p50 of 70 ms. The paced load of 800 per second is above that,
  and the second run's paced phase fell behind. The in-box provider cannot be compared.
- **SQL Server**: Scarlet's provider delivered three times the in-box provider's throughput in both runs, but its
  delivery p99 reached seconds, paced as well as not, where the in-box provider's stayed under 200 ms at its lower
  throughput. With SQL Server alone on the machine, the paced p99s were 142 ms and 129 ms, and these two runs, with every
  engine busy, show SQL Server's enqueue latency at two to four times that. Whether the tail is the shared machine or
  Scarlet's SQL Server path is open; it needs a run on a dedicated server.
- **SQLite**: its single writer serializes enqueues, dequeues and confirmations, so delivery falls behind sending and
  latency grows to seconds.
