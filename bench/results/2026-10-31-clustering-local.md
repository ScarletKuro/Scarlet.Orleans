# Local clustering results, 2026-10-31

One complete run on one developer machine: Windows (build 26300), 16 logical processors, .NET 10.0.14, local Docker
containers (PostgreSQL 17, SQL Server 2022, MySQL 8.4), and a SQLite file in WAL mode. Client and databases share the
machine, so the numbers compare providers within a run; absolute latencies depend on this setup. All sequential checks
passed, and no workload reported an error.

The Orleans baseline is `Microsoft.Orleans.Clustering.AdoNet` 10.4.0 with default options, set up from its own v10.4.0
scripts (commit `d114d7522a53b8cce4c6b97d44293e5c7273d766`, vendored unchanged). It runs on the three server engines;
SQLite compares the two Scarlet providers. Each provider has its own silo host, built but not started, and the benchmark
drives its `IMembershipTable` directly.

Each provider inserts 100 silos, reading the table before each insert as a silo does, then runs 1,000 reads of the whole
table, 1,000 row reads, 1,000 versioned updates (each after an untimed row read) and 1,000 heartbeats. Cleanup times one
call removing another 100 dead silos. Then 32 silos heartbeat their own rows for 10 seconds, and 32 silos read the table
and update their own row with the next version for 10 seconds; a conflict is another silo's update coming first.

Connections use `127.0.0.1`. Through `localhost`, Docker Desktop's forwarding on this machine stalled about one round
trip in a thousand for about 300 ms. The Scarlet providers take more round trips per write, so their update p99 showed
it (308 ms on PostgreSQL) and, as a stall holds the version row, contended throughput halved. The server's slow-query
log showed no slow statement, only lock waits behind the stalled one.

| Engine | Provider | Insert p50 | Read all p50 | Read all p99 | Read row p50 | Update p50 | Update p99 | Heartbeat p50 | Heartbeat p99 | Cleanup ms | Concurrent heartbeats/s | Concurrent heartbeat p99 | Contended updates/s | Conflicts/s | Errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.00 | 0.53 | 0.89 | 0.07 | 0.95 | 4.03 | 0.88 | 4.04 | 8.10 | 54811 | 0.03 | 549 | 1 | 0 |
| Sqlite | Scarlet EF Core | 1.74 | 0.87 | 1.41 | 0.55 | 1.65 | 4.79 | 1.15 | 2.58 | 25.43 | 5973 | 5.64 | 368 | 0 | 0 |
| PostgreSql | Scarlet ADO.NET | 5.11 | 3.29 | 4.13 | 2.94 | 4.01 | 5.53 | 2.11 | 4.85 | 9.26 | 4887 | 8.16 | 27 | 635 | 0 |
| PostgreSql | Scarlet EF Core | 7.23 | 3.53 | 4.69 | 3.19 | 4.36 | 7.48 | 2.27 | 2.72 | 15.99 | 4882 | 9.27 | 25 | 630 | 0 |
| PostgreSql | Orleans ADO.NET | 2.41 | 1.52 | 2.13 | 1.11 | 2.24 | 5.34 | 2.16 | 5.41 | 218.46 | 3980 | 12.30 | 111 | 1967 | 0 |
| SqlServer | Scarlet ADO.NET | 8.44 | 4.88 | 5.81 | 4.34 | 6.93 | 9.97 | 3.70 | 6.79 | 27.66 | 4783 | 8.84 | 19 | 580 | 0 |
| SqlServer | Scarlet EF Core | 9.64 | 5.15 | 6.29 | 4.56 | 7.22 | 10.47 | 3.86 | 7.08 | 29.89 | 4697 | 10.60 | 20 | 562 | 0 |
| SqlServer | Orleans ADO.NET | 3.73 | 1.53 | 3.04 | 1.17 | 3.72 | 6.98 | 3.66 | 6.92 | 366.26 | 2611 | 22.42 | 87 | 1788 | 0 |
| MySql | Scarlet ADO.NET | 9.87 | 5.19 | 6.25 | 4.87 | 8.81 | 12.88 | 5.79 | 9.67 | 12.02 | 2557 | 15.89 | 15 | 460 | 0 |
| MySql | Scarlet EF Core | 11.92 | 5.47 | 6.69 | 5.12 | 9.18 | 14.96 | 6.04 | 9.58 | 17.46 | 2499 | 16.71 | 15 | 454 | 0 |
| MySql | Orleans ADO.NET | 6.50 | 2.38 | 2.94 | 1.98 | 5.54 | 9.52 | 6.00 | 12.24 | 639.58 | 2509 | 19.07 | 36 | 1128 | 0 |

## Reading the results

- **Heartbeats**, the operation every silo repeats, are one statement in every provider and cost the same; concurrent
  heartbeat throughput is equal or higher for the Scarlet providers, with lower p99.
- **Cleanup** is one set-based delete under the version row lock, 9 to 28 ms, where the in-box provider took 218 to
  640 ms here.
- **Reads, inserts and updates** cost about two to three milliseconds more on server engines. A Scarlet read is a
  `REPEATABLE READ` transaction, version row then members, so it is one snapshot on every engine, SQL Server included;
  the in-box read is one statement, which on SQL Server's locking `READ COMMITTED` is not a snapshot. A Scarlet write
  is a transaction that swaps the version row and then writes the member row, where the in-box provider sends one batch.
- **Contended updates** are three to four times fewer, because each writer holds the version row across those round
  trips, and every writer of the cluster waits for it. That lock is what makes each change one version or nothing, and
  what fixes the lock order. Membership changes happen when silos join, leave or are suspected, far below even the
  lowest rate here, so the providers do not change those writes to trade it back.
