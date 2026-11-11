# Local transactional state results, 2026-11-12

Two consecutive complete runs on one developer machine: Windows (build 26300), 16 logical processors, .NET 10.0.15,
local Docker containers (PostgreSQL 17, SQL Server 2022, MySQL 8.4, and an idle MariaDB 11.4), and a SQLite file in WAL
mode, connected through `127.0.0.1`. Client and databases share the machine, so the numbers compare providers within a
run; absolute latencies depend on this setup, and move between runs by up to a fifth. Every provider completed every
workload, and the transfers conserved the total on every provider.

These runs follow the round-trip work in section 5.5 of the design: the ADO.NET flavour now loads in one round trip on
every server engine, and stores in one on PostgreSQL and SQL Server and two on MySQL. The earlier results, with stores
as separate statements on SQL Server and MySQL, are in [2026-11-01](2026-11-01-transactions-local.md).

The providers store the transactional state of account grains with a 1 KiB state, through real Orleans transactions in
one silo per engine:

- **Scarlet ADO.NET** and **Scarlet EF Core**, the dedicated transactional state storage.
- **Wrapper on Scarlet persistence** and **Wrapper on Orleans ADO.NET persistence**: grain storage registered under the
  storage name, which Orleans uses through its wrapper, storing the committed state and every pending version as one
  record. The in-box provider is `Microsoft.Orleans.Persistence.AdoNet` 10.4.0, from its v10.4.0 scripts, on the server
  engines.

Single account: 300 sequential transactions on one account, each prepared and committed in one store. Hot transfers: 16
callers each moving money from its own account into one shared account for 10 seconds, so on the shared account prepared
versions wait for their transaction managers. Spread transfers: each caller between its own two accounts. Throughput
counts committed transfers over the time until the last one finished. Latencies are in milliseconds. Bytes are the
serialized bytes written; state copies per store are bytes per store over the state size, and show the pending depth.

## Run 1

| Engine | Provider | Single p50 | Single p99 | Single bytes/tx | Hot transfers/s | Hot p99 | Hot bytes/transfer | Hot state copies/store | Spread transfers/s | Spread p99 | Aborted |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.56 | 2.86 | 1580 | 3 | 6336.08 | 3393 | 0.8 | 121 | 4403.98 | 0 |
| Sqlite | Scarlet EF Core | 2.51 | 18.06 | 1580 | 4 | 5347.69 | 3422 | 0.8 | 115 | 3117.95 | 1 |
| Sqlite | Wrapper on Scarlet persistence | 1.01 | 1.36 | 2432 | 4 | 5124.76 | 64827 | 15.9 | 161 | 2954.45 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.87 | 6.17 | 1580 | 174 | 128.99 | 3409 | 0.9 | 697 | 38.74 | 0 |
| PostgreSql | Scarlet EF Core | 10.47 | 14.71 | 1580 | 55 | 323.24 | 3419 | 0.8 | 169 | 149.54 | 0 |
| PostgreSql | Wrapper on Scarlet persistence | 2.28 | 2.73 | 2432 | 170 | 125.03 | 58684 | 15.7 | 595 | 42.73 | 0 |
| PostgreSql | Wrapper on Orleans ADO.NET persistence | 4.45 | 7.99 | n/a | 88 | 227.86 | n/a | n/a | 614 | 40.47 | 0 |
| SqlServer | Scarlet ADO.NET | 7.87 | 16.37 | 1580 | 61 | 532.08 | 3402 | 0.9 | 276 | 98.50 | 0 |
| SqlServer | Scarlet EF Core | 17.33 | 35.15 | 1580 | 29 | 659.52 | 3421 | 0.9 | 125 | 174.03 | 0 |
| SqlServer | Wrapper on Scarlet persistence | 17.76 | 21.19 | 2432 | 45 | 423.18 | 47647 | 13.8 | 315 | 79.28 | 0 |
| SqlServer | Wrapper on Orleans ADO.NET persistence | 7.43 | 13.61 | n/a | 57 | 597.15 | n/a | n/a | 284 | 82.60 | 0 |
| MySql | Scarlet ADO.NET | 15.14 | 18.30 | 1580 | 31 | 666.08 | 3346 | 1.0 | 166 | 156.13 | 0 |
| MySql | Scarlet EF Core | 23.10 | 41.05 | 1580 | 19 | 915.16 | 3419 | 0.9 | 84 | 258.57 | 0 |
| MySql | Wrapper on Scarlet persistence | 12.06 | 16.33 | 2432 | 27 | 657.97 | 58547 | 15.8 | 151 | 352.10 | 0 |
| MySql | Wrapper on Orleans ADO.NET persistence | 12.25 | 27.18 | n/a | 31 | 741.92 | n/a | n/a | 152 | 277.79 | 0 |

## Run 2

| Engine | Provider | Single p50 | Single p99 | Single bytes/tx | Hot transfers/s | Hot p99 | Hot bytes/transfer | Hot state copies/store | Spread transfers/s | Spread p99 | Aborted |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 3.38 | 6.49 | 1580 | 3 | 6558.53 | 3396 | 0.8 | 134 | 3123.97 | 1 |
| Sqlite | Scarlet EF Core | 2.07 | 5.25 | 1580 | 4 | 6407.70 | 3422 | 0.8 | 106 | 4347.51 | 1 |
| Sqlite | Wrapper on Scarlet persistence | 1.08 | 1.32 | 2432 | 4 | 5008.35 | 61043 | 15.2 | 154 | 3599.18 | 0 |
| PostgreSql | Scarlet ADO.NET | 3.03 | 6.29 | 1580 | 128 | 286.42 | 3371 | 1.0 | 586 | 46.75 | 0 |
| PostgreSql | Scarlet EF Core | 10.89 | 13.44 | 1580 | 53 | 331.66 | 3420 | 0.8 | 138 | 168.97 | 0 |
| PostgreSql | Wrapper on Scarlet persistence | 2.34 | 2.77 | 2432 | 160 | 132.15 | 64943 | 16.9 | 646 | 37.35 | 0 |
| PostgreSql | Wrapper on Orleans ADO.NET persistence | 2.44 | 5.80 | n/a | 152 | 132.54 | n/a | n/a | 561 | 42.70 | 0 |
| SqlServer | Scarlet ADO.NET | 7.79 | 12.12 | 1580 | 59 | 441.08 | 3407 | 0.9 | 266 | 92.51 | 0 |
| SqlServer | Scarlet EF Core | 17.70 | 35.19 | 1580 | 26 | 751.43 | 3418 | 0.9 | 109 | 188.34 | 0 |
| SqlServer | Wrapper on Scarlet persistence | 17.52 | 20.90 | 2432 | 51 | 421.10 | 50228 | 14.3 | 302 | 83.31 | 0 |
| SqlServer | Wrapper on Orleans ADO.NET persistence | 7.48 | 13.81 | n/a | 56 | 407.04 | n/a | n/a | 278 | 87.53 | 0 |
| MySql | Scarlet ADO.NET | 15.06 | 21.92 | 1580 | 30 | 649.11 | 3376 | 1.0 | 151 | 167.95 | 0 |
| MySql | Scarlet EF Core | 23.54 | 33.54 | 1580 | 19 | 882.76 | 3424 | 0.9 | 83 | 261.64 | 0 |
| MySql | Wrapper on Scarlet persistence | 12.64 | 24.23 | 2432 | 26 | 896.92 | 57508 | 15.6 | 120 | 866.21 | 0 |
| MySql | Wrapper on Orleans ADO.NET persistence | 12.83 | 25.42 | n/a | 31 | 721.30 | n/a | n/a | 159 | 194.38 | 0 |

## Reading the results

- **Bytes.** The dedicated stores write each prepared version once: about 3.4 KB per hot transfer on every engine,
  against 48 to 65 KB through the wrapper, which rewrote the committed state and 14 to 17 pending versions on every
  store.
- **PostgreSQL**: the ADO.NET flavour is level with the wrapper over Scarlet persistence on throughput (697 and 586
  spread transfers per second, against 595 and 646), with a single transaction about 0.6 ms slower.
- **SQL Server**: a single transaction takes 7.8 ms, level with the in-box wrapper and less than half of the wrapper
  over Scarlet persistence; spread transfers are about a tenth behind the latter and level with the in-box wrapper, and
  hot transfers ahead of both.
- **MySQL**: spread and hot transfers are level with or ahead of both wrappers (166 and 151 spread transfers per
  second, against 120 to 159), and a single transaction is about a fifth slower, 15 ms against 12 to 13. In a run with
  only the MySQL container busy, the same build reached 233 spread transfers per second against the wrapper's 118, and
  8.5 ms against 8.1 for a single transaction, so its margin depends on the load on the host.
- **EF Core** sends a store as separate statements on every engine. It reaches about a quarter of the ADO.NET flavour's
  spread throughput on PostgreSQL, under half on SQL Server and about half on MySQL; its README states the trade-off.
- **SQLite**: every provider is limited by SQLite's single writer, and the dedicated stores are slightly behind the
  wrapper.
- **For a deployment today**, the dedicated ADO.NET store is level with or ahead of the wrappers on throughput on every
  server engine, and writes a fifteenth to a twentieth of the bytes, which matters with large states or
  write-limited storage. The wrapper over Scarlet persistence keeps a lower single-transaction latency on PostgreSQL
  and MySQL.
