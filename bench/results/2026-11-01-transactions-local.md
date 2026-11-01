# Local transactional state results, 2026-11-01

One complete run on one developer machine: Windows (build 26300), 16 logical processors, .NET 10.0.14, local Docker
containers (PostgreSQL 17, SQL Server 2022, MySQL 8.4), and a SQLite file in WAL mode, connected through `127.0.0.1`.
Client and databases share the machine, so the numbers compare providers within a run; absolute latencies depend on this
setup. Every provider completed every workload, and the transfers conserved the total on every provider.

The providers store the transactional state of account grains with a 1 KiB state, through real Orleans transactions in
one silo per engine:

- **Scarlet ADO.NET** and **Scarlet EF Core**, the dedicated transactional state storage. On PostgreSQL the ADO.NET
  flavour loads in one statement and stores in one batch, one round trip each; on the other engines it runs a store as a
  transaction of separate statements.
- **Wrapper on Scarlet persistence** and **Wrapper on Orleans ADO.NET persistence**: grain storage registered under the
  storage name, which Orleans uses through its wrapper, storing the committed state and every pending version as one
  record. The in-box provider is `Microsoft.Orleans.Persistence.AdoNet` 10.4.0, from its v10.4.0 scripts, on the server
  engines.

Single account: 300 sequential transactions on one account, each prepared and committed in one store. Hot transfers: 16
callers each moving money from its own account into one shared account for 10 seconds, so on the shared account prepared
versions wait for their transaction managers. Spread transfers: each caller between its own two accounts. Throughput
counts committed transfers over the time until the last one finished. Bytes are the serialized bytes written; state
copies per store are bytes per store over the state size, and show the pending depth.

| Engine | Provider | Single p50 | Single p99 | Single bytes/tx | Hot transfers/s | Hot p99 | Hot bytes/transfer | Hot state copies/store | Spread transfers/s | Spread p99 | Aborted |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.49 | 4.75 | 1580 | 3 | 6469.12 | 3396 | 0.8 | 134 | 3099.95 | 0 |
| Sqlite | Scarlet EF Core | 2.08 | 5.36 | 1580 | 3 | 6246.23 | 3425 | 0.9 | 104 | 3892.09 | 1 |
| Sqlite | Wrapper on Scarlet persistence | 1.09 | 2.59 | 2432 | 4 | 4991.47 | 61303 | 15.3 | 159 | 3108.50 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.99 | 4.17 | 1580 | 168 | 143.36 | 3382 | 0.9 | 550 | 49.18 | 0 |
| PostgreSql | Scarlet EF Core | 11.79 | 20.46 | 1580 | 47 | 427.59 | 3417 | 0.9 | 111 | 188.85 | 0 |
| PostgreSql | Wrapper on Scarlet persistence | 2.44 | 3.22 | 2432 | 149 | 196.92 | 63738 | 16.7 | 522 | 54.26 | 0 |
| PostgreSql | Wrapper on Orleans ADO.NET persistence | 2.76 | 5.89 | n/a | 135 | 236.20 | n/a | n/a | 502 | 48.82 | 0 |
| SqlServer | Scarlet ADO.NET | 16.58 | 23.84 | 1580 | 30 | 684.30 | 3398 | 0.9 | 97 | 303.73 | 0 |
| SqlServer | Scarlet EF Core | 18.02 | 32.52 | 1580 | 26 | 990.12 | 3413 | 0.9 | 91 | 277.06 | 0 |
| SqlServer | Wrapper on Scarlet persistence | 7.74 | 22.59 | 2432 | 54 | 548.90 | 49351 | 14.1 | 283 | 89.46 | 0 |
| SqlServer | Wrapper on Orleans ADO.NET persistence | 7.63 | 22.56 | n/a | 50 | 499.65 | n/a | n/a | 235 | 100.89 | 0 |
| MySql | Scarlet ADO.NET | 20.46 | 25.54 | 1580 | 22 | 850.92 | 3388 | 0.9 | 78 | 285.55 | 0 |
| MySql | Scarlet EF Core | 24.82 | 32.03 | 1580 | 18 | 972.04 | 3418 | 0.9 | 71 | 297.88 | 0 |
| MySql | Wrapper on Scarlet persistence | 15.97 | 27.55 | 2432 | 22 | 845.88 | 60099 | 16.1 | 132 | 183.06 | 0 |
| MySql | Wrapper on Orleans ADO.NET persistence | 16.19 | 25.87 | n/a | 25 | 830.39 | n/a | n/a | 147 | 381.73 | 0 |

## Reading the results

- **Bytes.** The dedicated stores write each prepared version once: about 3.4 KB per hot transfer on every engine,
  against 49 to 64 KB through the wrapper, which rewrote the committed state and 14 to 17 pending versions on every
  store.
- **PostgreSQL**, where the ADO.NET flavour stores in one round trip, is the one engine where fewer bytes also mean more
  throughput: 168 hot and 550 spread transfers per second, against 149 and 522 through the wrapper over Scarlet
  persistence, with a single transaction about half a millisecond slower.
- **SQL Server and MySQL** still run a store as a transaction of six to eight separate statements, and there round trips
  outweigh bytes: the ADO.NET flavour has twice the single-transaction latency of the wrapper, and a third to three fifths
  of its spread throughput. Hot transfers are even on MySQL and about half on SQL Server. Batching these engines the way
  PostgreSQL is batched is the next step to measure; on MySQL a batch saves no round trips, so a different shape would be
  needed there.
- **EF Core** runs a store as separate statements on every engine, and costs three to five times the wrapper on
  PostgreSQL; on SQL Server and MySQL it is close to the ADO.NET flavour.
- **SQLite**: every provider is limited by SQLite's single writer, and the dedicated stores are even with the wrapper or
  slightly behind.
- **For a deployment today**, the wrapper over Scarlet persistence is the faster choice on SQL Server and MySQL, and the
  dedicated ADO.NET store on PostgreSQL; the dedicated stores write a fifteenth to a twentieth of the bytes everywhere,
  which matters with large states or write-limited storage.
