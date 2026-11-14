# Local clustering results, 2026-11-14–2026-11-15

Two consecutive runs on Windows 11, a Ryzen 7 5800X3D (16 logical processors), .NET 10.0.15, and local Docker databases through `127.0.0.1`. One benchmark process ran at a time. See the [all-suite report](2026-11-14-local.md) for versions, harness corrections, scope, and comparison limits.

Measurements: 2026-11-14 23:49 to 2026-11-15 00:05, Europe/Tallinn. Filenames use the campaign start date.

16 logical processors, .NET 10.0.15. A table of 100 silos; 1000 operations per sequential workload; 32 concurrent silos for 10 s per concurrent workload. Latencies in milliseconds; cleanup is one call removing 100 dead silos. A contended update reads the table and swaps the silo's row with the next version, as silos do; a conflict is another silo's update coming first.

## Reading the results

- **Real heartbeats:** both Scarlet flavours are within about 8% of the in-box provider's server-engine throughput in both runs. PostgreSQL is 1,967–2,038 heartbeats/s across providers, SQL Server 1,711–1,824, and MySQL 961–1,037. The data supports broadly comparable steady heartbeat throughput, rather than a general Scarlet lead.
- **SQLite:** ADO.NET reaches 853–870 advancing heartbeats/s versus EF Core's 629–648, a paired advantage of 34–36%. The previous 54,811/s ADO.NET figure came from future-dated rows that concurrent heartbeats did not advance; it is not a real-write baseline for these results. Every worker's stored timestamp advances in both retained runs.
- **Cleanup is much cheaper through Scarlet.** Removing 100 dead silos takes 9.80–10.35 ms for PostgreSQL ADO.NET and 16.95–17.86 ms for EF Core, against 242.01–246.55 ms in-box. SQL Server is 27.93–31.30 ms across the Scarlet flavours versus 403.76–435.99 ms; MySQL is 13.09–19.44 ms versus 634.44–636.75 ms. Across these cases the paired cleanup advantage is about 13–49 times.
- **Reads and membership churn carry a cost.** Scarlet's server read-all p50 is about 2.3–3.3 times the in-box provider's. Contended versioned updates reach 7–12/s across Scarlet providers versus 16–39/s in-box, about 2.3–4.3 times fewer successful updates in the paired runs. These paths perform additional transactional work; cleanup and steady heartbeats do not predict their performance.
- Both invocations pass the insert, row-count, versioned-update, cleanup, and advancing-heartbeat checks, with zero concurrent errors. The benchmark does not exercise failure detection or prove crash-recovery behavior.

## Run 1

| Engine | Provider | Insert p50 | Read all p50 | Read all p99 | Read row p50 | Update p50 | Update p99 | Heartbeat p50 | Heartbeat p99 | Cleanup ms | Concurrent heartbeats/s | Concurrent heartbeat p99 | Contended updates/s | Conflicts/s | Errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.03 | 0.52 | 0.98 | 0.07 | 0.95 | 4.10 | 0.88 | 2.18 | 10.02 | 853 | 4.02 | 549 | 0 | 0 |
| Sqlite | Scarlet EF Core | 1.63 | 0.87 | 1.34 | 0.54 | 1.59 | 4.53 | 1.05 | 1.82 | 24.50 | 629 | 4.19 | 376 | 1 | 0 |
| PostgreSql | Scarlet ADO.NET | 6.12 | 4.22 | 5.09 | 3.66 | 4.77 | 5.79 | 2.29 | 2.73 | 10.35 | 1996 | 22.83 | 12 | 327 | 0 |
| PostgreSql | Scarlet EF Core | 8.69 | 4.65 | 5.98 | 3.91 | 5.07 | 8.12 | 2.45 | 5.69 | 17.86 | 1967 | 22.99 | 11 | 319 | 0 |
| PostgreSql | Orleans ADO.NET | 2.61 | 1.84 | 2.19 | 1.46 | 2.46 | 3.02 | 2.38 | 3.29 | 246.55 | 2020 | 22.88 | 37 | 956 | 0 |
| SqlServer | Scarlet ADO.NET | 9.47 | 5.97 | 7.09 | 5.19 | 7.76 | 13.88 | 3.87 | 5.89 | 28.41 | 1726 | 32.60 | 9 | 273 | 0 |
| SqlServer | Scarlet EF Core | 11.01 | 6.27 | 7.29 | 5.55 | 8.13 | 11.54 | 4.10 | 4.71 | 31.30 | 1711 | 30.92 | 9 | 273 | 0 |
| SqlServer | Orleans ADO.NET | 3.99 | 1.89 | 2.25 | 1.38 | 3.92 | 5.79 | 3.88 | 7.07 | 435.99 | 1824 | 29.35 | 39 | 940 | 0 |
| MySql | Scarlet ADO.NET | 11.18 | 6.74 | 7.71 | 6.13 | 9.85 | 13.75 | 6.14 | 12.12 | 13.17 | 961 | 48.69 | 7 | 210 | 0 |
| MySql | Scarlet EF Core | 13.75 | 6.91 | 8.09 | 6.27 | 10.44 | 16.68 | 6.33 | 12.62 | 18.72 | 977 | 48.66 | 7 | 212 | 0 |
| MySql | Orleans ADO.NET | 6.72 | 2.92 | 3.47 | 2.44 | 6.00 | 9.32 | 6.16 | 12.03 | 636.75 | 1036 | 37.67 | 16 | 502 | 0 |

## Run 2

| Engine | Provider | Insert p50 | Read all p50 | Read all p99 | Read row p50 | Update p50 | Update p99 | Heartbeat p50 | Heartbeat p99 | Cleanup ms | Concurrent heartbeats/s | Concurrent heartbeat p99 | Contended updates/s | Conflicts/s | Errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.99 | 0.66 | 1.15 | 0.07 | 0.93 | 1.64 | 0.86 | 2.87 | 8.29 | 870 | 4.00 | 549 | 1 | 0 |
| Sqlite | Scarlet EF Core | 1.61 | 0.87 | 1.36 | 0.53 | 1.62 | 4.77 | 1.09 | 4.24 | 24.16 | 648 | 4.18 | 392 | 1 | 0 |
| PostgreSql | Scarlet ADO.NET | 6.18 | 4.23 | 5.23 | 3.62 | 4.77 | 7.75 | 2.29 | 2.64 | 9.80 | 2000 | 22.80 | 11 | 329 | 0 |
| PostgreSql | Scarlet EF Core | 8.69 | 4.62 | 5.34 | 3.93 | 5.05 | 8.25 | 2.46 | 5.03 | 16.95 | 1989 | 23.13 | 11 | 323 | 0 |
| PostgreSql | Orleans ADO.NET | 2.58 | 1.83 | 2.19 | 1.45 | 2.46 | 5.72 | 2.35 | 5.56 | 242.01 | 2038 | 22.13 | 33 | 975 | 0 |
| SqlServer | Scarlet ADO.NET | 9.41 | 5.98 | 7.04 | 5.21 | 7.75 | 10.90 | 3.88 | 7.18 | 27.93 | 1764 | 36.14 | 9 | 276 | 0 |
| SqlServer | Scarlet EF Core | 10.93 | 6.27 | 7.26 | 5.47 | 8.12 | 11.37 | 4.08 | 7.44 | 30.43 | 1765 | 31.42 | 9 | 276 | 0 |
| SqlServer | Orleans ADO.NET | 4.00 | 1.89 | 2.44 | 1.39 | 3.94 | 7.12 | 3.91 | 7.25 | 403.76 | 1789 | 30.32 | 31 | 954 | 0 |
| MySql | Scarlet ADO.NET | 11.33 | 6.73 | 8.19 | 6.08 | 9.82 | 15.85 | 6.12 | 12.02 | 13.09 | 994 | 45.70 | 7 | 213 | 0 |
| MySql | Scarlet EF Core | 13.64 | 6.89 | 8.31 | 6.27 | 10.23 | 13.68 | 6.26 | 12.23 | 19.44 | 984 | 49.65 | 7 | 213 | 0 |
| MySql | Orleans ADO.NET | 6.44 | 2.92 | 3.83 | 2.44 | 6.00 | 10.00 | 6.16 | 10.36 | 634.44 | 1037 | 38.86 | 16 | 502 | 0 |
