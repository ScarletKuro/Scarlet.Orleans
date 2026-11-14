# Local persistence results, 2026-11-14

Two consecutive runs on Windows 11, a Ryzen 7 5800X3D (16 logical processors), .NET 10.0.15, and local Docker databases through `127.0.0.1`. One benchmark process ran at a time. See the [all-suite report](2026-11-14-local.md) for versions, harness corrections, scope, and comparison limits.

Measurements: 2026-11-14 23:41 to 2026-11-14 23:49, Europe/Tallinn. Filenames use the campaign start date.

16 logical processors, .NET 10.0.15, 1024 B state, 32 concurrent writers for 10 s. Latencies in milliseconds.

## Reading the results

- **PostgreSQL and SQL Server:** the three providers have broadly comparable concurrent write throughput. Both Scarlet flavours are within 2% of the in-box provider on PostgreSQL and within 5% on SQL Server in the paired runs. EF Core adds roughly 0.1–0.2 ms to the Scarlet ADO.NET read and update medians, but that does not translate into a large throughput penalty here.
- **MySQL:** Scarlet reaches 961–985 writes/s across both flavours versus 806–810 in-box, a paired advantage of about 19–22%. The in-box provider still has the lowest point-read p50: 2.44–2.47 ms versus Scarlet's 2.57–2.66 ms. This is a write-throughput advantage, not a win on every operation.
- **SQLite with actual scheduled contention:** ADO.NET reaches 780–787 writes/s versus EF Core's 637–644, about 22% higher. Read p50 is 0.06 versus 0.22 ms. Concurrent p99 is 4.01 ms for ADO.NET and 4.20–4.23 ms for EF Core. Every one of the 32 writers participates in both runs.
- **Historical comparisons:** older SQLite concurrent results can represent a single synchronous writer consuming the whole window. Those values do not measure the corrected workload. Two initial uncorrected passes from this campaign are also excluded; the tables below are the corrected pair.
- Both full invocations completed with no operation failure. These are 1 KiB storage-contract workloads on one local host; they do not establish production capacity or long-running stability.

## Run 1

| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Concurrent writes/s | Concurrent p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.99 | 0.96 | 4.11 | 0.06 | 0.09 | 780 | 4.01 |
| Sqlite | Scarlet EF Core | 1.32 | 1.21 | 1.59 | 0.22 | 0.34 | 637 | 4.23 |
| PostgreSql | Scarlet ADO.NET | 2.38 | 2.34 | 5.54 | 1.36 | 1.66 | 2015 | 22.56 |
| PostgreSql | Scarlet EF Core | 2.45 | 2.49 | 2.97 | 1.52 | 1.91 | 1973 | 22.80 |
| PostgreSql | Orleans ADO.NET | 2.50 | 2.45 | 5.48 | 1.45 | 2.27 | 2011 | 22.99 |
| SqlServer | Scarlet ADO.NET | 4.01 | 3.93 | 7.53 | 1.42 | 1.83 | 1772 | 31.22 |
| SqlServer | Scarlet EF Core | 4.13 | 4.13 | 7.45 | 1.56 | 1.85 | 1830 | 28.98 |
| SqlServer | Orleans ADO.NET | 4.16 | 4.17 | 5.50 | 1.44 | 1.90 | 1818 | 30.69 |
| MySql | Scarlet ADO.NET | 6.88 | 6.23 | 12.16 | 2.59 | 3.14 | 973 | 46.82 |
| MySql | Scarlet EF Core | 7.12 | 6.33 | 12.57 | 2.62 | 3.46 | 977 | 47.52 |
| MySql | Orleans ADO.NET | 7.18 | 6.42 | 12.40 | 2.44 | 2.82 | 810 | 55.36 |

## Run 2

| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Concurrent writes/s | Concurrent p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.98 | 0.94 | 1.16 | 0.06 | 0.09 | 787 | 4.01 |
| Sqlite | Scarlet EF Core | 1.23 | 1.09 | 1.71 | 0.22 | 0.29 | 644 | 4.20 |
| PostgreSql | Scarlet ADO.NET | 2.37 | 2.34 | 5.60 | 1.40 | 1.88 | 1987 | 23.24 |
| PostgreSql | Scarlet EF Core | 2.43 | 2.47 | 5.65 | 1.51 | 1.77 | 1970 | 22.66 |
| PostgreSql | Orleans ADO.NET | 2.53 | 2.50 | 2.99 | 1.44 | 1.74 | 1995 | 23.03 |
| SqlServer | Scarlet ADO.NET | 4.02 | 3.92 | 8.01 | 1.40 | 1.67 | 1738 | 33.22 |
| SqlServer | Scarlet EF Core | 4.14 | 4.14 | 7.30 | 1.58 | 2.07 | 1729 | 31.78 |
| SqlServer | Orleans ADO.NET | 4.15 | 4.14 | 7.22 | 1.44 | 1.70 | 1820 | 31.49 |
| MySql | Scarlet ADO.NET | 6.92 | 6.26 | 12.82 | 2.57 | 3.10 | 985 | 48.18 |
| MySql | Scarlet EF Core | 7.13 | 6.35 | 12.63 | 2.66 | 3.51 | 961 | 51.90 |
| MySql | Orleans ADO.NET | 7.29 | 6.69 | 11.16 | 2.47 | 3.02 | 806 | 58.12 |
