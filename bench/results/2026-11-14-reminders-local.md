# Local reminder table results, 2026-11-14

Two consecutive runs on Windows 11, a Ryzen 7 5800X3D (16 logical processors), .NET 10.0.15, and local Docker databases through `127.0.0.1`. One benchmark process ran at a time. See the [all-suite report](2026-11-14-local.md) for versions, harness corrections, scope, and comparison limits.

Measurements: 2026-11-14 22:57 to 2026-11-14 23:08, Europe/Tallinn. Filenames use the campaign start date.

16 logical processors, .NET 10.0.15, 32 concurrent writers for 10 s. Latencies in milliseconds. Range reads cover a quarter of the hash ring over 1000 reminders.

## Reading the results

- **Server engines:** concurrent upsert rates are within about 7% of the in-box provider in both runs. PostgreSQL is 1,857–2,007 upserts/s across all providers, SQL Server 1,678–1,828, and MySQL 943–985. These runs support broadly comparable throughput, rather than a universal Scarlet advantage.
- **Sequential operations:** update medians are close within each server engine. First inserts cost more through Scarlet; range reads also depend on the engine and provider. On SQL Server, range-read p50 is 2.26–2.33 ms for Scarlet ADO.NET and 2.48–2.50 ms for EF Core, against 1.89–1.90 ms for the in-box provider.
- **SQLite:** Scarlet ADO.NET sustains 951–955 upserts/s against EF Core's 649–664, a paired advantage of 43–47%. Its point reads are also much cheaper: roughly 0.02 ms versus 0.32 ms.
- Both full invocations completed. The shorter workloads measure latency and throughput; they do not establish long-term resource stability.

## Run 1

| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Grain read p50 | Range read p50 | Remove p50 | Concurrent upserts/s | Concurrent p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.00 | 0.94 | 2.66 | 0.02 | 0.03 | 0.02 | 0.44 | 0.92 | 951 | 4.00 |
| Sqlite | Scarlet EF Core | 1.85 | 1.27 | 1.89 | 0.30 | 0.42 | 0.29 | 0.90 | 1.08 | 664 | 4.30 |
| PostgreSql | Scarlet ADO.NET | 3.73 | 2.29 | 2.60 | 1.40 | 1.81 | 1.38 | 1.93 | 2.27 | 1945 | 23.79 |
| PostgreSql | Scarlet EF Core | 3.98 | 2.43 | 5.54 | 1.48 | 1.78 | 1.52 | 2.08 | 2.50 | 1857 | 24.70 |
| PostgreSql | Orleans ADO.NET | 2.44 | 2.40 | 2.94 | 1.40 | 1.95 | 1.39 | 2.04 | 2.38 | 1975 | 23.35 |
| SqlServer | Scarlet ADO.NET | 5.42 | 3.87 | 10.08 | 1.38 | 1.94 | 1.37 | 2.26 | 3.89 | 1700 | 32.77 |
| SqlServer | Scarlet EF Core | 5.68 | 4.07 | 7.37 | 1.52 | 1.82 | 1.56 | 2.50 | 4.07 | 1731 | 34.17 |
| SqlServer | Orleans ADO.NET | 3.97 | 3.93 | 7.22 | 1.34 | 1.61 | 1.37 | 1.90 | 3.90 | 1828 | 30.21 |
| MySql | Scarlet ADO.NET | 8.57 | 6.18 | 9.97 | 2.38 | 2.99 | 2.42 | 3.33 | 6.15 | 943 | 57.89 |
| MySql | Scarlet EF Core | 9.27 | 6.28 | 9.62 | 2.58 | 3.08 | 2.59 | 3.53 | 6.29 | 972 | 43.88 |
| MySql | Orleans ADO.NET | 6.20 | 6.21 | 9.66 | 2.44 | 2.92 | 2.42 | 3.13 | 6.19 | 971 | 47.50 |

## Run 2

| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Grain read p50 | Range read p50 | Remove p50 | Concurrent upserts/s | Concurrent p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.99 | 0.93 | 2.86 | 0.02 | 0.05 | 0.04 | 0.42 | 0.90 | 955 | 4.01 |
| Sqlite | Scarlet EF Core | 1.94 | 1.29 | 4.52 | 0.32 | 0.52 | 0.31 | 0.77 | 1.09 | 649 | 4.27 |
| PostgreSql | Scarlet ADO.NET | 3.93 | 2.47 | 5.58 | 1.42 | 1.75 | 1.41 | 1.95 | 2.36 | 1931 | 23.40 |
| PostgreSql | Scarlet EF Core | 4.08 | 2.47 | 4.06 | 1.53 | 1.99 | 1.51 | 2.12 | 2.47 | 1954 | 23.44 |
| PostgreSql | Orleans ADO.NET | 2.47 | 2.41 | 5.55 | 1.44 | 1.80 | 1.41 | 2.05 | 2.39 | 2007 | 22.76 |
| SqlServer | Scarlet ADO.NET | 5.38 | 3.92 | 7.22 | 1.45 | 3.48 | 1.38 | 2.33 | 3.96 | 1709 | 36.01 |
| SqlServer | Scarlet EF Core | 5.77 | 4.18 | 7.42 | 1.54 | 2.07 | 1.56 | 2.48 | 4.08 | 1678 | 34.74 |
| SqlServer | Orleans ADO.NET | 3.99 | 3.94 | 7.14 | 1.39 | 1.84 | 1.38 | 1.89 | 3.92 | 1695 | 37.22 |
| MySql | Scarlet ADO.NET | 8.58 | 6.14 | 10.08 | 2.37 | 2.82 | 2.42 | 3.30 | 6.22 | 975 | 51.22 |
| MySql | Scarlet EF Core | 9.08 | 6.28 | 10.31 | 2.57 | 3.24 | 2.57 | 3.48 | 6.29 | 974 | 50.64 |
| MySql | Orleans ADO.NET | 6.20 | 6.22 | 11.61 | 2.41 | 2.83 | 2.40 | 3.12 | 6.20 | 985 | 44.33 |
