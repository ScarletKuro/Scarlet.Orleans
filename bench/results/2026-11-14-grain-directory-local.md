# Local grain directory results, 2026-11-14

Two consecutive runs on Windows 11, a Ryzen 7 5800X3D (16 logical processors), .NET 10.0.15, and local Docker databases through `127.0.0.1`. One benchmark process ran at a time. See the [all-suite report](2026-11-14-local.md) for versions, harness corrections, scope, and comparison limits.

Measurements: 2026-11-14 23:08 to 2026-11-14 23:20, Europe/Tallinn. Filenames use the campaign start date.

16 logical processors, .NET 10.0.15, 1000 operations per sequential workload, 32 concurrent workers for 10 s. A concurrent cycle registers, looks up and unregisters one activation. Latencies in milliseconds; cleanup is one call removing 1000 registrations.

## Reading the results

- **Normal directory cycles:** Scarlet ADO.NET is within 6% of the in-box provider on PostgreSQL and within 3% on SQL Server. EF Core is within 5% on PostgreSQL and about 6% behind on SQL Server. On MySQL, Scarlet ADO.NET is 9–15% ahead of the in-box provider; EF Core is 6–7% ahead. Each cycle includes register, lookup, and unregister, so these rates are not individual calls/s.
- **Existing registrations:** throughput does not remove the cost of a losing registration. On SQL Server and MySQL, Scarlet ADO.NET's existing-register p50 is roughly twice the in-box provider's, and EF Core's is about 2.2–2.4 times as high. This workload intentionally keeps the original activation.
- **Bulk cleanup:** removing 1,000 registrations takes 10.76–11.04 ms for PostgreSQL ADO.NET and 50.62–52.27 ms for EF Core, against 5.36–5.69 ms in-box. On SQL Server the corresponding ranges are 63.83–88.36, 105.42–113.19, and 23.25–30.87 ms. MySQL ADO.NET cleanup is competitive at 25.14–29.02 ms; EF Core takes 64.40–66.84 ms. Cleanup therefore needs its own capacity assessment.
- **SQLite:** ADO.NET reaches 395–436 cycles/s versus EF Core's 291–296, but its p99 changes from 1,247.76 ms to 18.74 ms between runs. EF Core p99 is 1,559.33–2,494.54 ms. Throughput alone does not describe this contention tail.
- Both full invocations completed with zero concurrent errors, and their registration, lookup, unregister, and cleanup checks passed.

## Run 1

| Engine | Provider | Register p50 | Register p99 | Existing register p50 | Existing register p99 | Lookup p50 | Lookup p99 | Missing lookup p50 | Unregister p50 | Cleanup 1000 ms | Concurrent cycles/s | Concurrent p99 | Concurrent errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.00 | 4.11 | 0.08 | 0.13 | 0.03 | 0.06 | 0.02 | 0.92 | 9.08 | 436 | 1247.76 | 0 |
| Sqlite | Scarlet EF Core | 1.40 | 4.64 | 0.72 | 1.03 | 0.29 | 0.40 | 0.26 | 1.15 | 50.23 | 291 | 2494.54 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.35 | 5.56 | 2.73 | 3.10 | 1.37 | 1.57 | 1.37 | 2.29 | 10.76 | 676 | 55.83 | 0 |
| PostgreSql | Scarlet EF Core | 2.42 | 5.73 | 3.23 | 4.18 | 1.54 | 2.00 | 1.52 | 2.41 | 50.62 | 668 | 56.77 | 0 |
| PostgreSql | Orleans ADO.NET | 2.37 | 2.68 | 2.35 | 3.16 | 1.43 | 1.72 | 1.41 | 2.31 | 5.36 | 683 | 53.74 | 0 |
| SqlServer | Scarlet ADO.NET | 3.97 | 7.35 | 2.94 | 3.26 | 1.35 | 1.60 | 1.34 | 3.88 | 88.36 | 621 | 77.89 | 0 |
| SqlServer | Scarlet EF Core | 4.14 | 6.04 | 3.38 | 4.03 | 1.55 | 1.89 | 1.53 | 4.07 | 113.19 | 599 | 102.65 | 0 |
| SqlServer | Orleans ADO.NET | 3.93 | 14.32 | 1.42 | 1.77 | 1.37 | 1.58 | 1.38 | 3.90 | 30.87 | 639 | 70.27 | 0 |
| MySql | Scarlet ADO.NET | 6.34 | 17.11 | 5.08 | 7.84 | 2.56 | 3.10 | 2.55 | 6.35 | 25.14 | 337 | 117.12 | 0 |
| MySql | Scarlet EF Core | 6.28 | 11.00 | 5.48 | 6.73 | 2.56 | 3.05 | 2.56 | 6.25 | 64.40 | 315 | 179.44 | 0 |
| MySql | Orleans ADO.NET | 6.18 | 10.61 | 2.49 | 3.23 | 2.40 | 2.79 | 2.41 | 6.26 | 33.28 | 294 | 237.26 | 0 |

## Run 2

| Engine | Provider | Register p50 | Register p99 | Existing register p50 | Existing register p99 | Lookup p50 | Lookup p99 | Missing lookup p50 | Unregister p50 | Cleanup 1000 ms | Concurrent cycles/s | Concurrent p99 | Concurrent errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.95 | 2.31 | 0.08 | 0.12 | 0.04 | 0.06 | 0.02 | 0.97 | 9.05 | 395 | 18.74 | 0 |
| Sqlite | Scarlet EF Core | 1.29 | 4.44 | 0.85 | 1.16 | 0.29 | 0.33 | 0.26 | 1.14 | 44.83 | 296 | 1559.33 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.37 | 5.28 | 2.74 | 3.21 | 1.40 | 1.67 | 1.40 | 2.37 | 11.04 | 645 | 97.69 | 0 |
| PostgreSql | Scarlet EF Core | 2.42 | 5.63 | 3.23 | 4.61 | 1.68 | 7.06 | 1.53 | 2.51 | 52.27 | 652 | 59.29 | 0 |
| PostgreSql | Orleans ADO.NET | 2.45 | 5.66 | 2.41 | 5.53 | 1.47 | 1.82 | 1.47 | 2.37 | 5.69 | 683 | 54.50 | 0 |
| SqlServer | Scarlet ADO.NET | 4.02 | 7.19 | 3.03 | 3.64 | 1.37 | 1.63 | 1.36 | 3.96 | 63.83 | 644 | 79.28 | 0 |
| SqlServer | Scarlet EF Core | 4.20 | 10.49 | 3.37 | 3.87 | 1.54 | 1.91 | 1.54 | 4.10 | 105.42 | 612 | 110.33 | 0 |
| SqlServer | Orleans ADO.NET | 3.92 | 7.24 | 1.40 | 2.30 | 1.40 | 3.15 | 1.37 | 3.94 | 23.25 | 652 | 77.48 | 0 |
| MySql | Scarlet ADO.NET | 6.13 | 12.11 | 4.97 | 5.59 | 2.55 | 3.16 | 2.50 | 6.25 | 29.02 | 329 | 128.39 | 0 |
| MySql | Scarlet EF Core | 6.32 | 14.92 | 5.57 | 7.49 | 2.66 | 3.19 | 2.58 | 6.21 | 66.84 | 320 | 128.67 | 0 |
| MySql | Orleans ADO.NET | 6.20 | 11.06 | 2.48 | 2.95 | 2.43 | 2.89 | 2.43 | 6.20 | 29.02 | 302 | 229.88 | 0 |
