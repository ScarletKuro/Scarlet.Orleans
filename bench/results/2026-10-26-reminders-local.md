# Local reminder results, 2026-10-26

Two consecutive complete runs on one developer machine: Windows (build 26300), 16 logical processors, .NET 10.0.13,
local Docker containers (PostgreSQL 17.11, SQL Server 2022, MySQL 8.4.13), and a SQLite file in WAL mode. Client and
databases share the machine, so the numbers compare providers within a run; absolute latencies depend on this setup.

The in-box provider is `Microsoft.Orleans.Reminders.AdoNet` 10.4.0 with its default options and its vendored v10.4.0
schema scripts. It runs on all three server engines. SQLite compares the two Scarlet providers using Microsoft.Data.Sqlite.
Each provider runs through `IReminderTable` in its own silo, sharing the same database per engine.

After 100 warm-up upserts, each provider performs 1,000 inserts, updates, point reads, grain reads, quarter-ring range
reads and removals, followed by 32 concurrent writers for 10 seconds. Range reads cover the 1,000 inserted reminders,
each on its own grain. Latencies are in milliseconds; concurrent throughput is upserts per second.

## Run 1

| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Grain read p50 | Range read p50 | Remove p50 | Concurrent upserts/s | Concurrent p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.02 | 0.91 | 4.09 | 0.02 | 0.05 | 0.02 | 0.47 | 0.91 | 927 | 4.00 |
| Sqlite | Scarlet EF Core | 1.77 | 1.30 | 4.39 | 0.32 | 0.46 | 0.31 | 0.63 | 1.10 | 672 | 4.28 |
| PostgreSql | Scarlet ADO.NET | 2.86 | 1.90 | 2.45 | 0.83 | 1.07 | 0.82 | 1.03 | 1.87 | 4557 | 10.32 |
| PostgreSql | Scarlet EF Core | 3.04 | 2.04 | 4.09 | 0.94 | 1.16 | 0.94 | 1.16 | 2.04 | 4416 | 11.75 |
| PostgreSql | Orleans ADO.NET | 2.02 | 2.01 | 5.19 | 0.86 | 1.24 | 0.85 | 1.35 | 1.96 | 4538 | 10.29 |
| SqlServer | Scarlet ADO.NET | 4.34 | 3.35 | 6.71 | 0.83 | 1.01 | 0.81 | 1.42 | 3.36 | 3663 | 20.17 |
| SqlServer | Scarlet EF Core | 4.68 | 3.55 | 6.84 | 1.00 | 1.31 | 1.00 | 1.64 | 3.68 | 3329 | 21.33 |
| SqlServer | Orleans ADO.NET | 3.43 | 3.42 | 6.58 | 0.84 | 1.09 | 0.83 | 1.21 | 3.42 | 3440 | 23.44 |
| MySql | Scarlet ADO.NET | 8.83 | 6.32 | 13.04 | 2.39 | 3.21 | 2.36 | 2.81 | 6.37 | 1435 | 37.70 |
| MySql | Scarlet EF Core | 9.27 | 6.45 | 13.40 | 2.59 | 3.10 | 2.62 | 3.13 | 6.46 | 1431 | 40.51 |
| MySql | Orleans ADO.NET | 6.24 | 6.24 | 10.10 | 2.40 | 3.08 | 2.37 | 2.95 | 6.29 | 1484 | 40.97 |

## Run 2

| Engine | Provider | Insert p50 | Update p50 | Update p99 | Read p50 | Read p99 | Grain read p50 | Range read p50 | Remove p50 | Concurrent upserts/s | Concurrent p99 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 0.99 | 0.88 | 1.09 | 0.02 | 0.04 | 0.02 | 0.43 | 0.91 | 898 | 3.98 |
| Sqlite | Scarlet EF Core | 1.74 | 1.28 | 4.44 | 0.36 | 0.54 | 0.31 | 0.72 | 1.17 | 677 | 4.33 |
| PostgreSql | Scarlet ADO.NET | 2.86 | 1.89 | 5.06 | 0.82 | 1.17 | 0.82 | 1.01 | 1.86 | 4504 | 11.15 |
| PostgreSql | Scarlet EF Core | 3.10 | 2.09 | 5.28 | 0.94 | 1.13 | 0.94 | 1.16 | 2.04 | 4227 | 12.80 |
| PostgreSql | Orleans ADO.NET | 2.09 | 2.08 | 2.57 | 0.89 | 1.22 | 0.84 | 1.36 | 1.99 | 4374 | 11.85 |
| SqlServer | Scarlet ADO.NET | 4.39 | 3.41 | 8.42 | 0.83 | 1.02 | 0.81 | 1.41 | 3.39 | 3688 | 22.84 |
| SqlServer | Scarlet EF Core | 4.68 | 3.57 | 7.04 | 1.00 | 1.25 | 1.00 | 1.62 | 3.53 | 3586 | 17.90 |
| SqlServer | Orleans ADO.NET | 3.45 | 3.39 | 9.87 | 0.81 | 1.01 | 0.81 | 1.17 | 3.47 | 3426 | 20.51 |
| MySql | Scarlet ADO.NET | 8.79 | 6.21 | 9.42 | 2.35 | 2.95 | 2.37 | 2.82 | 6.25 | 1502 | 39.29 |
| MySql | Scarlet EF Core | 9.39 | 6.33 | 14.49 | 2.75 | 7.81 | 2.68 | 3.15 | 6.37 | 1491 | 44.37 |
| MySql | Orleans ADO.NET | 6.17 | 6.17 | 10.20 | 2.39 | 3.05 | 2.38 | 2.94 | 6.25 | 1442 | 65.35 |
