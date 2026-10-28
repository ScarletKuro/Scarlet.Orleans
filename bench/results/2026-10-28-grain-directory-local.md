# Local grain directory results, 2026-10-28

Two consecutive complete runs on one developer machine: Windows (build 26300), 16 logical processors, .NET 10.0.13,
local Docker containers (PostgreSQL 17.11, SQL Server 2022, MySQL 8.4.13), and a SQLite file in WAL mode. Client and
databases share the machine, so the numbers compare providers within a run; absolute latencies depend on this setup.
All sequential workload checks passed, and both runs reported zero concurrent errors.

The Orleans baseline is `Microsoft.Orleans.GrainDirectory.AdoNet` **10.4.0-alpha.1**, the published grain directory
package built from the Orleans v10.4.0 commit, with default options on runtime 10.4.0. It runs on all three server
engines. SQLite compares the two Scarlet providers using Microsoft.Data.Sqlite. Each provider runs through
`IGrainDirectory` directly, using named directories in one silo and database per engine.

Schema scripts are from the same commit as the baseline package (`d114d7522a53b8cce4c6b97d44293e5c7273d766`).
The [vendored scripts' README](../Scarlet.Orleans.GrainDirectory.Benchmarks/InBoxScripts/README.md) documents MySQL
client delimiter corrections; table definitions, stored query text and procedure logic are unchanged.

After 100 warm-up register/lookup/unregister cycles, each provider performs 1,000 new registrations, 1,000
registrations of different activations for existing grains (the original must win), 1,000 lookup hits, 1,000 misses,
and 1,000 unregistrations. Untimed lookups verify unregistration. Silo cleanup times one call removing another 1,000
registrations, then verifies that all were removed and an entry on a different silo survived.

Next, 32 independently scheduled workers register, look up and unregister their own grain for 10 seconds.
**Concurrent throughput is successful cycles per second**, with three directory calls per cycle; concurrent p99 is
latency for the complete successful cycle. Latencies are in milliseconds. No benchmark-level retries are added.
Expected duplicate-key EF command logs are disabled. Results check the common grain, activation and silo fields;
conditional replacement and membership-version persistence are outside this comparison.

Cleanup is a single observation per provider per run. Two short runs over this dataset do not establish production
capacity, large-directory scaling or long-term stability. SQLite concurrent cycle latency includes writer contention.

## Run 1

| Engine | Provider | Register p50 | Register p99 | Existing register p50 | Existing register p99 | Lookup p50 | Lookup p99 | Missing lookup p50 | Unregister p50 | Cleanup 1000 ms | Concurrent cycles/s | Concurrent p99 | Concurrent errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.00 | 4.11 | 0.08 | 0.14 | 0.03 | 0.06 | 0.02 | 0.92 | 8.72 | 395 | 627.88 | 0 |
| Sqlite | Scarlet EF Core | 1.37 | 2.41 | 0.78 | 1.18 | 0.28 | 0.35 | 0.24 | 1.13 | 48.86 | 291 | 1401.66 | 0 |
| PostgreSql | Scarlet ADO.NET | 2.00 | 2.66 | 1.78 | 3.62 | 0.87 | 1.12 | 0.86 | 1.92 | 179.35 | 1362 | 34.27 | 0 |
| PostgreSql | Scarlet EF Core | 2.09 | 2.85 | 2.24 | 2.82 | 1.06 | 1.28 | 1.07 | 2.11 | 91.80 | 1344 | 33.47 | 0 |
| PostgreSql | Orleans ADO.NET | 1.95 | 2.35 | 1.94 | 5.07 | 0.88 | 1.12 | 0.86 | 1.95 | 5.50 | 1440 | 29.21 | 0 |
| SqlServer | Scarlet ADO.NET | 3.44 | 4.57 | 1.88 | 2.20 | 0.82 | 1.01 | 0.82 | 3.34 | 65.29 | 1441 | 51.37 | 0 |
| SqlServer | Scarlet EF Core | 3.54 | 7.24 | 2.31 | 2.90 | 1.02 | 1.27 | 1.00 | 3.58 | 119.39 | 1358 | 41.33 | 0 |
| SqlServer | Orleans ADO.NET | 3.39 | 13.68 | 0.84 | 0.98 | 0.82 | 1.00 | 0.80 | 3.39 | 33.14 | 1459 | 44.87 | 0 |
| MySql | Scarlet ADO.NET | 6.09 | 14.39 | 3.99 | 4.69 | 2.06 | 2.44 | 1.99 | 6.30 | 29.11 | 550 | 251.32 | 0 |
| MySql | Scarlet EF Core | 6.16 | 11.35 | 4.34 | 5.10 | 2.09 | 2.49 | 2.08 | 6.11 | 63.07 | 637 | 76.65 | 0 |
| MySql | Orleans ADO.NET | 6.08 | 10.52 | 2.08 | 2.41 | 2.13 | 2.85 | 2.08 | 6.42 | 37.25 | 682 | 77.72 | 0 |

## Run 2

| Engine | Provider | Register p50 | Register p99 | Existing register p50 | Existing register p99 | Lookup p50 | Lookup p99 | Missing lookup p50 | Unregister p50 | Cleanup 1000 ms | Concurrent cycles/s | Concurrent p99 | Concurrent errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Sqlite | Scarlet ADO.NET | 1.01 | 4.15 | 0.08 | 0.13 | 0.04 | 0.07 | 0.02 | 0.95 | 9.03 | 431 | 196.21 | 0 |
| Sqlite | Scarlet EF Core | 1.31 | 2.50 | 0.63 | 0.99 | 0.20 | 0.30 | 0.18 | 1.03 | 47.01 | 304 | 2190.45 | 0 |
| PostgreSql | Scarlet ADO.NET | 1.89 | 3.00 | 1.59 | 1.82 | 0.80 | 1.00 | 0.81 | 1.86 | 187.05 | 1385 | 30.72 | 0 |
| PostgreSql | Scarlet EF Core | 1.99 | 2.29 | 2.08 | 2.46 | 0.96 | 1.16 | 0.97 | 2.01 | 94.25 | 1261 | 34.92 | 0 |
| PostgreSql | Orleans ADO.NET | 1.96 | 5.05 | 1.95 | 2.29 | 0.87 | 1.04 | 0.87 | 1.90 | 5.08 | 1438 | 28.61 | 0 |
| SqlServer | Scarlet ADO.NET | 3.38 | 4.32 | 1.81 | 2.01 | 0.81 | 0.96 | 0.79 | 3.30 | 73.88 | 1104 | 53.74 | 0 |
| SqlServer | Scarlet EF Core | 3.59 | 4.76 | 2.24 | 2.74 | 0.99 | 1.20 | 0.98 | 3.50 | 215.34 | 1491 | 34.15 | 0 |
| SqlServer | Orleans ADO.NET | 3.37 | 6.77 | 0.83 | 0.99 | 0.82 | 0.97 | 0.81 | 3.32 | 20.64 | 1626 | 36.22 | 0 |
| MySql | Scarlet ADO.NET | 5.89 | 12.13 | 3.91 | 4.48 | 1.95 | 2.42 | 1.97 | 5.74 | 27.15 | 666 | 74.56 | 0 |
| MySql | Scarlet EF Core | 6.19 | 11.83 | 4.38 | 5.30 | 2.17 | 2.67 | 2.15 | 6.14 | 64.66 | 623 | 74.81 | 0 |
| MySql | Orleans ADO.NET | 5.86 | 10.99 | 2.02 | 2.42 | 1.96 | 2.28 | 2.05 | 6.28 | 31.95 | 627 | 98.40 | 0 |
