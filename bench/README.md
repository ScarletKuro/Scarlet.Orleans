# Benchmarks

`Scarlet.Orleans.Persistence.Benchmarks` compares grain storage providers through `IGrainStorage`, inside one silo per
engine, on the same database, with the default Orleans serializer and a 1 KiB state:

- **Scarlet ADO.NET** and **Scarlet EF Core**, this repository's two flavours;
- **Orleans ADO.NET**, the in-box `Microsoft.Orleans.Persistence.AdoNet`, set up from its own scripts (`InBoxScripts`,
  vendored from Orleans v10.4.0), on PostgreSQL, SQL Server and MySQL.

Each provider runs 1,000 first writes (inserts), 1,000 updates of one grain and 1,000 reads, sequentially, after a
warm-up, and then 32 concurrent writers updating their own grains for 10 seconds.

## Running

SQLite always runs. Server engines run when the same administrative connection strings as the tests are set
(`SCARLET_TEST_POSTGRESQL`, `SCARLET_TEST_SQLSERVER`, `SCARLET_TEST_MYSQL`); each run creates and drops its own database.

```sh
dotnet run -c Release --project bench/Scarlet.Orleans.Persistence.Benchmarks -- results.md
```

The table is printed and written to the given file.

## Reminder benchmarks

`Scarlet.Orleans.Reminders.Benchmarks` compares the same three provider flavours through `IReminderTable`.
Each provider has its own silo, sharing the benchmark database. SQLite compares the two Scarlet providers;
PostgreSQL, SQL Server and MySQL also compare `Microsoft.Orleans.Reminders.AdoNet`, using its vendored v10.4.0
schema scripts. The same `SCARLET_TEST_*` administrative connection strings enable server engines.

After 100 warm-up upserts, each provider performs 1,000 inserts, updates, point reads, grain reads, quarter-ring
range reads and removals, then runs 32 concurrent writers for 10 seconds. Range reads run over the 1,000 inserted
reminders, each on its own grain. Removals use the current ETag and fail the run if rejected. The report includes
median latency for each operation, p99 update/read/concurrent latency and concurrent upserts per second.
Reminder start times are seven days ahead so delivery does not interfere with the table measurements.

```sh
dotnet run -c Release --project bench/Scarlet.Orleans.Reminders.Benchmarks -- reminder-results.md
```

Both benchmark projects also support `--soak MINUTES`. The reminder soak drives the two Scarlet providers with
eight workers per table over 2,000 grains: 60% upserts, 30% point reads, 10% removals (already absent rows are
no-ops). It samples throughput, p99 latency, errors, database connections, process memory, managed heap and Gen 2
collections every 30 seconds and at completion. SQLite always runs for reminders, including the soak; connection
counts are reported as zero for SQLite. Persistence soak runs configured server engines only.

```sh
dotnet run -c Release --project bench/Scarlet.Orleans.Reminders.Benchmarks -- --soak 30 reminder-soak.md
dotnet run -c Release --project bench/Scarlet.Orleans.Persistence.Benchmarks -- --soak 30 persistence-soak.md
```

## Reading the results

The workloads measure the provider and the round trip to the database, which dominates. Run client and database the way
you deploy them before drawing conclusions about absolute numbers; compare providers within one run. Recorded runs are
in `results/`.

- [Persistence: local run, 2026-10-24](results/2026-10-24-local.md)
- [Reminders: two local runs across all four engines, 2026-10-26](results/2026-10-26-reminders-local.md)
- [Reminders: 36-second SQLite soak smoke run, 2026-10-26](results/2026-10-26-reminders-soak-smoke.md)
