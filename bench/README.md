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

## Grain directory benchmarks

`Scarlet.Orleans.GrainDirectory.Benchmarks` compares Scarlet ADO.NET, Scarlet EF Core and Orleans ADO.NET through
`IGrainDirectory`, using named directories in one silo and database per engine. SQLite runs the two Scarlet providers;
PostgreSQL, SQL Server and MySQL also run `Microsoft.Orleans.GrainDirectory.AdoNet` **10.4.0-alpha.1**, the published
grain directory package for Orleans 10.4. The runtime remains 10.4.0. Its schema scripts come from Orleans v10.4.0;
the vendored scripts' README documents MySQL client delimiter corrections.

After 100 warm-up register/lookup/unregister cycles, each provider runs 1,000 new registrations, 1,000 registrations
of different activations for existing grains (the original activation must win), 1,000 lookup hits, 1,000 misses and
1,000 unregistrations. Unregistration is verified with untimed lookups. Silo cleanup measures one call removing
another 1,000 registrations and verifies that a registration on a different silo survives.

Then 32 independently scheduled workers register, look up and unregister their own grain for 10 seconds. Throughput
is **successful cycles/s**, with three directory calls per cycle; concurrent p99 measures the complete successful
cycle. Errors are counted separately, with the first error recorded per provider, and the benchmark adds no retries.
Sequential operation failures stop the run. Registration and lookup results are checked against the expected grain,
activation and silo. These measurements cover the common directory contract, rather than conditional replacement.

```sh
dotnet run -c Release --project bench/Scarlet.Orleans.GrainDirectory.Benchmarks -- grain-directory-results.md
```

The same `SCARLET_TEST_POSTGRESQL`, `SCARLET_TEST_SQLSERVER` and `SCARLET_TEST_MYSQL` administrative connection strings
enable the server engines. Each invocation creates and drops its own databases. To reproduce the full comparison,
set all three variables and run the command twice consecutively.

`--soak MINUTES` runs the two Scarlet providers on SQLite and every configured server engine, with eight workers per
directory and 250 grains per worker: 40% registrations (including existing entries), 40% lookups and 20%
unregistrations (including absent entries). It samples throughput, p99 latency, errors, connections and process memory
every 30 seconds and at completion, like the reminder soak. Expected duplicate-key EF command logs are disabled
in both modes, so existing registrations do not flood the benchmark output; unexpected failures remain visible in
the exception or error report.

```sh
dotnet run -c Release --project bench/Scarlet.Orleans.GrainDirectory.Benchmarks -- --soak 30 grain-directory-soak.md
```

## Reading the results

The workloads measure the provider and the round trip to the database, which dominates. Run client and database the way
you deploy them before drawing conclusions about absolute numbers; compare providers within one run. Recorded runs are
in `results/`.

- [Persistence: local run, 2026-10-24](results/2026-10-24-local.md)
- [Reminders: two local runs across all four engines, 2026-10-26](results/2026-10-26-reminders-local.md)
- [Reminders: 36-second SQLite soak smoke run, 2026-10-26](results/2026-10-26-reminders-soak-smoke.md)
- [Grain directory: two local runs across all four engines, 2026-10-28](results/2026-10-28-grain-directory-local.md)
- [Grain directory: 36-second soak smoke run across all four engines, 2026-10-28](results/2026-10-28-grain-directory-soak-smoke.md)
