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

## Reading the results

The workloads measure the provider and the round trip to the database, which dominates. Run client and database the way
you deploy them before drawing conclusions about absolute numbers; compare providers within one run. Recorded runs are
in `results/`.
