# Scarlet.Orleans.Streaming.EntityFrameworkCore

The Orleans streaming tables of `Scarlet.Orleans.Streaming.AdoNet`, mapped into your own EF Core `DbContext`, so your
migrations create and upgrade them. Requires .NET 10 and EF Core 10.

> **In development**, like `Scarlet.Orleans.Streaming.AdoNet`.

This package runs no queues. Dequeuing needs each engine's own locking read, which EF Core cannot express, so the
queues run through `Scarlet.Orleans.Streaming.AdoNet` on the same database, which checks the schema your migration
recorded.

## Setup

```csharp
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder) => modelBuilder.ApplyOrleansStreamingModel();
}
```

Run `dotnet ef migrations add AddOrleansStreaming`, then record the schema version at the end of the generated `Up`
method:

```csharp
protected override void Up(MigrationBuilder migrationBuilder)
{
    // ... tables EF generated from the model
    migrationBuilder.ApplyOrleansStreamingSchema(1);
}
```

Then add the stream provider with the ADO.NET package, on the same database, keeping its default
`SchemaMode.Validate`:

```csharp
builder.UseOrleans(silo => silo
    .AddScarletAdoNetStreams("StreamProvider", options => options.DataSource = dataSource));
```

The model maps exactly the tables of the ADO.NET package's scripts on SQL Server, PostgreSQL, MySQL, MariaDB and
SQLite, and a test compares them on each engine.
