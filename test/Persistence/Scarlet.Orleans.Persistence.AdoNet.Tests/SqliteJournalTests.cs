using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Scarlet.Orleans.Persistence.AdoNet;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// SQLite's journal mode: the write-ahead log that concurrent writers need is enabled by <c>Apply</c> and only reported
/// otherwise, because the mode persists in the database file.
/// </summary>
public sealed class SqliteJournalTests(SqliteDatabaseFixture fixture) : IClassFixture<SqliteDatabaseFixture>
{
    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Apply_SwitchesARollbackJournalToTheWriteAheadLog()
    {
        var database = await RollbackJournalDatabaseAsync();
        var logger = new RecordingLogger();

        await new AdoNetGrainStateStore(database.DataSource, null, SchemaMode.Apply, TimeProvider.System, logger).InitializeAsync(CancellationToken);

        Assert.Equal("wal", await JournalModeAsync(database));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Information && e.Message.Contains("wal", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Validate_LeavesTheJournalAloneAndWarns()
    {
        var database = await RollbackJournalDatabaseAsync();
        await new AdoNetGrainStateStore(database.DataSource, null, SchemaMode.Apply, TimeProvider.System, new RecordingLogger()).InitializeAsync(CancellationToken);
        await SetJournalModeAsync(database, "DELETE");
        var logger = new RecordingLogger();

        await new AdoNetGrainStateStore(database.DataSource, null, SchemaMode.Validate, TimeProvider.System, logger).InitializeAsync(CancellationToken);

        Assert.Equal("delete", await JournalModeAsync(database));
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("PRAGMA journal_mode = WAL", StringComparison.Ordinal));
    }

    private async Task<TestDatabase> RollbackJournalDatabaseAsync()
    {
        var database = await fixture.CreateDatabaseAsync();
        await SetJournalModeAsync(database, "DELETE");
        Assert.Equal("delete", await JournalModeAsync(database));
        return database;
    }

    private static async Task SetJournalModeAsync(TestDatabase database, string mode)
    {
        // Leaving the write-ahead log needs the only connection to the file, so pooled ones are closed first.
        SqliteConnection.ClearAllPools();
        await using var connection = new SqliteConnection(database.ConnectionString);
        await connection.OpenAsync(CancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA journal_mode = {mode}";
        await command.ExecuteNonQueryAsync(CancellationToken);
    }

    private static async Task<string?> JournalModeAsync(TestDatabase database)
    {
        await using var command = database.DataSource.CreateCommand("PRAGMA journal_mode");
        return await command.ExecuteScalarAsync(CancellationToken) as string;
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));
    }
}
