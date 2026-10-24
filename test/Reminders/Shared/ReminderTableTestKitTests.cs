using Orleans;
using Orleans.Reminders.TestKit;
using Orleans.Runtime;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// The Orleans reminders TestKit (vendored under <c>test/External</c>), run against each flavour and engine. Every test
/// gets its own service ID, so tests share a database without seeing each other's reminders.
/// </summary>
public abstract class ReminderTableTestKitTests(DatabaseFixture fixture)
    : ReminderTableTestRunner(new RequiredEngineTable(fixture, $"service-{Guid.NewGuid():N}"), "Scarlet"), IAsyncLifetime
{
    public async ValueTask InitializeAsync()
    {
        if (fixture.IsAvailable)
        {
            await ReminderTable.StartAsync(TestContext.Current.CancellationToken);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (fixture.IsAvailable)
        {
            await ReminderTable.TestOnlyClearTable();
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public Task GeneratedOperationSequences_MatchTheModel() =>
        new ReminderTableModelBasedTestRunner(ReminderTable, "Scarlet").RunGeneratedConformanceTests(TestContext.Current.CancellationToken);

    [Fact]
    public Task ReminderTable_ReadRows_FullRange_Returns500Reminders() => ReminderTable_ReadRows_FullRange_ReturnsExactRequestedCardinality(500);

    [Fact]
    public override Task ReminderTable_StartAsync_IsIdempotent() => base.ReminderTable_StartAsync_IsIdempotent();

    [Fact]
    public override Task ReminderTable_StopAsync_ThenRestart_ResumesService() => base.ReminderTable_StopAsync_ThenRestart_ResumesService();

    [Fact]
    public override Task ReminderTable_UpsertRow_ReturnsNewNonEmptyETag() => base.ReminderTable_UpsertRow_ReturnsNewNonEmptyETag();

    [Fact]
    public override Task ReminderTable_UpsertRow_PersistsScheduleForPointRead() => base.ReminderTable_UpsertRow_PersistsScheduleForPointRead();

    [Fact]
    public override Task ReminderTable_ReadRow_MissingReminder_ReturnsNull() => base.ReminderTable_ReadRow_MissingReminder_ReturnsNull();

    [Fact]
    public override Task ReminderTable_ReadRows_ForGrain_ReturnsOnlyThatGrainsReminders() => base.ReminderTable_ReadRows_ForGrain_ReturnsOnlyThatGrainsReminders();

    [Fact]
    public override Task ReminderTable_ReadRows_ForUnknownGrain_ReturnsEmpty() => base.ReminderTable_ReadRows_ForUnknownGrain_ReturnsEmpty();

    [Fact]
    public override Task ReminderTable_Identity_IsGrainIdAndReminderName() => base.ReminderTable_Identity_IsGrainIdAndReminderName();

    [Fact]
    public override Task ReminderTable_Identity_WithSpecialCharacters_RoundTrips() => base.ReminderTable_Identity_WithSpecialCharacters_RoundTrips();

    [Fact]
    public override Task ReminderTable_UpsertRow_ReplacesETagOnEachWrite() => base.ReminderTable_UpsertRow_ReplacesETagOnEachWrite();

    [Fact]
    public override Task ReminderTable_RemoveRow_WithCurrentETag_RemovesRow() => base.ReminderTable_RemoveRow_WithCurrentETag_RemovesRow();

    [Fact]
    public override Task ReminderTable_RemoveRow_WithStaleETag_FailsAndRetainsRow() => base.ReminderTable_RemoveRow_WithStaleETag_FailsAndRetainsRow();

    [Fact]
    public override Task ReminderTable_RemoveRow_WithUnknownReminderName_ReturnsFalse() => base.ReminderTable_RemoveRow_WithUnknownReminderName_ReturnsFalse();

    [Fact]
    public override Task ReminderTable_RemoveRow_Repeated_ReturnsFalseAfterFirstSuccess() => base.ReminderTable_RemoveRow_Repeated_ReturnsFalseAfterFirstSuccess();

    [Fact]
    public override Task ReminderTable_UpsertRow_UpdatesStartAtAndPeriod() => base.ReminderTable_UpsertRow_UpdatesStartAtAndPeriod();

    [Fact]
    public override Task ReminderTable_UpsertRow_MovesReminderBetweenLoadingWindows() => base.ReminderTable_UpsertRow_MovesReminderBetweenLoadingWindows();

    [Fact]
    public override Task ReminderTable_ReadRows_FullRange_ReturnsAllReminders() => base.ReminderTable_ReadRows_FullRange_ReturnsAllReminders();

    [Fact]
    public override Task ReminderTable_ReadRows_Range_ExcludesBeginAndIncludesEnd() => base.ReminderTable_ReadRows_Range_ExcludesBeginAndIncludesEnd();

    [Fact]
    public override Task ReminderTable_ReadRows_WrapAroundRange_ReturnsWrappedSegment() => base.ReminderTable_ReadRows_WrapAroundRange_ReturnsWrappedSegment();

    [Fact]
    public override Task ReminderTable_ReadRows_OutsideRange_DoesNotDeleteReminder() => base.ReminderTable_ReadRows_OutsideRange_DoesNotDeleteReminder();

    [Fact]
    public override Task ReminderTable_ReadRows_AfterRemoval_OmitsRemovedReminder() => base.ReminderTable_ReadRows_AfterRemoval_OmitsRemovedReminder();

    [Fact]
    public override Task ReminderTable_ReadRow_AfterRemoval_ReturnsNull() => base.ReminderTable_ReadRow_AfterRemoval_ReturnsNull();

    [Fact]
    public override Task ReminderTable_ConcurrentUpserts_ProduceDistinctETags() => base.ReminderTable_ConcurrentUpserts_ProduceDistinctETags();

    [Fact]
    public override Task ReminderTable_ParallelUpserts_AcrossGrains_RemainIsolated() => base.ReminderTable_ParallelUpserts_AcrossGrains_RemainIsolated();

    [Fact]
    public override Task ReminderTable_TestOnlyClearTable_RemovesAllReminders() => base.ReminderTable_TestOnlyClearTable_RemovesAllReminders();

    /// <summary>
    /// The table under test, created on first use, which skips the calling test when the engine is not configured.
    /// </summary>
    private sealed class RequiredEngineTable(DatabaseFixture fixture, string serviceId) : IReminderTable
    {
        private IReminderTable? table;

        private IReminderTable Table
        {
            get
            {
                fixture.Require();
                return table ??= FlavourTable.Create(fixture.Database, serviceId, TestSchemaMode.Apply);
            }
        }

        public Task StartAsync(CancellationToken cancellationToken = default) => Table.StartAsync(cancellationToken);

        public Task StopAsync(CancellationToken cancellationToken = default) => Table.StopAsync(cancellationToken);

        public Task<ReminderTableData> ReadRows(GrainId grainId) => Table.ReadRows(grainId);

        public Task<ReminderTableData> ReadRows(GrainId grainId, CancellationToken cancellationToken) => Table.ReadRows(grainId, cancellationToken);

        public Task<ReminderTableData> ReadRows(uint begin, uint end) => Table.ReadRows(begin, end);

        public Task<ReminderTableData> ReadRows(uint begin, uint end, CancellationToken cancellationToken) => Table.ReadRows(begin, end, cancellationToken);

        public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName) => Table.ReadRow(grainId, reminderName);

        public Task<ReminderEntry?> ReadRow(GrainId grainId, string reminderName, CancellationToken cancellationToken) => Table.ReadRow(grainId, reminderName, cancellationToken);

        public Task<string?> UpsertRow(ReminderEntry entry) => Table.UpsertRow(entry);

        public Task<string?> UpsertRow(ReminderEntry entry, CancellationToken cancellationToken) => Table.UpsertRow(entry, cancellationToken);

        public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag) => Table.RemoveRow(grainId, reminderName, eTag);

        public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag, CancellationToken cancellationToken) =>
            Table.RemoveRow(grainId, reminderName, eTag, cancellationToken);

        public Task TestOnlyClearTable() => Table.TestOnlyClearTable();

        public Task TestOnlyClearTable(CancellationToken cancellationToken) => Table.TestOnlyClearTable(cancellationToken);
    }
}
