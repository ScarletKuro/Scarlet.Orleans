using Orleans;
using Orleans.Reminders.TestKit;
using Orleans.Runtime;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// The Orleans reminders TestKit (vendored under <c>test/External</c>), run against each flavour and engine. Every test
/// gets its own service ID, so tests share a database without seeing each other's reminders.
/// </summary>
/// <remarks>
/// The table is created from <see cref="DatabaseFixture.Database"/>, which skips the test when the engine is not
/// configured. That happens in the constructor, before any TestKit code runs: the TestKit retries failed operations and
/// would turn the skip into a failure.
/// </remarks>
public abstract class ReminderTableTestKitTests(DatabaseFixture fixture)
    : ReminderTableTestRunner(FlavourTable.Create(fixture.Database, $"service-{Guid.NewGuid():N}", TestSchemaMode.Apply), "Scarlet"), IAsyncLifetime
{
    public async ValueTask InitializeAsync() => await ReminderTable.StartAsync(TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await ReminderTable.TestOnlyClearTable();
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
    public override Task ReminderTable_ReadRows_UnsignedBoundary_UsesUInt32Ordering() => base.ReminderTable_ReadRows_UnsignedBoundary_UsesUInt32Ordering();

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
}
