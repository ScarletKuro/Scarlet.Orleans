using Orleans.Persistence.TestKit;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// The Orleans persistence TestKit (vendored under <c>test/External</c>), run against each flavour, engine and clear mode.
/// </summary>
public abstract class TestKitTests(TestKitFixture fixture) : GrainStorageTestRunner(fixture.TestStorage)
{
    [Fact]
    public Task GeneratedOperationSequences_MatchTheModel() =>
        new GrainStorageModelBasedTestRunner(fixture.TestStorage, "Scarlet").RunGeneratedConformanceTests(TestContext.Current.CancellationToken);

    [Fact]
    public override Task PersistenceStorage_WriteReadIdCyrillic() => base.PersistenceStorage_WriteReadIdCyrillic();

    [Fact]
    public override Task PersistenceStorage_WriteDuplicateFailsWithInconsistentStateException() => base.PersistenceStorage_WriteDuplicateFailsWithInconsistentStateException();

    [Fact]
    public override Task PersistenceStorage_WriteInconsistentFailsWithInconsistentStateException() => base.PersistenceStorage_WriteInconsistentFailsWithInconsistentStateException();

    [Fact]
    public override Task PersistenceStorage_WriteReadWriteReadStatesInParallel() => base.PersistenceStorage_WriteReadWriteReadStatesInParallel();

    [Fact]
    public override Task PersistenceStorage_ReadNonExistentState() => base.PersistenceStorage_ReadNonExistentState();

    [Fact]
    public override Task PersistenceStorage_WriteClearWrite() => base.PersistenceStorage_WriteClearWrite();

    [Fact]
    public override Task PersistenceStorage_WriteClearRead() => base.PersistenceStorage_WriteClearRead();

    [Fact]
    public override Task PersistenceStorage_WriteRead_StringKey() => base.PersistenceStorage_WriteRead_StringKey();

    [Fact]
    public override Task PersistenceStorage_WriteRead_IntegerKey() => base.PersistenceStorage_WriteRead_IntegerKey();

    [Fact]
    public override Task PersistenceStorage_ETagChangesOnWrite() => base.PersistenceStorage_ETagChangesOnWrite();

    [Fact]
    public override Task PersistenceStorage_ClearBeforeWrite() => base.PersistenceStorage_ClearBeforeWrite();

    [Fact]
    public override Task PersistenceStorage_ClearStateDoesNotNullifyState() => base.PersistenceStorage_ClearStateDoesNotNullifyState();

    [Fact]
    public override Task PersistenceStorage_ClearUpdatesETag() => base.PersistenceStorage_ClearUpdatesETag();

    [Fact]
    public override Task PersistenceStorage_ReadAfterClear() => base.PersistenceStorage_ReadAfterClear();

    [Fact]
    public override Task PersistenceStorage_MultipleClearOperations() => base.PersistenceStorage_MultipleClearOperations();

    [Fact]
    public override Task PersistenceStorage_ReadNonExistentStateHasNonNullState() => base.PersistenceStorage_ReadNonExistentStateHasNonNullState();

    [Fact]
    public override Task PersistenceStorage_StateNamesUseIndependentRecords() => base.PersistenceStorage_StateNamesUseIndependentRecords();

    [Fact]
    public override Task PersistenceStorage_ClearInconsistentFailsWithInconsistentStateException() => base.PersistenceStorage_ClearInconsistentFailsWithInconsistentStateException();

    [Fact]
    public override Task PersistenceStorage_WriteReadClearReadCycle() => base.PersistenceStorage_WriteReadClearReadCycle();

    [Fact]
    public override Task PersistenceStorage_WriteWithSameValuesUpdatesETag() => base.PersistenceStorage_WriteWithSameValuesUpdatesETag();
}
