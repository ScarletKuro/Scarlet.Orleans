using System.Data;
using System.Globalization;
using Orleans.Clustering.TestKit;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Clustering.Tests;

/// <summary>
/// Orleans' membership table conformance suite: its 27 direct scenarios and its model-based suite, against one database
/// per engine. Each scenario owns fresh cluster IDs, so they share the database.
/// </summary>
public abstract class MembershipTableTestKitTests(DatabaseFixture fixture)
{
    private const int Seed = 17;

    private TestDatabase Database => fixture.Database;

    [Fact]
    public Task InsertRow_CurrentTableVersion_CommitsExactlyOneVersion() => RunAsync((r, ct) => r.InsertRow_CurrentTableVersion_CommitsExactlyOneVersion(ct));

    [Fact]
    public Task UpdateRow_CurrentTokens_CommitsExactlyOneVersion() => RunAsync((r, ct) => r.UpdateRow_CurrentTokens_CommitsExactlyOneVersion(ct));

    [Fact]
    public Task Reads_SameVersion_PreservesRetainedCanonicalFields() => RunAsync((r, ct) => r.Reads_SameVersion_PreservesRetainedCanonicalFields(ct));

    [Fact]
    public Task Reads_MaySkipCommittedVersions_WithoutSkippingHistoryValidation() => RunAsync((r, ct) => r.Reads_MaySkipCommittedVersions_WithoutSkippingHistoryValidation(ct));

    [Fact]
    public Task Lifecycle_DeadRemainsTerminalAfterCompaction_SuccessorUsesNewGeneration() => RunAsync((r, ct) => r.Lifecycle_DeadRemainsTerminalAfterCompaction_SuccessorUsesNewGeneration(ct));

    [Fact]
    public Task UpdateIAmAlive_OwnerWrites_PreserveCanonicalFieldsAndTableVersion() => RunAsync((r, ct) => r.UpdateIAmAlive_OwnerWrites_PreserveCanonicalFieldsAndTableVersion(ct));

    [Fact]
    public Task UpdateRow_TokensCapturedBeforeHeartbeat_CommitStatusChange() => RunAsync((r, ct) => r.UpdateRow_TokensCapturedBeforeHeartbeat_CommitStatusChange(ct));

    [Fact]
    public Task UpdateRow_TokensCapturedBeforeHeartbeat_CommitVoteChange() => RunAsync((r, ct) => r.UpdateRow_TokensCapturedBeforeHeartbeat_CommitVoteChange(ct));

    [Fact]
    public Task Handles_IndependentlyConstructed_ShareCommittedBackingState() => RunAsync((r, ct) => r.Handles_IndependentlyConstructed_ShareCommittedBackingState(ct));

    [Fact]
    public Task InsertRow_StaleTableToken_ReturnsFalseWithoutSideEffects() => RunAsync((r, ct) => r.InsertRow_StaleTableToken_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task UpdateRow_StaleTableTokenWithCurrentRowToken_ReturnsFalseWithoutSideEffects() => RunAsync((r, ct) => r.UpdateRow_StaleTableTokenWithCurrentRowToken_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task UpdateRow_StaleSnapshotAfterSameRowCommit_ReturnsFalseWithoutSideEffects() => RunAsync((r, ct) => r.UpdateRow_StaleSnapshotAfterSameRowCommit_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task InsertRow_DuplicateIdentity_ReturnsFalseWithoutSideEffects() => RunAsync((r, ct) => r.InsertRow_DuplicateIdentity_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task UpdateRow_MissingIdentityWithRealToken_ReturnsFalseWithoutSideEffects() => RunAsync((r, ct) => r.UpdateRow_MissingIdentityWithRealToken_ReturnsFalseWithoutSideEffects(ct));

    [Fact]
    public Task ReadRow_AndReadAll_AgreeForPresentAndAbsentIdentities() => RunAsync((r, ct) => r.ReadRow_AndReadAll_AgreeForPresentAndAbsentIdentities(ct));

    [Fact]
    public Task Reads_RetainedObjectsRemainUnchangedAfterLaterWrites() => RunAsync((r, ct) => r.Reads_RetainedObjectsRemainUnchangedAfterLaterWrites(ct));

    [Fact]
    public Task InsertRow_MutatingInputAndSuspectList_DoesNotMutateStoredState() => RunAsync((r, ct) => r.InsertRow_MutatingInputAndSuspectList_DoesNotMutateStoredState(ct));

    [Fact]
    public Task UpdateRow_MutatingInputAndSuspectList_DoesNotMutateStoredState() => RunAsync((r, ct) => r.UpdateRow_MutatingInputAndSuspectList_DoesNotMutateStoredState(ct));

    [Fact]
    public Task Reads_MutatingReturnedEntryAndSuspectList_DoesNotMutateStoredState() => RunAsync((r, ct) => r.Reads_MutatingReturnedEntryAndSuspectList_DoesNotMutateStoredState(ct));

    [Fact]
    public Task ConcurrentCrossRowUpdates_SharedTableVersion_HaveExactlyOneWinner() => RunAsync((r, ct) => r.ConcurrentCrossRowUpdates_SharedTableVersion_HaveExactlyOneWinner(ct));

    [Fact]
    public Task ConcurrentReadAll_ReturnsOnlyAtomicCommittedViews() => RunAsync((r, ct) => r.ConcurrentReadAll_ReturnsOnlyAtomicCommittedViews(ct));

    [Fact]
    public Task ConcurrentReadRow_ReturnsOnlyAtomicCommittedViews() => RunAsync((r, ct) => r.ConcurrentReadRow_ReturnsOnlyAtomicCommittedViews(ct));

    [Fact]
    public Task InitializeMembershipTable_RepeatedWithData_PreservesCommittedState() => RunAsync((r, ct) => r.InitializeMembershipTable_RepeatedWithData_PreservesCommittedState(ct));

    [Fact]
    public Task Clusters_SharedBackendWithOverlappingSiloAddresses_AreIsolated() => RunAsync((r, ct) => r.Clusters_SharedBackendWithOverlappingSiloAddresses_AreIsolated(ct));

    [Fact]
    public Task CleanupDefunctSiloEntries_RemovesOnlyStrictlyOldDeadRows() => RunAsync((r, ct) => r.CleanupDefunctSiloEntries_RemovesOnlyStrictlyOldDeadRows(ct));

    [Fact]
    public Task DeleteMembershipTableEntries_DeletesOwnClusterAndPreservesOtherCluster() => RunAsync((r, ct) => r.DeleteMembershipTableEntries_DeletesOwnClusterAndPreservesOtherCluster(ct));

    [Fact]
    public Task DeleteMembershipTableEntries_DifferentClusterId_NeverDeletesConfiguredCluster() => RunAsync((r, ct) => r.DeleteMembershipTableEntries_DifferentClusterId_NeverDeletesConfiguredCluster(ct));

    [Fact]
    public async Task MembershipTable_ModelBased_GeneratedConformance()
    {
        await ApplySchemaAsync();
        await new MembershipTableModelBasedTestRunner(
            CreateFixture,
            new MembershipTableModelBasedConformanceOptions { ProviderName = GetType().Name, Seed = Seed, MaxDepth = 3, MaxSequenceLength = 3 },
            output: Output).RunGeneratedConformanceTests(TestContext.Current.CancellationToken);
    }

    private async Task RunAsync(Func<MembershipTableTestRunner, CancellationToken, Task> scenario)
    {
        await ApplySchemaAsync();
        await CreateFixture().RunAsync(
            (testFixture, ct) => scenario(new MembershipTableTestRunner(testFixture, Seed, output: Output), ct),
            TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Every handle is a separately constructed table over the same database. The schema is applied once per test, so
    /// handles skip the check.
    /// </summary>
    private MembershipTableTestFixture CreateFixture() =>
        new(
            GetType().Name,
            (_, clusterId, _) => ValueTask.FromResult(new MembershipTableTestHandle(FlavourMembership.CreateTable(Database, clusterId, TestSchemaMode.None))),
            IsDeletedAsync,
            serviceId: "scarlet-clustering-testkit");

    private Task ApplySchemaAsync() =>
        FlavourMembership.CreateTable(Database, "schema", TestSchemaMode.Apply).InitializeMembershipTableAsync(tryInitTableVersion: false, TestContext.Current.CancellationToken);

    /// <summary>
    /// Reads the cluster's rows directly, so a deletion that left any version or member row behind is seen.
    /// </summary>
    private async ValueTask<bool> IsDeletedAsync(string clusterId, CancellationToken cancellationToken)
    {
        await using var command = Database.DataSource.CreateCommand(
            $"SELECT (SELECT COUNT(*) FROM {ClusteringSchema.VersionTableName} WHERE cluster_hash = @cluster_hash) " +
            $"+ (SELECT COUNT(*) FROM {ClusteringSchema.MemberTableName} WHERE cluster_hash = @cluster_hash)");
        command.AddParameter("cluster_hash", new ClusterScope(clusterId).Hash, DbType.Binary);
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture) == 0;
    }

    private static void Output(string message) => TestContext.Current.TestOutputHelper?.WriteLine(message);
}
