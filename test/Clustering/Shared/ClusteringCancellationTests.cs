using System.Net;
using Orleans;
using Orleans.Runtime;

namespace Scarlet.Orleans.Clustering.Tests;

/// <summary>
/// Cancellation of membership writes: before they start, before they commit, and after they committed, as when a token
/// fires while the server is answering.
/// </summary>
/// <remarks>
/// A canceled membership write surfaces the cancellation whether or not it committed; Orleans reads the table again
/// before its next write, and finds the table as one version or the other, never in between.
/// </remarks>
public abstract class ClusteringCancellationTests(DatabaseFixture fixture)
{
    private static readonly DateTime Start = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    private readonly string clusterId = $"cluster-{Guid.NewGuid():N}";

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Operations_CanceledBeforeTheyStart_DoNothing()
    {
        var (table, _) = await CreateTableAsync();
        await table.InsertRowAsync(Entry(1, SiloStatus.Dead), (await table.ReadAllAsync(CancellationToken)).Version.Next(), CancellationToken);
        var before = await table.ReadAllAsync(CancellationToken);
        var (entry, etag) = before.Members[0];
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.InsertRowAsync(Entry(2, SiloStatus.Joining), before.Version.Next(), canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.UpdateRowAsync(entry, etag, before.Version.Next(), canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.UpdateIAmAliveAsync(Entry(1, SiloStatus.Dead, Start.AddHours(1)), canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.CleanupDefunctSiloEntriesAsync(Start.AddDays(1), canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.DeleteMembershipTableEntriesAsync(clusterId, canceled.Token));

        var after = await table.ReadAllAsync(CancellationToken);
        Assert.Equal(before.Version, after.Version);
        var (stored, storedETag) = Assert.Single(after.Members);
        Assert.Equal(etag, storedETag);
        Assert.Equal(Start, stored.IAmAliveTime);
    }

    [Fact]
    public async Task Write_CanceledBeforeItCommitted_IsCanceledAndChangesNothing()
    {
        var (table, store) = await CreateTableAsync();
        var before = await table.ReadAllAsync(CancellationToken);

        store.CancelBeforeCommit = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.InsertRowAsync(Entry(1, SiloStatus.Joining), before.Version.Next(), CancellationToken));
        store.CancelBeforeCommit = false;

        var after = await table.ReadAllAsync(CancellationToken);
        Assert.Equal(before.Version, after.Version);
        Assert.Empty(after.Members);
    }

    [Fact]
    public async Task Write_CanceledAfterItCommitted_IsCanceled_AndTheNextReadFindsIt()
    {
        var (table, store) = await CreateTableAsync();
        var before = await table.ReadAllAsync(CancellationToken);
        var entry = Entry(1, SiloStatus.Joining);

        store.CommitThenCancel = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.InsertRowAsync(entry, before.Version.Next(), CancellationToken));
        store.CommitThenCancel = false;

        var after = await table.ReadAllAsync(CancellationToken);
        Assert.Equal(before.Version.Next().Version, after.Version.Version);
        var (stored, etag) = Assert.Single(after.Members);
        Assert.Equal(entry.SiloAddress, stored.SiloAddress);
        Assert.True(await table.UpdateRowAsync(Entry(1, SiloStatus.Active), etag, after.Version.Next(), CancellationToken));
    }

    private async Task<(RelationalMembershipTable Table, CancelingStore Store)> CreateTableAsync()
    {
        var store = new CancelingStore(FlavourMembership.CreateStore(fixture.Database, TestSchemaMode.Apply));
        var table = FlavourMembership.CreateTable(store, clusterId);
        await table.InitializeMembershipTableAsync(tryInitTableVersion: true, CancellationToken);
        return (table, store);
    }

    private static MembershipEntry Entry(int silo, SiloStatus status, DateTime? iAmAlive = null) => new()
    {
        SiloAddress = SiloAddress.New(IPAddress.Loopback, 11000 + silo, 1000 + silo),
        Status = status,
        ProxyPort = 30000,
        HostName = "host",
        SiloName = $"silo-{silo}",
        RoleName = "role",
        StartTime = Start,
        IAmAliveTime = iAmAlive ?? Start,
    };

    /// <summary>
    /// Fails versioned writes as a driver does when the token fires while the transaction runs: after the server
    /// committed it, or before the server ran it.
    /// </summary>
    private sealed class CancelingStore(IMembershipStore inner) : IMembershipStore
    {
        public bool CommitThenCancel { get; set; }

        public bool CancelBeforeCommit { get; set; }

        public Task InitializeAsync(CancellationToken cancellationToken) => inner.InitializeAsync(cancellationToken);

        public Task<byte[]?> TryCreateVersionAsync(ClusterScope scope, byte[] etag, CancellationToken cancellationToken) =>
            inner.TryCreateVersionAsync(scope, etag, cancellationToken);

        public Task<MembershipSnapshot> ReadAsync(ClusterScope scope, byte[]? memberKeyHash, CancellationToken cancellationToken) =>
            inner.ReadAsync(scope, memberKeyHash, cancellationToken);

        public async Task<MemberInsertResult> TryInsertAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] etag, CancellationToken cancellationToken)
        {
            ThrowIf(CancelBeforeCommit);
            var result = await inner.TryInsertAsync(scope, version, row, etag, cancellationToken);
            ThrowIf(CommitThenCancel && result.Inserted);
            return result;
        }

        public async Task<bool> TryUpdateAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] expectedETag, byte[] etag, CancellationToken cancellationToken)
        {
            ThrowIf(CancelBeforeCommit);
            var updated = await inner.TryUpdateAsync(scope, version, row, expectedETag, etag, cancellationToken);
            ThrowIf(CommitThenCancel && updated);
            return updated;
        }

        public Task UpdateIAmAliveAsync(ClusterScope scope, byte[] memberKeyHash, byte[] memberIdentity, long iAmAliveTicks, CancellationToken cancellationToken) =>
            inner.UpdateIAmAliveAsync(scope, memberKeyHash, memberIdentity, iAmAliveTicks, cancellationToken);

        public Task<(int Deleted, byte[]? VersionIdentity)> DeleteDefunctAsync(ClusterScope scope, long beforeTicks, CancellationToken cancellationToken) =>
            inner.DeleteDefunctAsync(scope, beforeTicks, cancellationToken);

        public Task<byte[]?> DeleteClusterAsync(ClusterScope scope, CancellationToken cancellationToken) => inner.DeleteClusterAsync(scope, cancellationToken);

        public Task<IReadOnlyList<StoredGateway>> ReadGatewaysAsync(ClusterScope scope, CancellationToken cancellationToken) =>
            inner.ReadGatewaysAsync(scope, cancellationToken);

        private static void ThrowIf(bool condition)
        {
            if (condition)
            {
                throw new OperationCanceledException();
            }
        }
    }
}
