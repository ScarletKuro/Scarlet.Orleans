using Orleans.Runtime;

namespace Scarlet.Orleans.GrainDirectory.Tests;

/// <summary>
/// Cancellation of registrations whose statement may already have committed, as when a token fires while the server
/// is answering.
/// </summary>
public abstract class GrainDirectoryCancellationTests(DatabaseFixture fixture)
{
    private static readonly SiloAddress Silo = SiloAddress.FromParsableString("10.0.23.12:1000@5678");

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Register_CanceledAfterItCommitted_ReportsTheRegistration(bool replacing)
    {
        var store = new CancelingStore(FlavourDirectory.CreateStore(fixture.Database, TestSchemaMode.Apply));
        var directory = FlavourDirectory.Create(store, "default", $"cluster-{Guid.NewGuid():N}");
        await directory.InitializeAsync(CancellationToken);
        var grainId = GrainId.Create("cancel", "key");
        var previous = replacing ? await directory.Register(Address(grainId), CancellationToken) : null;

        store.CommitThenCancel = true;
        var address = Address(grainId);
        var registered = await directory.Register(address, previous, CancellationToken);
        store.CommitThenCancel = false;

        Assert.Equal(address, registered);
        Assert.Equal(address, await directory.Lookup(grainId, CancellationToken));
    }

    [Fact]
    public async Task Register_CanceledBeforeItCommitted_IsCanceledAndChangesNothing()
    {
        var store = new CancelingStore(FlavourDirectory.CreateStore(fixture.Database, TestSchemaMode.Apply));
        var directory = FlavourDirectory.Create(store, "default", $"cluster-{Guid.NewGuid():N}");
        await directory.InitializeAsync(CancellationToken);
        var grainId = GrainId.Create("cancel", "key");
        var first = await directory.Register(Address(grainId), CancellationToken);

        store.CancelBeforeCommit = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => directory.Register(Address(grainId), first, CancellationToken));
        store.CancelBeforeCommit = false;

        Assert.Equal(first, await directory.Lookup(grainId, CancellationToken));
    }

    [Fact]
    public async Task Operations_CanceledBeforeTheyStart_DoNothing()
    {
        var directory = FlavourDirectory.Create(fixture.Database, "default", $"cluster-{Guid.NewGuid():N}", TestSchemaMode.Apply);
        await directory.InitializeAsync(CancellationToken);
        var grainId = GrainId.Create("cancel", "key");
        var first = await directory.Register(Address(grainId), CancellationToken);
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => directory.Register(Address(grainId), first, canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => directory.Unregister(first!, canceled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => directory.UnregisterSilos([Silo], canceled.Token));

        Assert.Equal(first, await directory.Lookup(grainId, CancellationToken));
    }

    private static GrainAddress Address(GrainId grainId) => new()
    {
        ActivationId = ActivationId.NewId(),
        GrainId = grainId,
        SiloAddress = Silo,
        MembershipVersion = new MembershipVersion(7),
    };

    /// <summary>
    /// Fails writes as a driver does when the token fires while the statement runs: after the server committed it, or
    /// before the server ran it.
    /// </summary>
    private sealed class CancelingStore(IGrainDirectoryStore inner) : IGrainDirectoryStore
    {
        public bool CommitThenCancel { get; set; }

        public bool CancelBeforeCommit { get; set; }

        public Task InitializeAsync(CancellationToken cancellationToken) => inner.InitializeAsync(cancellationToken);

        public Task<StoredDirectoryEntry?> ReadAsync(DirectoryKey key, CancellationToken cancellationToken) => inner.ReadAsync(key, cancellationToken);

        public Task<bool> TryInsertAsync(DirectoryKey key, DirectoryEntry entry, CancellationToken cancellationToken) =>
            WriteAsync(() => inner.TryInsertAsync(key, entry, CancellationToken.None));

        public Task<bool> TryReplaceAsync(DirectoryKey key, byte[] expectedActivationId, DirectoryEntry entry, CancellationToken cancellationToken) =>
            WriteAsync(() => inner.TryReplaceAsync(key, expectedActivationId, entry, CancellationToken.None));

        public Task<bool> TryDeleteAsync(DirectoryKey key, byte[] expectedActivationId, CancellationToken cancellationToken) =>
            inner.TryDeleteAsync(key, expectedActivationId, cancellationToken);

        public Task<int> DeleteSiloBatchAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, int limit, CancellationToken cancellationToken) =>
            inner.DeleteSiloBatchAsync(scopeHash, siloHash, siloAddress, limit, cancellationToken);

        public Task<bool> HasSiloEntriesAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, CancellationToken cancellationToken) =>
            inner.HasSiloEntriesAsync(scopeHash, siloHash, siloAddress, cancellationToken);

        private async Task<bool> WriteAsync(Func<Task<bool>> write)
        {
            if (CancelBeforeCommit)
            {
                throw new OperationCanceledException();
            }

            var written = await write();
            if (CommitThenCancel && written)
            {
                throw new OperationCanceledException();
            }

            return written;
        }
    }
}
