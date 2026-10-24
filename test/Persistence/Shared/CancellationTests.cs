using Microsoft.Extensions.Logging.Abstractions;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Cancellation of grain storage operations, over an in-memory store that records the tokens it receives.
/// </summary>
/// <remarks>Adapted from Orleans' <c>AdoNetGrainStorageCancellationTests</c>.</remarks>
public sealed class CancellationTests
{
    private const string StateName = "state";
    private static readonly GrainId GrainId = GrainId.Create("cancellation-grain", "key");

    public static TheoryData<string> Operations => ["Read", "Write", "Clear", "Delete"];

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task Operations_ForwardTheCallersToken(string operation)
    {
        var (store, storage, state) = await ArrangeAsync(operation);
        using var cancellation = new CancellationTokenSource();

        await InvokeAsync(storage, operation, state, cancellation.Token);

        Assert.NotEmpty(store.Tokens);
        Assert.All(store.Tokens, token => Assert.Equal(cancellation.Token, token));
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task OverloadsWithoutAToken_UseNone(string operation)
    {
        var (store, storage, state) = await ArrangeAsync(operation);

        await (operation switch
        {
            "Read" => storage.ReadStateAsync(StateName, GrainId, state),
            "Write" => storage.WriteStateAsync(StateName, GrainId, state),
            _ => storage.ClearStateAsync(StateName, GrainId, state),
        });

        Assert.NotEmpty(store.Tokens);
        Assert.All(store.Tokens, token => Assert.Equal(CancellationToken.None, token));
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task PreCanceledOperations_PreserveStateAndSkipWork(string operation)
    {
        var (store, storage, state) = await ArrangeAsync(operation);
        var before = Snapshot(state);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => InvokeAsync(storage, operation, state, cancellation.Token));

        Assert.Empty(store.Tokens);
        Assert.Equal(before, Snapshot(state));
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task InFlightCancellation_PreservesTheExceptionAndState(string operation)
    {
        var (store, storage, state) = await ArrangeAsync(operation);
        var before = Snapshot(state);
        using var cancellation = new CancellationTokenSource();
        store.BlockUntilCanceled = true;

        var invocation = InvokeAsync(storage, operation, state, cancellation.Token);
        await store.Blocked.Task.WaitAsync(TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();

        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => invocation);
        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.Equal(before, Snapshot(state));
    }

    [Theory]
    [InlineData("Write")]
    [InlineData("Clear")]
    [InlineData("Delete")]
    public async Task CancellationAfterTheWriteCommitted_StillReportsTheWrite(string operation)
    {
        var (store, storage, state) = await ArrangeAsync(operation);
        var before = Snapshot(state);
        using var cancellation = new CancellationTokenSource();
        store.AfterWrite = cancellation.Cancel;

        await InvokeAsync(storage, operation, state, cancellation.Token);

        Assert.True(cancellation.IsCancellationRequested);
        Assert.NotEqual(before, Snapshot(state));
        Assert.Equal(store.CurrentETag, state.ETag);
    }

    [Theory]
    [InlineData("Write")]
    [InlineData("Clear")]
    public async Task CancellationWhileTheCommittedStatementIsAnswered_StillReportsTheWrite(string operation)
    {
        var (store, storage, state) = await ArrangeAsync(operation);
        using var cancellation = new CancellationTokenSource();
        store.CancelAfterCommit = cancellation;

        await InvokeAsync(storage, operation, state, cancellation.Token);

        Assert.True(cancellation.IsCancellationRequested);
        Assert.Equal(store.CurrentETag, state.ETag);
        Assert.Equal(operation == "Write", state.RecordExists);
    }

    [Fact]
    public async Task CancellationWhileACommittedDeleteIsAnswered_IsReportedAsCanceled()
    {
        var (store, storage, state) = await ArrangeAsync("Delete");
        var before = Snapshot(state);
        using var cancellation = new CancellationTokenSource();
        store.CancelAfterCommit = cancellation;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => InvokeAsync(storage, "Delete", state, cancellation.Token));

        Assert.Null(store.CurrentETag);
        Assert.Equal(before, Snapshot(state));
    }

    private static async Task<(InMemoryStore Store, RelationalGrainStorage Storage, GrainState<TestState> State)> ArrangeAsync(string operation)
    {
        var store = new InMemoryStore();
        var storage = new RelationalGrainStorage(
            "test", "service", store, new JsonTestSerializer(), new TestActivatorProvider(),
            deleteStateOnClear: operation == "Delete", maxPayloadSize: null, initStage: 0, TimeProvider.System, NullLogger<RelationalGrainStorage>.Instance);
        await storage.InitializeAsync(CancellationToken.None);
        var state = new GrainState<TestState> { State = new TestState { Value = 1 } };
        if (operation != "Write")
        {
            await storage.WriteStateAsync(StateName, GrainId, state, CancellationToken.None);
            state.State = new TestState { Value = 2 };
        }

        store.Tokens.Clear();
        return (store, storage, state);
    }

    private static Task InvokeAsync(RelationalGrainStorage storage, string operation, GrainState<TestState> state, CancellationToken cancellationToken) =>
        operation switch
        {
            "Read" => storage.ReadStateAsync(StateName, GrainId, state, cancellationToken),
            "Write" => storage.WriteStateAsync(StateName, GrainId, state, cancellationToken),
            _ => storage.ClearStateAsync(StateName, GrainId, state, cancellationToken),
        };

    private static (string? ETag, bool RecordExists, int? Value) Snapshot(GrainState<TestState> state) =>
        (state.ETag, state.RecordExists, state.State?.Value);

    /// <summary>
    /// A single-row store that records every token, and can block until canceled or act after a write commits.
    /// </summary>
    private sealed class InMemoryStore : IGrainStateStore
    {
        private StoredGrainState? row;

        public List<CancellationToken> Tokens { get; } = [];

        public bool BlockUntilCanceled { get; set; }

        public TaskCompletionSource Blocked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Action? AfterWrite { get; set; }

        /// <summary>Commits the next write, then cancels this source and fails as canceled, as a driver does.</summary>
        public CancellationTokenSource? CancelAfterCommit { get; set; }

        public string? CurrentETag => row is null ? null : new Guid(row.ETag).ToString("N");

        public Task InitializeAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task<StoredGrainState?> ReadAsync(GrainStateKey key, CancellationToken cancellationToken)
        {
            await EnterAsync(cancellationToken);
            return row;
        }

        public async Task<bool> TryInsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken)
        {
            await EnterAsync(cancellationToken);
            if (row is not null)
            {
                return false;
            }

            return Commit(new StoredGrainState(key.Identity, payload, etag));
        }

        public async Task<bool> TryUpdateAsync(GrainStateKey key, byte[] expectedETag, byte[]? payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken)
        {
            await EnterAsync(cancellationToken);
            if (row is null || !row.ETag.AsSpan().SequenceEqual(expectedETag))
            {
                return false;
            }

            return Commit(new StoredGrainState(key.Identity, payload, etag));
        }

        public async Task<bool> TryDeleteAsync(GrainStateKey key, byte[] expectedETag, CancellationToken cancellationToken)
        {
            await EnterAsync(cancellationToken);
            if (row is null || !row.ETag.AsSpan().SequenceEqual(expectedETag))
            {
                return false;
            }

            return Commit(null);
        }

        private bool Commit(StoredGrainState? committed)
        {
            row = committed;
            AfterWrite?.Invoke();
            if (CancelAfterCommit is { } cancellation)
            {
                CancelAfterCommit = null;
                cancellation.Cancel();
                throw new OperationCanceledException(cancellation.Token);
            }

            return true;
        }

        private async Task EnterAsync(CancellationToken cancellationToken)
        {
            Tokens.Add(cancellationToken);
            if (BlockUntilCanceled && cancellationToken.CanBeCanceled)
            {
                Blocked.TrySetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
        }
    }
}
