namespace Scarlet.Orleans.Persistence;

/// <summary>
/// The atomic database operations behind grain storage. Each method runs one statement and reports that statement's result.
/// </summary>
/// <remarks>
/// <para>
/// Stores hold no Orleans logic: ETag rules, identity verification and serialization live in
/// <see cref="RelationalGrainStorage"/>, which both flavours share.
/// </para>
/// <para>
/// <b>Results describe the last statement, not the logical operation.</b> A <see langword="false"/> result means the
/// statement that ran wrote nothing. It does not prove that the operation never took effect: if a driver or EF Core
/// repeated a statement whose first attempt committed but whose acknowledgement was lost, the repeat matches no row.
/// Stores do not try to tell these cases apart. <see cref="RelationalGrainStorage"/> settles them, using the ETag it
/// generated for the write.
/// </para>
/// <para>
/// <b>Retries.</b> A store may repeat a statement only when the server reported that the statement was not applied
/// (<c>TransientRetry</c>). It never repeats a statement after a connection failure, whose outcome is unknown; that
/// failure surfaces to Orleans. A store also prevents layers it controls from repeating writes, such as a retrying EF
/// Core execution strategy.
/// </para>
/// <para>
/// <b>Transactions.</b> Every statement autocommits. Callers run outside any ambient transaction.
/// </para>
/// </remarks>
internal interface IGrainStateStore
{
    /// <summary>
    /// Resolves the engine and checks or applies the schema. Called once before any other method.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Reads the row stored under <see cref="GrainStateKey.Hash"/>, whatever its identity, or <see langword="null"/>.
    /// </summary>
    Task<StoredGrainState?> ReadAsync(GrainStateKey key, CancellationToken cancellationToken);

    /// <summary>
    /// Inserts a row. Returns <see langword="false"/> when the statement wrote nothing because a row with the same hash exists.
    /// </summary>
    Task<bool> TryInsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken);

    /// <summary>
    /// Replaces the payload and ETag of the row whose hash, identity and ETag all match. Returns whether the statement
    /// matched a row. A <see langword="null"/> payload marks the state as cleared while keeping the row.
    /// </summary>
    Task<bool> TryUpdateAsync(GrainStateKey key, byte[] expectedETag, byte[]? payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the row whose hash, identity and ETag all match. Returns whether the statement matched a row.
    /// </summary>
    Task<bool> TryDeleteAsync(GrainStateKey key, byte[] expectedETag, CancellationToken cancellationToken);
}

/// <param name="Identity">The identity bytes stored with the row, compared with the requested key.</param>
/// <param name="Payload">The serialized state, or <see langword="null"/> when the state was cleared.</param>
/// <param name="ETag">The current ETag of the row.</param>
internal sealed record StoredGrainState(byte[] Identity, byte[]? Payload, byte[] ETag);
