namespace Scarlet.Orleans.Persistence;

/// <summary>
/// The atomic database operations behind grain storage. Each method is one statement with the stated outcome.
/// </summary>
/// <remarks>
/// Stores hold no Orleans logic: ETag rules, identity verification and serialization live in
/// <see cref="RelationalGrainStorage"/>, which both flavours share.
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
    /// Inserts a row. Returns <see langword="false"/>, writing nothing, when a row with the same hash exists.
    /// </summary>
    Task<bool> TryInsertAsync(GrainStateKey key, byte[] payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken);

    /// <summary>
    /// Replaces the payload and ETag of the row whose hash, identity and ETag all match. Returns whether a row matched.
    /// A <see langword="null"/> payload marks the state as cleared while keeping the row.
    /// </summary>
    Task<bool> TryUpdateAsync(GrainStateKey key, byte[] expectedETag, byte[]? payload, byte[] etag, DateTimeOffset modifiedOn, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the row whose hash, identity and ETag all match. Returns whether a row matched.
    /// </summary>
    Task<bool> TryDeleteAsync(GrainStateKey key, byte[] expectedETag, CancellationToken cancellationToken);
}

/// <param name="Identity">The identity bytes stored with the row, compared with the requested key.</param>
/// <param name="Payload">The serialized state, or <see langword="null"/> when the state was cleared.</param>
/// <param name="ETag">The current ETag of the row.</param>
internal sealed record StoredGrainState(byte[] Identity, byte[]? Payload, byte[] ETag);
