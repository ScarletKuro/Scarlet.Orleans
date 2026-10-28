namespace Scarlet.Orleans.GrainDirectory;

/// <summary>
/// The atomic database operations behind a grain directory. Each method runs one statement and reports that statement's result.
/// </summary>
/// <remarks>
/// <para>
/// Stores hold no Orleans logic: first-writer-wins registration, identity verification and address conversion live in
/// <see cref="RelationalGrainDirectory"/>, which both flavours share. Rows found by a hash are returned whatever their identity.
/// </para>
/// <para>
/// <b>Retries.</b> A store may repeat a statement only when the server reported that the statement was not applied
/// (<c>TransientRetry</c>). It never repeats a statement after a connection failure, whose outcome is unknown; that
/// failure surfaces to Orleans. A store also prevents layers it controls from repeating writes, such as a retrying EF
/// Core execution strategy.
/// </para>
/// <para>
/// <b>Transactions.</b> Every statement autocommits. Callers run outside any ambient transaction. No statement is an
/// upsert or a range lock: registration is a plain insert, so concurrent registrations of different grains never take
/// gap locks on each other (design section 5.2).
/// </para>
/// </remarks>
internal interface IGrainDirectoryStore
{
    /// <summary>
    /// Resolves the engine and checks or applies the schema. Called once before any other method.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Reads the row stored under <see cref="DirectoryKey.Hash"/>, whatever its identity, or <see langword="null"/>.
    /// </summary>
    Task<StoredDirectoryEntry?> ReadAsync(DirectoryKey key, CancellationToken cancellationToken);

    /// <summary>
    /// Inserts a row. Returns <see langword="false"/> when the statement wrote nothing because a row with the same hash exists.
    /// </summary>
    Task<bool> TryInsertAsync(DirectoryKey key, DirectoryEntry entry, CancellationToken cancellationToken);

    /// <summary>
    /// Replaces the entry of the row whose hash, identity and activation ID all match. Returns whether the statement
    /// matched a row.
    /// </summary>
    Task<bool> TryReplaceAsync(DirectoryKey key, byte[] expectedActivationId, DirectoryEntry entry, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the row whose hash, identity and activation ID all match. Returns whether the statement matched a row.
    /// </summary>
    Task<bool> TryDeleteAsync(DirectoryKey key, byte[] expectedActivationId, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes up to <paramref name="limit"/> rows of a directory that point at a silo, in key order. Returns how many it deleted.
    /// </summary>
    /// <remarks>
    /// A silo that held many activations leaves many rows. Deleting them in bounded batches keeps each statement's locks
    /// short, so registrations on live silos are not held up behind it.
    /// </remarks>
    Task<int> DeleteSiloBatchAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, int limit, CancellationToken cancellationToken);
}

/// <param name="Identity">The identity bytes stored with the row (<see cref="DirectoryKey"/>).</param>
/// <param name="Entry">The stored address.</param>
internal sealed record StoredDirectoryEntry(byte[] Identity, DirectoryEntry Entry);
