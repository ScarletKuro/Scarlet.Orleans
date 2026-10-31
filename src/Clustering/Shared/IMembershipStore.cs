namespace Scarlet.Orleans.Clustering;

/// <summary>
/// The atomic database operations behind a membership table.
/// </summary>
/// <remarks>
/// <para>
/// Stores hold no Orleans logic: conversion, identity verification and version arithmetic live in
/// <see cref="RelationalMembershipTable"/>, which both flavours share. Rows found by a hash are returned whatever their
/// identity.
/// </para>
/// <para>
/// <b>Lock order.</b> Every operation that writes member rows of a cluster, except the heartbeat, first writes or locks
/// the cluster's version row, in the same transaction. Writers of one cluster are therefore serialized on that row, the
/// member rows need no concurrency control of their own, and no two transactions take locks in opposite orders. The
/// heartbeat writes one member column and no version, so it needs no order (design section 5.1).
/// </para>
/// <para>
/// <b>Isolation.</b> Writes run at <c>READ COMMITTED</c>, set explicitly, because MySQL and MariaDB otherwise take
/// next-key locks that reach into the neighbouring cluster's rows. Reads run at <c>REPEATABLE READ</c>, version row
/// first: one snapshot on PostgreSQL, MySQL, MariaDB and SQLite, and on SQL Server a shared lock on the version row held
/// to the end, so no write can commit between the version and the members.
/// </para>
/// <para>
/// <b>Retries.</b> A store repeats an operation only when the server reported that it was not applied
/// (<c>TransientRetry</c>), after its transaction rolled back. It never repeats one after a connection failure, whose
/// outcome is unknown; Orleans re-reads the table after any failed write.
/// </para>
/// </remarks>
internal interface IMembershipStore
{
    /// <summary>
    /// Resolves the engine and checks or applies the schema. Called before any other method, possibly more than once.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Creates the cluster's version row at version 0, unless one exists. Returns the identity of the row that existed,
    /// or <see langword="null"/> if this call created it.
    /// </summary>
    Task<byte[]?> TryCreateVersionAsync(ClusterScope scope, byte[] etag, CancellationToken cancellationToken);

    /// <summary>
    /// Reads the cluster's version row and its member rows as one snapshot: all of them, or only the row stored under
    /// <paramref name="memberKeyHash"/>. Without a version row the cluster has no members, so none are read.
    /// </summary>
    Task<MembershipSnapshot> ReadAsync(ClusterScope scope, byte[]? memberKeyHash, CancellationToken cancellationToken);

    /// <summary>
    /// Applies <paramref name="version"/> and inserts <paramref name="row"/>, in one transaction, if the version row
    /// matches and no row exists under the key. Changes nothing otherwise.
    /// </summary>
    Task<MemberInsertResult> TryInsertAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] etag, CancellationToken cancellationToken);

    /// <summary>
    /// Applies <paramref name="version"/> and replaces the entry of the row whose hash, identity and ETag match, in one
    /// transaction. Changes nothing unless both match.
    /// </summary>
    Task<bool> TryUpdateAsync(ClusterScope scope, VersionChange version, MemberRow row, byte[] expectedETag, byte[] etag, CancellationToken cancellationToken);

    /// <summary>
    /// Raises the row's heartbeat time to <paramref name="iAmAliveTicks"/>, unless it is already later, in one statement.
    /// Changes nothing else, not even the row's ETag.
    /// </summary>
    Task UpdateIAmAliveAsync(ClusterScope scope, byte[] memberKeyHash, byte[] memberIdentity, long iAmAliveTicks, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the cluster's dead members whose start, heartbeat and suspicion times are all before
    /// <paramref name="beforeTicks"/>, if the version row under the cluster's hash is the cluster's. Returns how many it
    /// deleted, and the identity of that version row, or <see langword="null"/> if there is none. The version is
    /// unchanged.
    /// </summary>
    Task<(int Deleted, byte[]? VersionIdentity)> DeleteDefunctAsync(ClusterScope scope, long beforeTicks, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes the cluster's version row and member rows, in one transaction, if the version row under the cluster's hash
    /// is the cluster's. Returns the identity of a version row it left in place, or <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// Member rows exist only while their cluster's version row does, because every insert swaps that row. So without a
    /// version row nothing is deleted, and a cluster created concurrently is never left with members and no version.
    /// </remarks>
    Task<byte[]?> DeleteClusterAsync(ClusterScope scope, CancellationToken cancellationToken);

    /// <summary>
    /// Reads the cluster's active members that accept client connections.
    /// </summary>
    Task<IReadOnlyList<StoredGateway>> ReadGatewaysAsync(ClusterScope scope, CancellationToken cancellationToken);
}
