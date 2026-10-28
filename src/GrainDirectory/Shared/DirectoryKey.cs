using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.GrainDirectory;

/// <summary>
/// The storage identity of one grain's directory entry, and the hashes that find it: by grain, and by directory.
/// </summary>
/// <remarks>
/// The identity is the cluster ID, directory name, grain type and grain key (<see cref="StorageIdentity"/>).
/// <see cref="Hash"/> hashes the whole identity and is the primary key; <see cref="ScopeHash"/> hashes the cluster ID and
/// directory name, so one directory's entries can be found by silo. Every read and write compares <see cref="Identity"/>,
/// so a collision fails loudly instead of returning or replacing another grain's entry.
/// </remarks>
internal sealed class DirectoryKey
{
    private DirectoryKey(string clusterId, string directoryName, GrainId grainId, byte[] identity, byte[] scopeHash)
    {
        ClusterId = clusterId;
        DirectoryName = directoryName;
        GrainId = grainId;
        Identity = identity;
        Hash = StorageIdentity.Hash(identity);
        ScopeHash = scopeHash;
    }

    public string ClusterId { get; }

    public string DirectoryName { get; }

    public GrainId GrainId { get; }

    public byte[] Identity { get; }

    public byte[] Hash { get; }

    public byte[] ScopeHash { get; }

    /// <summary>The cluster ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string ClusterIdText => StorageIdentity.DisplayText(ClusterId);

    /// <summary>The directory name as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string DirectoryNameText => StorageIdentity.DisplayText(DirectoryName);

    /// <summary>The grain ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string GrainIdText => StorageIdentity.DisplayText(GrainId.ToString());

    public static DirectoryKey Create(DirectoryScope scope, GrainId grainId) =>
        new(
            scope.ClusterId,
            scope.DirectoryName,
            grainId,
            StorageIdentity.Encode(scope.ClusterIdBytes, scope.DirectoryNameBytes, grainId.Type.AsSpan(), grainId.Key.AsSpan()),
            scope.Hash);

    public override string ToString() => $"ClusterId={ClusterId} Directory={DirectoryName} GrainId={GrainId}";
}

/// <summary>
/// One grain directory of one cluster, whose entries share <see cref="Hash"/>.
/// </summary>
internal sealed class DirectoryScope
{
    public DirectoryScope(string clusterId, string directoryName)
    {
        ClusterId = clusterId;
        DirectoryName = directoryName;
        ClusterIdBytes = StorageIdentity.StrictUtf8.GetBytes(clusterId);
        DirectoryNameBytes = StorageIdentity.StrictUtf8.GetBytes(directoryName);
        Hash = StorageIdentity.Hash(StorageIdentity.Encode(ClusterIdBytes, DirectoryNameBytes, []));
    }

    public string ClusterId { get; }

    public string DirectoryName { get; }

    public byte[] ClusterIdBytes { get; }

    public byte[] DirectoryNameBytes { get; }

    public byte[] Hash { get; }
}

/// <summary>
/// The parts of a <see cref="GrainAddress"/> a directory entry stores, in their stored form.
/// </summary>
/// <param name="SiloAddress">The silo's parsable address, which is also compared to find a silo's entries.</param>
/// <param name="SiloHash">The 16-byte hash of <paramref name="SiloAddress"/>, which indexes a silo's entries.</param>
/// <param name="ActivationId">The activation ID's 16 bytes.</param>
/// <param name="MembershipVersion">The membership version the activation was registered at.</param>
internal sealed record DirectoryEntry(string SiloAddress, byte[] SiloHash, byte[] ActivationId, long MembershipVersion)
{
    public static DirectoryEntry From(GrainAddress address)
    {
        var silo = address.SiloAddress!.ToParsableString();
        return new(silo, SiloHashOf(silo), ActivationBytes(address.ActivationId), address.MembershipVersion.Value);
    }

    public static byte[] SiloHashOf(string siloAddress) => StorageIdentity.Hash(StorageIdentity.StrictUtf8.GetBytes(siloAddress));

    /// <summary>
    /// The activation ID's GUID as 16 bytes. Its parsable form is <c>@</c> followed by the GUID in <c>N</c> format.
    /// </summary>
    public static byte[] ActivationBytes(ActivationId activationId) =>
        Guid.ParseExact(activationId.ToParsableString().AsSpan(1), "N").ToByteArray();

    public GrainAddress ToAddress(GrainId grainId) => new()
    {
        GrainId = grainId,
        SiloAddress = global::Orleans.Runtime.SiloAddress.FromParsableString(SiloAddress),
        ActivationId = new ActivationId(new Guid(ActivationId)),
        MembershipVersion = new MembershipVersion(MembershipVersion),
    };
}
