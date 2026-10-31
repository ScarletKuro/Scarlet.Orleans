namespace Scarlet.Orleans.Clustering.EntityFrameworkCore;

/// <summary>
/// A row of <c>orleans_cluster_version</c>. Internal, so the table is part of the model without being public API.
/// </summary>
internal sealed class ClusterVersionRecord
{
    public byte[] ClusterHash { get; set; } = null!;

    public byte[] ClusterIdentity { get; set; } = null!;

    public string ClusterId { get; set; } = null!;

    public int Version { get; set; }

    public byte[] ETag { get; set; } = null!;
}

/// <summary>
/// A row of <c>orleans_cluster_member</c>. Internal, so the table is part of the model without being public API.
/// </summary>
internal sealed class ClusterMemberRecord
{
    public byte[] ClusterHash { get; set; } = null!;

    public byte[] KeyHash { get; set; } = null!;

    public byte[] MemberIdentity { get; set; } = null!;

    public string SiloAddress { get; set; } = null!;

    public int Status { get; set; }

    public int ProxyPort { get; set; }

    public string? HostName { get; set; }

    public string? SiloName { get; set; }

    public string? RoleName { get; set; }

    public int UpdateZone { get; set; }

    public int FaultZone { get; set; }

    public string? SuspectTimes { get; set; }

    public long? LatestSuspectTicks { get; set; }

    public long StartTicks { get; set; }

    public long IAmAliveTicks { get; set; }

    public byte[] ETag { get; set; } = null!;
}
