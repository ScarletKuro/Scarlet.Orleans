namespace Scarlet.Orleans.GrainDirectory.EntityFrameworkCore;

/// <summary>
/// A row of <c>orleans_grain_directory</c>. Internal, so the table is part of the model without being public API.
/// </summary>
internal sealed class GrainDirectoryRecord
{
    public byte[] KeyHash { get; set; } = null!;

    public byte[] GrainIdentity { get; set; } = null!;

    public byte[] ScopeHash { get; set; } = null!;

    public byte[] SiloHash { get; set; } = null!;

    public string ClusterId { get; set; } = null!;

    public string DirectoryName { get; set; } = null!;

    public string GrainId { get; set; } = null!;

    public string SiloAddress { get; set; } = null!;

    public byte[] ActivationId { get; set; } = null!;

    public long MembershipVersion { get; set; }
}
