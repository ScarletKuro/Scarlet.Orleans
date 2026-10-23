namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// A row of <c>orleans_grain_state</c>. Internal, so the table is part of the model without being public API.
/// </summary>
internal sealed class GrainStateRecord
{
    public byte[] KeyHash { get; set; } = null!;

    public byte[] GrainIdentity { get; set; } = null!;

    public string ServiceId { get; set; } = null!;

    public string StateName { get; set; } = null!;

    public string GrainId { get; set; } = null!;

    public byte[]? Payload { get; set; }

    public byte[] ETag { get; set; } = null!;

    public DateTime ModifiedOn { get; set; }
}
