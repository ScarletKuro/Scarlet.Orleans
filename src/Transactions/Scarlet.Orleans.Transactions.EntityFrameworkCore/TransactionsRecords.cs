namespace Scarlet.Orleans.Transactions.EntityFrameworkCore;

/// <summary>
/// A row of <c>orleans_transactional_state</c>. Internal, so the table is part of the model without being public API.
/// </summary>
internal sealed class TransactionalStateRecord
{
    public byte[] KeyHash { get; set; } = null!;

    public byte[] StateIdentity { get; set; } = null!;

    public string ServiceId { get; set; } = null!;

    public string StateName { get; set; } = null!;

    public string GrainId { get; set; } = null!;

    public byte[] ETag { get; set; } = null!;

    public long CommittedSequenceId { get; set; }

    public byte[] Metadata { get; set; } = null!;

    public DateTime ModifiedOn { get; set; }
}

/// <summary>
/// A row of <c>orleans_transactional_state_entry</c>: one prepared version of a transactional state.
/// </summary>
internal sealed class TransactionalStateEntryRecord
{
    public byte[] KeyHash { get; set; } = null!;

    public long SequenceId { get; set; }

    public string TransactionId { get; set; } = null!;

    public long TimestampTicks { get; set; }

    public byte[] TransactionManager { get; set; } = null!;

    public byte[] Payload { get; set; } = null!;
}
