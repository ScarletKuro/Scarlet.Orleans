namespace Scarlet.Orleans.Reminders.EntityFrameworkCore;

/// <summary>
/// A row of <c>orleans_reminder</c>. Internal, so the table is part of the model without being public API.
/// </summary>
internal sealed class ReminderRecord
{
    public byte[] KeyHash { get; set; } = null!;

    public byte[] ReminderIdentity { get; set; } = null!;

    public byte[] ServiceHash { get; set; } = null!;

    public byte[] GrainKeyHash { get; set; } = null!;

    public long GrainHash { get; set; }

    public string ServiceId { get; set; } = null!;

    public string GrainId { get; set; } = null!;

    public string ReminderName { get; set; } = null!;

    public long StartAtTicks { get; set; }

    public long PeriodTicks { get; set; }

    public byte[] ETag { get; set; } = null!;
}
