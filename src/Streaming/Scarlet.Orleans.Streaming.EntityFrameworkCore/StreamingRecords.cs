namespace Scarlet.Orleans.Streaming.EntityFrameworkCore;

/// <summary>
/// A row of <c>orleans_stream_queue</c>. Internal, so the table is part of the model without being public API.
/// </summary>
internal sealed class StreamQueueRecord
{
    public byte[] QueueHash { get; set; } = null!;

    public byte[] QueueIdentity { get; set; } = null!;

    public string ServiceId { get; set; } = null!;

    public string ProviderName { get; set; } = null!;

    public string QueueId { get; set; } = null!;

    public long EvictOn { get; set; }

    public DateTime CreatedOn { get; set; }
}

/// <summary>A row of <c>orleans_stream_message</c>: one enqueued batch.</summary>
internal sealed class StreamMessageRecord
{
    public long MessageId { get; set; }

    public byte[] QueueHash { get; set; } = null!;

    public byte[] MessageKey { get; set; } = null!;

    public int Dequeued { get; set; }

    public long VisibleOn { get; set; }

    public long ExpiresOn { get; set; }

    public DateTime CreatedOn { get; set; }

    public byte[] Payload { get; set; } = null!;
}

/// <summary>A row of <c>orleans_stream_dead_letter</c>: a message that was not delivered.</summary>
internal sealed class StreamDeadLetterRecord
{
    public byte[] QueueHash { get; set; } = null!;

    public long MessageId { get; set; }

    public byte[] MessageKey { get; set; } = null!;

    public int Dequeued { get; set; }

    public long ExpiresOn { get; set; }

    public DateTime CreatedOn { get; set; }

    public byte[] Payload { get; set; } = null!;

    public DateTime DeadOn { get; set; }

    public long RemoveOn { get; set; }
}
