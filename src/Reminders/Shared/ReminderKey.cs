using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Reminders;

/// <summary>
/// The storage identity of one reminder, and the hashes that find it: by reminder, by grain and by service.
/// </summary>
/// <remarks>
/// <para>
/// The identity is the service ID, grain type, grain key and reminder name (<see cref="StorageIdentity"/>). Range reads
/// return reminders of any grain, so a row's grain ID and name are decoded from its identity rather than stored in
/// columns of limited size (dotnet/orleans#9653).
/// </para>
/// <para>
/// Every hash is a 16-byte XxHash128 of an encoded identity: <see cref="Hash"/> of the whole identity,
/// <see cref="GrainKeyHash"/> of the service ID, grain type and grain key, <see cref="ServiceHash"/> of the service ID.
/// Rows found by any of them are verified against the identity, so a collision fails loudly instead of returning or
/// replacing another reminder.
/// </para>
/// </remarks>
internal sealed class ReminderKey
{
    private ReminderKey(string serviceId, GrainId grainId, string reminderName)
    {
        ServiceId = serviceId;
        GrainId = grainId;
        ReminderName = reminderName;
        var service = StorageIdentity.StrictUtf8.GetBytes(serviceId);
        Identity = StorageIdentity.Encode(service, grainId.Type.AsSpan(), grainId.Key.AsSpan(), StorageIdentity.StrictUtf8.GetBytes(reminderName));
        Hash = StorageIdentity.Hash(Identity);
        GrainKeyHash = ComputeGrainKeyHash(service, grainId);
        ServiceHash = ComputeServiceHash(service);
    }

    public string ServiceId { get; }

    public GrainId GrainId { get; }

    public string ReminderName { get; }

    public byte[] Identity { get; }

    public byte[] Hash { get; }

    public byte[] GrainKeyHash { get; }

    public byte[] ServiceHash { get; }

    /// <summary>The grain's position on the Orleans ring, which range reads select by.</summary>
    public uint GrainHash => GrainId.GetUniformHashCode();

    /// <summary>The service ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string ServiceIdText => StorageIdentity.DisplayText(ServiceId);

    /// <summary>The grain ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string GrainIdText => StorageIdentity.DisplayText(GrainId.ToString());

    /// <summary>The reminder name as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string ReminderNameText => StorageIdentity.DisplayText(ReminderName);

    public static ReminderKey Create(string serviceId, GrainId grainId, string reminderName) => new(serviceId, grainId, reminderName);

    public static byte[] ComputeServiceHash(ReadOnlySpan<byte> service) =>
        StorageIdentity.Hash(StorageIdentity.Encode(service, [], []));

    public static byte[] ComputeGrainKeyHash(ReadOnlySpan<byte> service, GrainId grainId) =>
        StorageIdentity.Hash(StorageIdentity.Encode(service, grainId.Type.AsSpan(), grainId.Key.AsSpan()));

    /// <summary>
    /// Decodes a stored identity. Returns <see langword="false"/> if it is malformed or belongs to another service.
    /// </summary>
    public static bool TryDecode(byte[] identity, ReadOnlySpan<byte> service, out GrainId grainId, out string reminderName)
    {
        Span<Range> parts = stackalloc Range[4];
        if (!StorageIdentity.TryDecode(identity, parts) || !identity.AsSpan(parts[0]).SequenceEqual(service))
        {
            grainId = default;
            reminderName = null!;
            return false;
        }

        grainId = GrainId.Create(new GrainType(identity[parts[1]]), new IdSpan(identity[parts[2]]));
        reminderName = StorageIdentity.StrictUtf8.GetString(identity.AsSpan(parts[3]));
        return true;
    }

    public override string ToString() => $"ServiceId={ServiceId} GrainId={GrainId} ReminderName={ReminderName}";
}
