using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Transactions;

/// <summary>
/// The storage identity of one transactional state: its exact bytes, and the 128-bit hash used as the primary key.
/// </summary>
/// <remarks>
/// The identity is the service ID, state name, grain type and grain key (<see cref="StorageIdentity"/>), the same parts as
/// a grain state's; the two live in different tables, so they never share a row. Every read and write compares
/// <see cref="Identity"/>, so a hash collision fails loudly instead of touching another state's rows.
/// </remarks>
internal sealed class TransactionalStateKey
{
    private TransactionalStateKey(byte[] identity, string serviceId, string stateName, GrainId grainId)
    {
        Identity = identity;
        Hash = StorageIdentity.Hash(identity);
        ServiceId = serviceId;
        StateName = stateName;
        GrainId = grainId;
    }

    public byte[] Identity { get; }

    public byte[] Hash { get; }

    public string ServiceId { get; }

    public string StateName { get; }

    public GrainId GrainId { get; }

    public static TransactionalStateKey Create(string serviceId, string stateName, GrainId grainId) =>
        new(
            StorageIdentity.Encode(StorageIdentity.StrictUtf8.GetBytes(serviceId), StorageIdentity.StrictUtf8.GetBytes(stateName), grainId.Type.AsSpan(), grainId.Key.AsSpan()),
            serviceId,
            stateName,
            grainId);

    /// <summary>The service ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string ServiceIdText => StorageIdentity.DisplayText(ServiceId);

    /// <summary>The state name as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string StateNameText => StorageIdentity.DisplayText(StateName);

    /// <summary>The grain ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string GrainIdText => StorageIdentity.DisplayText(GrainId.ToString());

    public override string ToString() => $"ServiceId={ServiceId} StateName={StateName} GrainId={GrainId}";
}
