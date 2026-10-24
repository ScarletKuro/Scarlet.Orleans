using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Persistence;

/// <summary>
/// The storage identity of one grain state: its exact bytes, and the 128-bit hash used as the primary key.
/// </summary>
/// <remarks>
/// The identity is the service ID, state name, grain type and grain key (<see cref="StorageIdentity"/>). Every read and
/// write compares <see cref="Identity"/>, so a hash collision fails loudly instead of touching another grain's row.
/// </remarks>
internal sealed class GrainStateKey
{
    private GrainStateKey(byte[] identity, string serviceId, string stateName, GrainId grainId)
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

    public static GrainStateKey Create(string serviceId, string stateName, GrainId grainId) =>
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
