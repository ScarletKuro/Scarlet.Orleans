using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;
using Orleans.Runtime;

namespace Scarlet.Orleans.Persistence;

/// <summary>
/// The storage identity of one grain state: its exact bytes, and the 128-bit hash used as the primary key.
/// </summary>
/// <remarks>
/// <para>
/// The identity is the service ID, state name, grain type and grain key, each prefixed with its length so that no two
/// distinct identities encode to the same bytes. Grain types and keys are Orleans byte strings, not necessarily valid
/// UTF-8, so they are used as raw bytes; decoding them would map distinct invalid sequences to the same replacement
/// character. String parts are encoded with strict UTF-8, which rejects unpaired surrogates instead of replacing them.
/// </para>
/// <para>
/// Every read and write compares <see cref="Identity"/>, so a hash collision fails loudly instead of touching another
/// grain's row (design section 4.3).
/// </para>
/// </remarks>
internal sealed class GrainStateKey
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private GrainStateKey(byte[] identity, byte[] hash, string serviceId, string stateName, GrainId grainId)
    {
        Identity = identity;
        Hash = hash;
        ServiceId = serviceId;
        StateName = stateName;
        GrainId = grainId;
    }

    public byte[] Identity { get; }

    public byte[] Hash { get; }

    public string ServiceId { get; }

    public string StateName { get; }

    public GrainId GrainId { get; }

    public static GrainStateKey Create(string serviceId, string stateName, GrainId grainId)
    {
        var serviceIdBytes = StrictUtf8.GetBytes(serviceId);
        var stateNameBytes = StrictUtf8.GetBytes(stateName);
        var type = grainId.Type.AsSpan();
        var key = grainId.Key.AsSpan();

        var identity = new byte[(4 * sizeof(int)) + serviceIdBytes.Length + stateNameBytes.Length + type.Length + key.Length];
        var remaining = identity.AsSpan();
        Append(ref remaining, serviceIdBytes);
        Append(ref remaining, stateNameBytes);
        Append(ref remaining, type);
        Append(ref remaining, key);

        return new GrainStateKey(identity, XxHash128.Hash(identity), serviceId, stateName, grainId);

        static void Append(ref Span<byte> destination, ReadOnlySpan<byte> part)
        {
            BinaryPrimitives.WriteInt32LittleEndian(destination, part.Length);
            part.CopyTo(destination[sizeof(int)..]);
            destination = destination[(sizeof(int) + part.Length)..];
        }
    }

    /// <summary>The service ID as storable display text (<see cref="DisplayText"/>).</summary>
    public string ServiceIdText => DisplayText(ServiceId);

    /// <summary>The state name as storable display text (<see cref="DisplayText"/>).</summary>
    public string StateNameText => DisplayText(StateName);

    /// <summary>The grain ID as storable display text (<see cref="DisplayText"/>).</summary>
    public string GrainIdText => DisplayText(GrainId.ToString());

    public override string ToString() => $"ServiceId={ServiceId} StateName={StateName} GrainId={GrainId}";

    /// <summary>
    /// Makes an identity part storable in a text column. PostgreSQL rejects NUL in text values, and identities may hold
    /// any byte, so NUL is written as <c>\0</c>. The text is never compared; identity lives in <see cref="Identity"/>.
    /// </summary>
    private static string DisplayText(string value) => value.Replace("\0", "\\0", StringComparison.Ordinal);
}
