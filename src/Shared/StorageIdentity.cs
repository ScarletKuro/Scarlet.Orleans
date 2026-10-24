using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// Encodes the identity of a stored row as exact bytes, and hashes it into the 16-byte key used as the primary key.
/// </summary>
/// <remarks>
/// <para>
/// Each part is prefixed with its length, so no two distinct identities encode to the same bytes. Grain types and keys are
/// Orleans byte strings, not necessarily valid UTF-8, so they are used as raw bytes; decoding them would map distinct
/// invalid sequences to the same replacement character. String parts are encoded with <see cref="StrictUtf8"/>, which
/// rejects unpaired surrogates instead of replacing them.
/// </para>
/// <para>
/// Rows store their identity next to the hash, and every read and write compares it, so a hash collision fails loudly
/// instead of touching another row (design section 4.3).
/// </para>
/// </remarks>
internal static class StorageIdentity
{
    public static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static byte[] Encode(ReadOnlySpan<byte> part0, ReadOnlySpan<byte> part1, ReadOnlySpan<byte> part2)
    {
        var identity = new byte[(3 * sizeof(int)) + part0.Length + part1.Length + part2.Length];
        var remaining = identity.AsSpan();
        Append(ref remaining, part0);
        Append(ref remaining, part1);
        Append(ref remaining, part2);
        return identity;
    }

    public static byte[] Encode(ReadOnlySpan<byte> part0, ReadOnlySpan<byte> part1, ReadOnlySpan<byte> part2, ReadOnlySpan<byte> part3)
    {
        var identity = new byte[(4 * sizeof(int)) + part0.Length + part1.Length + part2.Length + part3.Length];
        var remaining = identity.AsSpan();
        Append(ref remaining, part0);
        Append(ref remaining, part1);
        Append(ref remaining, part2);
        Append(ref remaining, part3);
        return identity;
    }

    /// <summary>
    /// Splits an identity produced by <see cref="Encode(ReadOnlySpan{byte}, ReadOnlySpan{byte}, ReadOnlySpan{byte}, ReadOnlySpan{byte})"/>
    /// back into its parts. Returns <see langword="false"/> if the bytes are not exactly <paramref name="parts"/>.Length parts.
    /// </summary>
    public static bool TryDecode(ReadOnlySpan<byte> identity, Span<Range> parts)
    {
        var offset = 0;
        for (var i = 0; i < parts.Length; i++)
        {
            if (identity.Length - offset < sizeof(int))
            {
                return false;
            }

            var length = BinaryPrimitives.ReadInt32LittleEndian(identity[offset..]);
            offset += sizeof(int);
            if (length < 0 || length > identity.Length - offset)
            {
                return false;
            }

            parts[i] = offset..(offset + length);
            offset += length;
        }

        return offset == identity.Length;
    }

    public static byte[] Hash(ReadOnlySpan<byte> identity) => XxHash128.Hash(identity);

    /// <summary>
    /// Makes an identity part storable in a text column. PostgreSQL rejects NUL in text values, and identities may hold
    /// any byte, so NUL is written as <c>\0</c>. The text is never compared; identity lives in the encoded bytes.
    /// </summary>
    public static string DisplayText(string value) => value.Replace("\0", "\\0", StringComparison.Ordinal);

    private static void Append(ref Span<byte> destination, ReadOnlySpan<byte> part)
    {
        BinaryPrimitives.WriteInt32LittleEndian(destination, part.Length);
        part.CopyTo(destination[sizeof(int)..]);
        destination = destination[(sizeof(int) + part.Length)..];
    }
}
