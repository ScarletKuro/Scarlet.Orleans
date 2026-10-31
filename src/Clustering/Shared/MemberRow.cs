using System.Globalization;
using System.Text;
using Orleans;
using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Clustering;

/// <summary>
/// One cluster in the shared tables: its identity, the cluster ID's UTF-8 bytes, and their hash, the key of its version
/// row and the first part of its members' keys.
/// </summary>
internal sealed class ClusterScope
{
    public ClusterScope(string clusterId)
    {
        ClusterId = clusterId;
        Identity = StorageIdentity.StrictUtf8.GetBytes(clusterId);
        Hash = StorageIdentity.Hash(Identity);
    }

    public string ClusterId { get; }

    public byte[] Identity { get; }

    public byte[] Hash { get; }

    /// <summary>The cluster ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string ClusterIdText => StorageIdentity.DisplayText(ClusterId);

    /// <summary>
    /// The identity of a silo's row: the cluster ID and the silo's parsable address, which includes its generation.
    /// </summary>
    public byte[] MemberIdentity(string siloAddress) =>
        StorageIdentity.Encode(Identity, StorageIdentity.StrictUtf8.GetBytes(siloAddress));

    /// <summary>
    /// Whether a member row's identity belongs to this cluster, so a row found by the cluster's hash is not another cluster's.
    /// </summary>
    public bool OwnsMember(byte[] memberIdentity)
    {
        Span<Range> parts = stackalloc Range[2];
        return StorageIdentity.TryDecode(memberIdentity, parts) && memberIdentity.AsSpan(parts[0]).SequenceEqual(Identity);
    }

    public override string ToString() => $"ClusterId={ClusterId}";
}

/// <summary>
/// The table version of a cluster, in its stored form.
/// </summary>
/// <param name="Identity">The cluster identity stored with the row (<see cref="ClusterScope.Identity"/>).</param>
/// <param name="Version">The version, which every membership change advances by one.</param>
/// <param name="ETag">The 16-byte ETag, replaced with every change.</param>
internal sealed record StoredVersion(byte[] Identity, int Version, byte[] ETag)
{
    public TableVersion ToTableVersion() => new(Version, ETags.Format(ETag));
}

/// <summary>
/// The compare-and-swap of a cluster's version row that every membership change makes.
/// </summary>
/// <param name="ExpectedVersion">The version the caller read, which the row must still hold.</param>
/// <param name="ExpectedETag">The ETag the caller read, which the row must still hold.</param>
/// <param name="NewVersion">The version to write, one more than <paramref name="ExpectedVersion"/>.</param>
/// <param name="NewETag">The ETag to write.</param>
internal sealed record VersionChange(int ExpectedVersion, byte[] ExpectedETag, int NewVersion, byte[] NewETag);

/// <summary>
/// A <see cref="MembershipEntry"/> in its stored form.
/// </summary>
/// <remarks>
/// Times are UTC ticks, exact on every engine; Orleans compares them exactly. The suspect list is text, one
/// <c>address|ticks</c> pair per vote, separated by <c>;</c>, neither of which a parsable silo address contains.
/// <see cref="LatestSuspectTicks"/> repeats the latest vote time, so cleanup can compare it in SQL.
/// </remarks>
internal sealed record MemberRow(
    byte[] KeyHash,
    byte[] Identity,
    string SiloAddress,
    int Status,
    int ProxyPort,
    string? HostName,
    string? SiloName,
    string? RoleName,
    int UpdateZone,
    int FaultZone,
    string? SuspectTimes,
    long? LatestSuspectTicks,
    long StartTicks,
    long IAmAliveTicks)
{
    public static MemberRow From(ClusterScope scope, MembershipEntry entry)
    {
        var siloAddress = entry.SiloAddress.ToParsableString();
        var identity = scope.MemberIdentity(siloAddress);
        var (suspectTimes, latest) = EncodeSuspectTimes(entry.SuspectTimes);
        return new MemberRow(
            StorageIdentity.Hash(identity),
            identity,
            siloAddress,
            (int)entry.Status,
            entry.ProxyPort,
            entry.HostName,
            entry.SiloName,
            entry.RoleName,
            entry.UpdateZone,
            entry.FaultZone,
            suspectTimes,
            latest,
            UtcTicks(entry.StartTime),
            UtcTicks(entry.IAmAliveTime));
    }

    public MembershipEntry ToEntry() => new()
    {
        SiloAddress = global::Orleans.Runtime.SiloAddress.FromParsableString(SiloAddress),
        Status = (SiloStatus)Status,
        ProxyPort = ProxyPort,
        HostName = HostName!,
        SiloName = SiloName!,
        RoleName = RoleName,
        UpdateZone = UpdateZone,
        FaultZone = FaultZone,
        SuspectTimes = DecodeSuspectTimes(SuspectTimes),
        StartTime = new DateTime(StartTicks, DateTimeKind.Utc),
        IAmAliveTime = new DateTime(IAmAliveTicks, DateTimeKind.Utc),
    };

    /// <summary>
    /// A time as UTC ticks. A local time is converted; an unspecified one is taken as UTC, as Orleans creates them.
    /// </summary>
    public static long UtcTicks(DateTime time) => time.Kind == DateTimeKind.Local ? time.ToUniversalTime().Ticks : time.Ticks;

    private static (string? Text, long? Latest) EncodeSuspectTimes(List<Tuple<SiloAddress, DateTime>>? votes)
    {
        if (votes is null || votes.Count == 0)
        {
            return (null, null);
        }

        var text = new StringBuilder();
        var latest = long.MinValue;
        foreach (var (silo, time) in votes)
        {
            var ticks = UtcTicks(time);
            latest = Math.Max(latest, ticks);
            if (text.Length > 0)
            {
                text.Append(';');
            }

            text.Append(silo.ToParsableString()).Append('|').Append(ticks.ToString(CultureInfo.InvariantCulture));
        }

        return (text.ToString(), latest);
    }

    private static List<Tuple<SiloAddress, DateTime>>? DecodeSuspectTimes(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        var votes = new List<Tuple<SiloAddress, DateTime>>();
        foreach (var vote in text.Split(';'))
        {
            var separator = vote.LastIndexOf('|');
            votes.Add(Tuple.Create(
                global::Orleans.Runtime.SiloAddress.FromParsableString(vote[..separator]),
                new DateTime(long.Parse(vote.AsSpan(separator + 1), NumberStyles.Integer, CultureInfo.InvariantCulture), DateTimeKind.Utc)));
        }

        return votes;
    }
}

/// <param name="Row">The stored entry.</param>
/// <param name="ETag">The row's 16-byte ETag, replaced by every change of the entry but not by heartbeats.</param>
internal sealed record StoredMember(MemberRow Row, byte[] ETag);

/// <param name="Version">The cluster's version row, or <see langword="null"/> if the cluster has none.</param>
/// <param name="Members">The member rows read with it, in no particular order.</param>
internal sealed record MembershipSnapshot(StoredVersion? Version, IReadOnlyList<StoredMember> Members);

/// <param name="Inserted">Whether the row was inserted and the version advanced.</param>
/// <param name="ExistingIdentity">
/// When the version matched but a row exists under the key, that row's identity; <see langword="null"/> otherwise.
/// </param>
internal readonly record struct MemberInsertResult(bool Inserted, byte[]? ExistingIdentity);

/// <param name="Identity">The member row's identity.</param>
/// <param name="SiloAddress">The silo's parsable address.</param>
/// <param name="ProxyPort">The silo's gateway port.</param>
internal sealed record StoredGateway(byte[] Identity, string SiloAddress, int ProxyPort);
