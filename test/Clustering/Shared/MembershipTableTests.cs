using System.Data;
using System.Net;
using System.Transactions;
using Orleans;
using Orleans.Runtime;
using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Clustering.Tests;

/// <summary>
/// The provider's own guarantees, beyond Orleans' conformance suite: the gateway list, identities, heartbeat order and
/// ambient transactions. Tests isolate their rows with unique cluster IDs.
/// </summary>
public abstract class MembershipTableTests(DatabaseFixture fixture) : IAsyncLifetime
{
    private static readonly DateTime Start = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);

    private TestDatabase Database => fixture.Database;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    public ValueTask InitializeAsync()
    {
        fixture.Require();
        return new(FlavourMembership.CreateTable(Database, "schema", TestSchemaMode.Apply).InitializeMembershipTableAsync(false, CancellationToken));
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task GatewayList_HoldsActiveSilosWithAProxyPort_OfItsOwnCluster()
    {
        var clusterId = NewClusterId();
        var table = await CreateTableAsync(clusterId);
        var gateway = Entry(1, SiloStatus.Active, proxyPort: 30001);
        await InsertAsync(table, gateway);
        await InsertAsync(table, Entry(2, SiloStatus.Active, proxyPort: 0));
        await InsertAsync(table, Entry(3, SiloStatus.Joining, proxyPort: 30003));
        await InsertAsync(table, Entry(4, SiloStatus.Dead, proxyPort: 30004));
        var other = await CreateTableAsync(clusterId + "-other");
        await InsertAsync(other, Entry(5, SiloStatus.Active, proxyPort: 30005));

        var gateways = FlavourMembership.CreateGatewayListProvider(Database, clusterId);
        await gateways.InitializeGatewayListProvider();

        var expected = SiloAddress.New(gateway.SiloAddress.Endpoint.Address, 30001, gateway.SiloAddress.Generation).ToGatewayUri();
        Assert.Equal([expected], await gateways.GetGateways());
    }

    [Fact]
    public async Task Collision_OfAMemberKey_FailsReadsAndInserts_AndChangesNothing()
    {
        var clusterId = NewClusterId();
        var table = await CreateTableAsync(clusterId);
        var holder = Entry(1, SiloStatus.Active);
        var victim = Entry(2, SiloStatus.Active);
        await InsertAsync(table, holder);

        // Moves the holder's row under the victim's key, as a hash collision between the two would place it.
        await ExecuteAsync(
            $"UPDATE {ClusteringSchema.MemberTableName} SET key_hash = @to WHERE key_hash = @from",
            ("to", KeyHash(clusterId, victim)),
            ("from", KeyHash(clusterId, holder)));
        var before = await table.ReadAllAsync(CancellationToken);

        await Assert.ThrowsAsync<MembershipKeyCollisionException>(() => table.ReadRowAsync(victim.SiloAddress, CancellationToken));
        await Assert.ThrowsAsync<MembershipKeyCollisionException>(() => table.InsertRowAsync(victim, before.Version.Next(), CancellationToken));
        Assert.False(await table.UpdateRowAsync(victim, before.Members[0].Item2, before.Version.Next(), CancellationToken));
        await table.UpdateIAmAliveAsync(victim.WithIAmAlive(Start.AddHours(1)), CancellationToken);

        var after = await table.ReadAllAsync(CancellationToken);
        Assert.Equal(before.Version, after.Version);
        var (entry, etag) = Assert.Single(after.Members);
        Assert.Equal(holder.SiloAddress, entry.SiloAddress);
        Assert.Equal(holder.IAmAliveTime, entry.IAmAliveTime);
        Assert.Equal(before.Members[0].Item2, etag);
    }

    [Fact]
    public async Task Collision_OfAClusterKey_FailsReads()
    {
        var clusterId = NewClusterId();
        var table = await CreateTableAsync(clusterId);
        await ExecuteAsync(
            $"UPDATE {ClusteringSchema.VersionTableName} SET cluster_identity = @identity WHERE cluster_hash = @hash",
            ("identity", StorageIdentity.StrictUtf8.GetBytes("another cluster")),
            ("hash", new ClusterScope(clusterId).Hash));

        await Assert.ThrowsAsync<MembershipKeyCollisionException>(() => table.ReadAllAsync(CancellationToken));
        await Assert.ThrowsAsync<MembershipKeyCollisionException>(() => table.InitializeMembershipTableAsync(true, CancellationToken));
    }

    [Fact]
    public async Task Collision_OfAClusterKey_FailsCleanupAndDeletion_AndDeletesNothing()
    {
        var clusterId = NewClusterId();
        var table = await CreateTableAsync(clusterId);
        await InsertAsync(table, Entry(1, SiloStatus.Dead));
        var hash = new ClusterScope(clusterId).Hash;
        await ExecuteAsync(
            $"UPDATE {ClusteringSchema.VersionTableName} SET cluster_identity = @identity WHERE cluster_hash = @hash",
            ("identity", StorageIdentity.StrictUtf8.GetBytes("another cluster")),
            ("hash", hash));

        await Assert.ThrowsAsync<MembershipKeyCollisionException>(() => table.CleanupDefunctSiloEntriesAsync(Start.AddDays(1), CancellationToken));
        await Assert.ThrowsAsync<MembershipKeyCollisionException>(() => table.DeleteMembershipTableEntriesAsync(clusterId, CancellationToken));

        Assert.Equal(1, await CountAsync(ClusteringSchema.VersionTableName, hash));
        Assert.Equal(1, await CountAsync(ClusteringSchema.MemberTableName, hash));
    }

    [Fact]
    public async Task Cleanup_KeepsTheVersion_AtItsLargestValue()
    {
        var clusterId = NewClusterId();
        var table = await CreateTableAsync(clusterId);
        await ExecuteAsync(
            $"UPDATE {ClusteringSchema.VersionTableName} SET version = {int.MaxValue} WHERE cluster_hash = @hash",
            ("hash", new ClusterScope(clusterId).Hash));
        var before = await table.ReadAllAsync(CancellationToken);

        await table.CleanupDefunctSiloEntriesAsync(Start, CancellationToken);

        Assert.Equal(int.MaxValue, before.Version.Version);
        Assert.Equal(before.Version, (await table.ReadAllAsync(CancellationToken)).Version);
    }

    [Fact]
    public async Task WithoutAVersionRow_MemberRowsAreNeitherReadNorDeleted()
    {
        var clusterId = NewClusterId();
        var table = await CreateTableAsync(clusterId);
        var entry = Entry(1, SiloStatus.Active);
        await InsertAsync(table, entry);
        var hash = new ClusterScope(clusterId).Hash;
        await ExecuteAsync($"DELETE FROM {ClusteringSchema.VersionTableName} WHERE cluster_hash = @hash", ("hash", hash));

        var all = await table.ReadAllAsync(CancellationToken);
        var row = await table.ReadRowAsync(entry.SiloAddress, CancellationToken);
        await table.DeleteMembershipTableEntriesAsync(clusterId, CancellationToken);

        Assert.Empty(all.Members);
        Assert.Equal(0, all.Version.Version);
        Assert.Empty(row.Members);
        Assert.Equal(1, await CountAsync(ClusteringSchema.MemberTableName, hash));
    }

    [Fact]
    public async Task DeletingACluster_RemovesItsRows_AndNoOtherClusters()
    {
        var clusterId = NewClusterId();
        var table = await CreateTableAsync(clusterId);
        await InsertAsync(table, Entry(1, SiloStatus.Active));
        var other = await CreateTableAsync(clusterId + "-other");
        await InsertAsync(other, Entry(1, SiloStatus.Active));

        await table.DeleteMembershipTableEntriesAsync(clusterId, CancellationToken);

        var hash = new ClusterScope(clusterId).Hash;
        Assert.Equal(0, await CountAsync(ClusteringSchema.VersionTableName, hash));
        Assert.Equal(0, await CountAsync(ClusteringSchema.MemberTableName, hash));
        Assert.Single((await other.ReadAllAsync(CancellationToken)).Members);
    }

    [Fact]
    public async Task Heartbeat_NeverMovesBackwards_AndKeepsTheVersionAndETag()
    {
        var table = await CreateTableAsync(NewClusterId());
        var entry = Entry(1, SiloStatus.Active);
        await InsertAsync(table, entry);
        var before = await table.ReadAllAsync(CancellationToken);

        await table.UpdateIAmAliveAsync(entry.WithIAmAlive(Start.AddMinutes(5)), CancellationToken);
        await table.UpdateIAmAliveAsync(entry.WithIAmAlive(Start.AddMinutes(2)), CancellationToken);

        var after = await table.ReadAllAsync(CancellationToken);
        Assert.Equal(before.Version, after.Version);
        var (stored, etag) = Assert.Single(after.Members);
        Assert.Equal(Start.AddMinutes(5), stored.IAmAliveTime);
        Assert.Equal(before.Members[0].Item2, etag);

        // An update made with the entry read before the heartbeat keeps the later heartbeat.
        Assert.True(await table.UpdateRowAsync(entry.WithStatus(SiloStatus.ShuttingDown), etag, after.Version.Next(), CancellationToken));
        Assert.Equal(Start.AddMinutes(5), Assert.Single((await table.ReadAllAsync(CancellationToken)).Members).Item1.IAmAliveTime);
    }

    [Fact]
    public async Task Times_RoundTripToTheTick_AsUtc()
    {
        var table = await CreateTableAsync(NewClusterId());
        var entry = Entry(1, SiloStatus.Active);
        entry.StartTime = new DateTime(Start.Ticks + 1234567, DateTimeKind.Unspecified);
        entry.IAmAliveTime = new DateTime(Start.Ticks + 7654321, DateTimeKind.Utc);
        entry.AddSuspector(Entry(2, SiloStatus.Active).SiloAddress, new DateTime(Start.Ticks + 1, DateTimeKind.Utc));
        await InsertAsync(table, entry);

        var (stored, _) = Assert.Single((await table.ReadAllAsync(CancellationToken)).Members);

        Assert.Equal(entry.StartTime.Ticks, stored.StartTime.Ticks);
        Assert.Equal(entry.IAmAliveTime.Ticks, stored.IAmAliveTime.Ticks);
        Assert.Equal(DateTimeKind.Utc, stored.StartTime.Kind);
        Assert.NotNull(stored.SuspectTimes);
        var (suspector, time) = Assert.Single(stored.SuspectTimes);
        Assert.Equal(Entry(2, SiloStatus.Active).SiloAddress, suspector);
        Assert.Equal(Start.Ticks + 1, time.Ticks);
    }

    [Fact]
    public async Task WithoutAVersionRow_ReadsAreEmpty_AndWritesFail()
    {
        var table = FlavourMembership.CreateTable(Database, NewClusterId(), TestSchemaMode.None);
        await table.InitializeMembershipTableAsync(tryInitTableVersion: false, CancellationToken);

        var data = await table.ReadAllAsync(CancellationToken);

        Assert.Empty(data.Members);
        Assert.Equal(0, data.Version.Version);
        Assert.False(await table.InsertRowAsync(Entry(1, SiloStatus.Joining), data.Version.Next(), CancellationToken));
    }

    [Fact]
    public async Task AmbientTransaction_IsNotJoined()
    {
        var table = await CreateTableAsync(NewClusterId());
        var entry = Entry(1, SiloStatus.Joining);

        using (new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            await InsertAsync(table, entry);
        }

        Assert.Single((await table.ReadAllAsync(CancellationToken)).Members);
    }

    private async Task<RelationalMembershipTable> CreateTableAsync(string clusterId)
    {
        var table = FlavourMembership.CreateTable(Database, clusterId, TestSchemaMode.None);
        await table.InitializeMembershipTableAsync(tryInitTableVersion: true, CancellationToken);
        return table;
    }

    private static async Task InsertAsync(IMembershipTable table, MembershipEntry entry)
    {
        var data = await table.ReadAllAsync(CancellationToken);
        Assert.True(await table.InsertRowAsync(entry, data.Version.Next(), CancellationToken));
    }

    private static MembershipEntry Entry(int silo, SiloStatus status, int proxyPort = 30000) => new()
    {
        SiloAddress = SiloAddress.New(IPAddress.Loopback, 11000 + silo, 1000 + silo),
        Status = status,
        ProxyPort = proxyPort,
        HostName = "host",
        SiloName = $"silo-{silo}",
        RoleName = "role",
        StartTime = Start,
        IAmAliveTime = Start,
    };

    private static byte[] KeyHash(string clusterId, MembershipEntry entry) => MemberRow.From(new ClusterScope(clusterId), entry).KeyHash;

    private static string NewClusterId() => $"cluster-{Guid.NewGuid():N}";

    private async Task ExecuteAsync(string sql, params (string Name, byte[] Value)[] parameters)
    {
        await using var command = Database.DataSource.CreateCommand(sql);
        foreach (var (name, value) in parameters)
        {
            command.AddParameter(name, value, DbType.Binary);
        }

        Assert.Equal(1, await command.ExecuteNonQueryAsync(CancellationToken));
    }

    private async Task<long> CountAsync(string table, byte[] clusterHash)
    {
        await using var command = Database.DataSource.CreateCommand($"SELECT COUNT(*) FROM {table} WHERE cluster_hash = @hash");
        command.AddParameter("hash", clusterHash, DbType.Binary);
        return Convert.ToInt64(await command.ExecuteScalarAsync(CancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }
}

internal static class MembershipEntryExtensions
{
    public static MembershipEntry WithIAmAlive(this MembershipEntry entry, DateTime time) => With(entry, e => e.IAmAliveTime = time);

    public static MembershipEntry WithStatus(this MembershipEntry entry, SiloStatus status) => With(entry, e => e.Status = status);

    private static MembershipEntry With(MembershipEntry entry, Action<MembershipEntry> change)
    {
        var copy = new MembershipEntry
        {
            SiloAddress = entry.SiloAddress,
            Status = entry.Status,
            SuspectTimes = entry.SuspectTimes is null ? null : [.. entry.SuspectTimes],
            ProxyPort = entry.ProxyPort,
            HostName = entry.HostName,
            SiloName = entry.SiloName,
            RoleName = entry.RoleName,
            UpdateZone = entry.UpdateZone,
            FaultZone = entry.FaultZone,
            StartTime = entry.StartTime,
            IAmAliveTime = entry.IAmAliveTime,
        };
        change(copy);
        return copy;
    }
}
