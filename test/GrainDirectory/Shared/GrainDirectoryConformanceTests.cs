using System.Collections.Concurrent;
using System.Data;
using System.Transactions;
using Orleans.GrainDirectory;
using Orleans.Runtime;

namespace Scarlet.Orleans.GrainDirectory.Tests;

/// <summary>
/// The <see cref="IGrainDirectory"/> contract, on one engine and flavour. The first five tests are those of Orleans'
/// <c>GrainDirectoryTests</c> base (MIT, .NET Foundation), which Orleans does not ship as a test kit.
/// </summary>
public abstract class GrainDirectoryConformanceTests(DatabaseFixture fixture)
{
    private static readonly SiloAddress SiloA = SiloAddress.FromParsableString("10.0.23.12:1000@5678");
    private static readonly SiloAddress SiloB = SiloAddress.FromParsableString("10.0.23.14:1000@4583");

    private readonly string clusterId = $"cluster-{Guid.NewGuid():N}";
    private RelationalGrainDirectory? directory;

    private static CancellationToken CancellationToken => TestContext.Current.CancellationToken;

    private RelationalGrainDirectory Directory => directory ??= Create("default");

    [Fact]
    public async Task RegisterLookupUnregisterLookup()
    {
        var expected = Address(NewGrain(), SiloA);

        Assert.Equal(expected, await (await StartedAsync()).Register(expected, null, CancellationToken));
        Assert.Equal(expected, await Directory.Lookup(expected.GrainId, CancellationToken));

        await Directory.Unregister(expected, CancellationToken);

        Assert.Null(await Directory.Lookup(expected.GrainId, CancellationToken));
    }

    [Fact]
    public async Task DoNotOverwriteEntry()
    {
        var expected = Address(NewGrain(), SiloA);
        var differentActivation = Address(expected.GrainId, SiloA);
        var differentSilo = Copy(expected, silo: SiloB);

        await StartedAsync();
        Assert.Equal(expected, await Directory.Register(expected, null, CancellationToken));
        Assert.Equal(expected, await Directory.Register(differentActivation, null, CancellationToken));
        Assert.Equal(expected, await Directory.Register(differentSilo, null, CancellationToken));
        Assert.Equal(expected, await Directory.Lookup(expected.GrainId, CancellationToken));
    }

    [Fact]
    public async Task OverwriteEntryIfMatch()
    {
        var initial = Address(NewGrain(), SiloA);
        var differentActivation = Address(initial.GrainId, SiloA);
        var differentSilo = Copy(initial, silo: SiloB);

        await StartedAsync();
        Assert.Equal(initial, await Directory.Register(initial, differentSilo, CancellationToken));
        Assert.Equal(differentActivation, await Directory.Register(differentActivation, initial, CancellationToken));
        Assert.Equal(differentActivation, await Directory.Register(differentSilo, initial, CancellationToken));
        Assert.Equal(differentActivation, await Directory.Lookup(initial.GrainId, CancellationToken));
    }

    [Fact]
    public async Task DoNotDeleteDifferentActivationIdEntry()
    {
        var expected = Address(NewGrain(), SiloA);
        var otherEntry = Address(expected.GrainId, SiloA);

        await StartedAsync();
        Assert.Equal(expected, await Directory.Register(expected, null, CancellationToken));
        await Directory.Unregister(otherEntry, CancellationToken);
        Assert.Equal(expected, await Directory.Lookup(expected.GrainId, CancellationToken));
    }

    [Fact]
    public async Task LookupNotFound() =>
        Assert.Null(await (await StartedAsync()).Lookup(NewGrain(), CancellationToken));

    [Fact]
    public async Task Register_KeepsTheMembershipVersion()
    {
        var address = Copy(Address(NewGrain(), SiloA), version: new MembershipVersion(long.MaxValue - 1));
        await (await StartedAsync()).Register(address, CancellationToken);

        Assert.Equal(address.MembershipVersion, (await Directory.Lookup(address.GrainId, CancellationToken))!.MembershipVersion);
    }

    [Fact]
    public async Task Register_FromManyActivationsAtOnce_HasOneWinnerForAll()
    {
        var grainId = NewGrain();
        var candidates = Enumerable.Range(0, 16).Select(i => Address(grainId, i % 2 == 0 ? SiloA : SiloB)).ToArray();
        await StartedAsync();

        var results = await Task.WhenAll(candidates.Select(c => Directory.Register(c, CancellationToken)));

        var winner = Assert.Single(results.Distinct());
        Assert.Contains(winner, candidates);
        Assert.Equal(winner, await Directory.Lookup(grainId, CancellationToken));
    }

    [Fact]
    public async Task Directories_AndClusters_DoNotSeeEachOthersEntries()
    {
        var grainId = NewGrain();
        await StartedAsync();
        var named = Create("named");
        var otherCluster = FlavourDirectory.Create(fixture.Database, "default", $"other-{Guid.NewGuid():N}", TestSchemaMode.None);
        await named.InitializeAsync(CancellationToken);
        await otherCluster.InitializeAsync(CancellationToken);
        var mine = Address(grainId, SiloA);

        await Directory.Register(mine, CancellationToken);
        await named.UnregisterSilos([SiloA], CancellationToken);
        await otherCluster.UnregisterSilos([SiloA], CancellationToken);

        Assert.Null(await named.Lookup(grainId, CancellationToken));
        Assert.Null(await otherCluster.Lookup(grainId, CancellationToken));
        Assert.Equal(mine, await Directory.Lookup(grainId, CancellationToken));
    }

    [Fact]
    public async Task UnregisterSilos_RemovesEveryEntryOfThoseSilosOnly_AcrossBatches()
    {
        await StartedAsync();
        var onA = Enumerable.Range(0, RelationalGrainDirectory.SiloBatchSize * 2 + 7).Select(_ => Address(NewGrain(), SiloA)).ToArray();
        var onB = Enumerable.Range(0, 5).Select(_ => Address(NewGrain(), SiloB)).ToArray();
        foreach (var chunk in onA.Concat(onB).Chunk(16))
        {
            await Task.WhenAll(chunk.Select(a => Directory.Register(a, CancellationToken)));
        }

        await Directory.UnregisterSilos([SiloA], CancellationToken);

        Assert.All(await Task.WhenAll(onA.Take(20).Concat(onA.TakeLast(20)).Select(a => Directory.Lookup(a.GrainId, CancellationToken))), Assert.Null);
        Assert.Equal(onB, await Task.WhenAll(onB.Select(a => Directory.Lookup(a.GrainId, CancellationToken))));
        Assert.Equal(onB.Length, Convert.ToInt32(await ScalarAsync($"SELECT COUNT(*) FROM {GrainDirectorySchema.TableName} WHERE cluster_id = '{clusterId}'"), System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A replacement moves one of a dead silo's entries to a live silo while the cleanup waits on that row: the cleanup
    /// must not delete the live registration. On PostgreSQL, a delete of keys selected beforehand would.
    /// </summary>
    [Fact]
    public async Task UnregisterSilos_SparesAnEntryMovedToALiveSiloWhileItWaits()
    {
        var moved = Address(NewGrain(), SiloA);
        var stays = Address(NewGrain(), SiloA);
        await (await StartedAsync()).Register(moved, CancellationToken);
        await Directory.Register(stays, CancellationToken);
        var replacement = Address(moved.GrainId, SiloB);
        var entry = DirectoryEntry.From(replacement);

        await using var connection = await fixture.Database.DataSource.OpenConnectionAsync(CancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(CancellationToken);
        await using (var update = connection.CreateCommand())
        {
            update.Transaction = transaction;
            update.CommandText = $"UPDATE {GrainDirectorySchema.TableName} SET silo_address = @silo_address, silo_hash = @silo_hash, activation_id = @activation_id WHERE key_hash = @key_hash";
            AddParameter(update, "silo_address", entry.SiloAddress, DbType.String);
            AddParameter(update, "silo_hash", entry.SiloHash, DbType.Binary);
            AddParameter(update, "activation_id", entry.ActivationId, DbType.Binary);
            AddParameter(update, "key_hash", Key(moved.GrainId).Hash, DbType.Binary);
            Assert.Equal(1, await update.ExecuteNonQueryAsync(CancellationToken));
        }

        // On another thread: Microsoft.Data.Sqlite blocks the calling thread while it waits for a lock.
        var cleanup = Task.Run(() => Directory.UnregisterSilos([SiloA], CancellationToken), CancellationToken);
        await Task.Delay(500, CancellationToken);
        Assert.False(cleanup.IsCompleted, "The cleanup did not wait for the replacement, so this test exercised nothing.");
        await transaction.CommitAsync(CancellationToken);
        await cleanup;

        Assert.Equal(replacement, await Directory.Lookup(moved.GrainId, CancellationToken));
        Assert.Null(await Directory.Lookup(stays.GrainId, CancellationToken));
    }

    /// <summary>
    /// A batch whose selected entries all moved to a live silo before the delete deletes nothing, while more of the dead
    /// silo's entries remain: the cleanup must go on until none are left.
    /// </summary>
    [Fact]
    public async Task UnregisterSilos_AfterABatchThatDeletedNothing_RemovesTheRest()
    {
        var store = new FirstBatchMovedStore(FlavourDirectory.CreateStore(fixture.Database, TestSchemaMode.Apply));
        var cleaning = FlavourDirectory.Create(store, "default", clusterId);
        await cleaning.InitializeAsync(CancellationToken);
        var onA = Enumerable.Range(0, 5).Select(_ => Address(NewGrain(), SiloA)).ToArray();
        foreach (var address in onA)
        {
            await cleaning.Register(address, CancellationToken);
        }

        await cleaning.UnregisterSilos([SiloA], CancellationToken);

        Assert.True(store.EmptyBatchReported);
        Assert.All(await Task.WhenAll(onA.Select(a => cleaning.Lookup(a.GrainId, CancellationToken))), Assert.Null);
    }

    [Fact]
    public async Task UnregisterSilos_WithNoSilos_DoesNothing()
    {
        var address = Address(NewGrain(), SiloA);
        await (await StartedAsync()).Register(address, CancellationToken);

        await Directory.UnregisterSilos([], CancellationToken);

        Assert.Equal(address, await Directory.Lookup(address.GrainId, CancellationToken));
    }

    [Fact]
    public async Task LongAndBinaryGrainIds_RoundTrip()
    {
        await StartedAsync();
        foreach (var grainId in new[] { GrainId.Create(new string('t', 300), new string('k', 10_000)), GrainId.Create(new GrainType([0xFF, 0x00, 0xC3]), new IdSpan([0x00, 0xFE])) })
        {
            var address = Address(grainId, SiloA);

            Assert.Equal(address, await Directory.Register(address, CancellationToken));
            Assert.Equal(address, await Directory.Lookup(grainId, CancellationToken));
        }
    }

    [Fact]
    public async Task Operations_InsideAnAmbientTransactionThatRollsBack_StayCommitted()
    {
        var address = Address(NewGrain(), SiloA);
        await StartedAsync();
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await Directory.Register(address, CancellationToken);
        }

        Assert.Equal(address, await Directory.Lookup(address.GrainId, CancellationToken));
    }

    /// <summary>
    /// Orleans' <c>AdoNetGrainDirectory_ChaosTest</c>: 10,000 overlapping lifecycles on 10 grains from 10 silos, each
    /// registering, looking up, and unregistering whatever registration won. Every winner unregisters itself, so the
    /// directory must end empty: an entry left behind would point at an activation that no longer exists.
    /// </summary>
    [Fact]
    public async Task ChaosTest_OverlappingLifecycles_LeaveTheDirectoryEmpty()
    {
        await StartedAsync();
        var grains = Enumerable.Range(0, 10).Select(i => GrainId.Create("MyGrainType", $"MyGrainKey{i}-{Guid.NewGuid():N}")).ToArray();
        var silos = Enumerable.Range(0, 10).Select(i => SiloAddress.FromParsableString($"127.0.0.{i}:11111@123456")).ToArray();

        // SQLite runs one writer at a time, so there the test can only show that writes queue: a smaller run, which stays
        // within its busy timeout on a loaded host. The server engines, where writes truly overlap, run the full test.
        var (iterations, concurrency) = fixture.Engine == "Sqlite" ? (1_000, 4) : (10_000, 16);
        await Parallel.ForAsync(0, iterations, new ParallelOptions { MaxDegreeOfParallelism = concurrency, CancellationToken = CancellationToken }, async (_, ct) =>
        {
            var grainId = grains[Random.Shared.Next(grains.Length)];
            var registered = await Directory.Register(Address(grainId, silos[Random.Shared.Next(silos.Length)]), ct);
            for (var lookups = Random.Shared.Next(10); lookups > 0; lookups--)
            {
                await Directory.Lookup(grainId, ct);
            }

            if (registered is not null)
            {
                await Directory.Unregister(registered, ct);
            }
        });

        Assert.Equal(0, Convert.ToInt32(await ScalarAsync($"SELECT COUNT(*) FROM {GrainDirectorySchema.TableName} WHERE cluster_id = '{clusterId}'"), System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A row under one grain's key holding another grain's identity is what a 128-bit hash collision would leave.
    /// Every operation on the colliding grain fails or matches nothing, and the other grain's entry is left as it was.
    /// </summary>
    [Fact]
    public async Task HashCollision_NeitherReturnsNorReplacesTheOtherGrainsEntry()
    {
        var victim = Address(NewGrain(), SiloA);
        var intruder = Address(NewGrain(), SiloB);
        await (await StartedAsync()).Register(victim, CancellationToken);
        await ExecuteAsync(
            $"UPDATE {GrainDirectorySchema.TableName} SET key_hash = @intruder WHERE key_hash = @victim",
            ("intruder", Key(intruder.GrainId).Hash),
            ("victim", Key(victim.GrainId).Hash));

        await Assert.ThrowsAsync<GrainDirectoryKeyCollisionException>(() => Directory.Lookup(intruder.GrainId, CancellationToken));
        await Assert.ThrowsAsync<GrainDirectoryKeyCollisionException>(() => Directory.Register(intruder, CancellationToken));
        await Assert.ThrowsAsync<GrainDirectoryKeyCollisionException>(() => Directory.Register(intruder, victim, CancellationToken));
        await Directory.Unregister(Copy(victim, grainId: intruder.GrainId), CancellationToken);

        var row = await ScalarAsync($"SELECT COUNT(*) FROM {GrainDirectorySchema.TableName} WHERE cluster_id = '{clusterId}' AND silo_address = '{SiloA.ToParsableString()}'");
        Assert.Equal(1, Convert.ToInt32(row, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Many grains registered, replaced, looked up and unregistered at once, as silos do while activations move. On MySQL
    /// this is where upserts and gap locks deadlock; every call must succeed, and every grain must end with an entry one
    /// of its activations registered.
    /// </summary>
    [Fact]
    public async Task ConcurrentRegisterLookupUnregister_NeverFailsAndKeepsOneEntryPerGrain()
    {
        await StartedAsync();
        var grains = Enumerable.Range(0, 16).Select(_ => NewGrain()).ToArray();
        var registered = new ConcurrentDictionary<GrainId, ConcurrentBag<GrainAddress>>();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(4));

        var workers = Enumerable.Range(0, 16).Select(worker => Task.Run(async () =>
        {
            var random = new Random(worker);
            var operations = 0;
            while (!stop.IsCancellationRequested)
            {
                var grainId = grains[random.Next(grains.Length)];
                var candidate = Address(grainId, random.Next(2) == 0 ? SiloA : SiloB);
                registered.GetOrAdd(grainId, _ => []).Add(candidate);
                switch (random.Next(4))
                {
                    case 0:
                        await Directory.Register(candidate, CancellationToken);
                        break;
                    case 1:
                        var current = await Directory.Lookup(grainId, CancellationToken);
                        await Directory.Register(candidate, current, CancellationToken);
                        break;
                    case 2:
                        if (await Directory.Lookup(grainId, CancellationToken) is { } existing)
                        {
                            await Directory.Unregister(existing, CancellationToken);
                        }

                        break;
                    default:
                        await Directory.Lookup(grainId, CancellationToken);
                        break;
                }

                operations++;
            }

            return operations;
        })).ToArray();

        Assert.True((await Task.WhenAll(workers)).Sum() > 0);
        foreach (var grainId in grains)
        {
            if (await Directory.Lookup(grainId, CancellationToken) is { } entry)
            {
                Assert.Contains(entry, registered[grainId]);
            }
        }
    }

    private async Task<RelationalGrainDirectory> StartedAsync()
    {
        await Directory.InitializeAsync(CancellationToken);
        return Directory;
    }

    private RelationalGrainDirectory Create(string name) =>
        FlavourDirectory.Create(fixture.Database, name, clusterId, TestSchemaMode.Apply);

    private DirectoryKey Key(GrainId grainId) => DirectoryKey.Create(new DirectoryScope(clusterId, "default"), grainId);

    private static GrainId NewGrain() => GrainId.Parse("user/somerandomuser_" + Guid.NewGuid().ToString("N"), System.Globalization.CultureInfo.InvariantCulture);

    private static GrainAddress Address(GrainId grainId, SiloAddress silo) => new()
    {
        ActivationId = ActivationId.NewId(),
        GrainId = grainId,
        SiloAddress = silo,
        MembershipVersion = new MembershipVersion(51),
    };

    private static GrainAddress Copy(GrainAddress address, GrainId? grainId = null, SiloAddress? silo = null, MembershipVersion? version = null) => new()
    {
        ActivationId = address.ActivationId,
        GrainId = grainId ?? address.GrainId,
        SiloAddress = silo ?? address.SiloAddress,
        MembershipVersion = version ?? address.MembershipVersion,
    };

    /// <summary>
    /// Reports the first batch as having deleted nothing, as when every entry it selected moved to a live silo first.
    /// </summary>
    private sealed class FirstBatchMovedStore(IGrainDirectoryStore inner) : IGrainDirectoryStore
    {
        public bool EmptyBatchReported { get; private set; }

        public Task InitializeAsync(CancellationToken cancellationToken) => inner.InitializeAsync(cancellationToken);

        public Task<StoredDirectoryEntry?> ReadAsync(DirectoryKey key, CancellationToken cancellationToken) => inner.ReadAsync(key, cancellationToken);

        public Task<bool> TryInsertAsync(DirectoryKey key, DirectoryEntry entry, CancellationToken cancellationToken) => inner.TryInsertAsync(key, entry, cancellationToken);

        public Task<bool> TryReplaceAsync(DirectoryKey key, byte[] expectedActivationId, DirectoryEntry entry, CancellationToken cancellationToken) =>
            inner.TryReplaceAsync(key, expectedActivationId, entry, cancellationToken);

        public Task<bool> TryDeleteAsync(DirectoryKey key, byte[] expectedActivationId, CancellationToken cancellationToken) =>
            inner.TryDeleteAsync(key, expectedActivationId, cancellationToken);

        public Task<int> DeleteSiloBatchAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, int limit, CancellationToken cancellationToken)
        {
            if (!EmptyBatchReported)
            {
                EmptyBatchReported = true;
                return Task.FromResult(0);
            }

            return inner.DeleteSiloBatchAsync(scopeHash, siloHash, siloAddress, limit, cancellationToken);
        }

        public Task<bool> HasSiloEntriesAsync(byte[] scopeHash, byte[] siloHash, string siloAddress, CancellationToken cancellationToken) =>
            inner.HasSiloEntriesAsync(scopeHash, siloHash, siloAddress, cancellationToken);
    }

    private static void AddParameter(System.Data.Common.DbCommand command, string name, object value, DbType type)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        parameter.DbType = type;
        command.Parameters.Add(parameter);
    }

    private async Task<object?> ScalarAsync(string sql)
    {
        await using var command = fixture.Database.DataSource.CreateCommand(sql);
        return await command.ExecuteScalarAsync(CancellationToken);
    }

    private async Task ExecuteAsync(string sql, params (string Name, byte[] Value)[] parameters)
    {
        await using var command = fixture.Database.DataSource.CreateCommand(sql);
        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.DbType = DbType.Binary;
            command.Parameters.Add(parameter);
        }

        await command.ExecuteNonQueryAsync(CancellationToken);
    }
}
