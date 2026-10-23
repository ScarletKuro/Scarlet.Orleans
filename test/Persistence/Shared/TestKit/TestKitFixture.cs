using Orleans.Hosting;
using Orleans.Persistence.TestKit;
using Orleans.Runtime;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.Tests;

/// <summary>
/// Hosts the storage under test in a silo for the Orleans persistence TestKit, configured through the package's public
/// hosting extensions, on a database of <paramref name="database"/>'s engine.
/// </summary>
public abstract class TestKitFixture(DatabaseFixture database, bool deleteStateOnClear) : GrainStorageTestFixture, IAsyncLifetime
{
    public DatabaseFixture DatabaseFixture { get; } = database;

    public bool DeleteStateOnClear { get; } = deleteStateOnClear;

    /// <summary>
    /// The storage under test, which skips the calling test when the engine is not configured.
    /// </summary>
    public IGrainStorage TestStorage => new RequiredEngineStorage(this);

    protected override string StorageProviderName => "TestKit";

    protected override void ConfigureSilo(ISiloBuilder siloBuilder) =>
        FlavourStorage.AddStorage(siloBuilder, StorageProviderName, DatabaseFixture.Database, DeleteStateOnClear);

    public override async ValueTask InitializeAsync()
    {
        await DatabaseFixture.InitializeAsync();
        if (DatabaseFixture.IsAvailable)
        {
            await base.InitializeAsync();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await DatabaseFixture.DisposeAsync();
    }

    private sealed class RequiredEngineStorage(TestKitFixture fixture) : IGrainStorage
    {
        private IGrainStorage Storage
        {
            get
            {
                fixture.DatabaseFixture.Require();
                return fixture.Storage;
            }
        }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => Storage.ReadStateAsync(stateName, grainId, grainState);

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken) =>
            Storage.ReadStateAsync(stateName, grainId, grainState, cancellationToken);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => Storage.WriteStateAsync(stateName, grainId, grainState);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken) =>
            Storage.WriteStateAsync(stateName, grainId, grainState, cancellationToken);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) => Storage.ClearStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState, CancellationToken cancellationToken) =>
            Storage.ClearStateAsync(stateName, grainId, grainState, cancellationToken);
    }
}
