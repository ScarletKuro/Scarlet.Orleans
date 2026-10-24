using Orleans;
using Orleans.Hosting;
using Orleans.Reminders.TestKit;

namespace Scarlet.Orleans.Reminders.Tests;

/// <summary>
/// Hosts the reminder table in a silo through the package's public hosting extensions, on a database of
/// <paramref name="database"/>'s engine.
/// </summary>
public abstract class ReminderServiceFixture(DatabaseFixture database) : ReminderTableTestFixture, IAsyncLifetime
{
    public DatabaseFixture DatabaseFixture { get; } = database;

    protected override void ConfigureSilo(ISiloBuilder siloBuilder) => FlavourTable.AddReminders(siloBuilder, DatabaseFixture.Database);

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

    /// <summary>
    /// Skips the calling test when the engine is not configured, then returns the deployed fixture.
    /// </summary>
    public ReminderServiceFixture Require()
    {
        DatabaseFixture.Require();
        return this;
    }
}

/// <summary>
/// The Orleans reminders TestKit's service conformance: grains register, update, list and unregister reminders through
/// the reminder service of a silo, which stores them in the table under test.
/// </summary>
public abstract class ReminderServiceTests(ReminderServiceFixture fixture)
    : ReminderServiceTestRunner(fixture.Require().GrainFactory, fixture.ReminderTable, "Scarlet")
{
    [Fact]
    public override Task ReminderService_RegisterLookupEnumerateAndUnregister() => base.ReminderService_RegisterLookupEnumerateAndUnregister();

    [Fact]
    public override Task ReminderService_UpdateReplacesScheduleAndETagWithoutDuplicate() => base.ReminderService_UpdateReplacesScheduleAndETagWithoutDuplicate();
}
