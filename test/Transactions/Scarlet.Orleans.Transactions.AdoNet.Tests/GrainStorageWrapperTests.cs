using Orleans.Hosting;

namespace Scarlet.Orleans.Transactions.Tests;

/// <summary>
/// A cluster where the transaction store is Scarlet grain storage instead: with no transactional state storage under
/// the name, Orleans stores transactional state through its grain storage wrapper, the way that works without this
/// package. The same runners cover both ways.
/// </summary>
public abstract class GrainStorageWrapperFixture(DatabaseFixture database) : TransactionsClusterFixture(database)
{
    protected override void ConfigureStorage(ISiloBuilder silo, string name) =>
        silo.AddScarletAdoNetGrainStorage(name, options =>
        {
            options.DataSource = Database.Database.DataSource;
            options.Engine = Database.Database.Engine == "MariaDb" ? "MariaDb" : null;
            options.SchemaMode = Persistence.AdoNet.SchemaMode.Apply;
        });
}
