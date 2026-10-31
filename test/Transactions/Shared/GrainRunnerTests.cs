using Microsoft.Extensions.DependencyInjection;
using Orleans.Transactions.TestKit.xUnit;

namespace Scarlet.Orleans.Transactions.Tests;

// Orleans' transaction runners, driving TestKit grains whose states use the storage under test.

public abstract class GoldenPathTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : GoldenPathTransactionTestRunnerxUnit(fixture.GrainFactory, output);

public abstract class GrainFaultTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : GrainFaultTransactionTestRunnerxUnit(fixture.GrainFactory, output);

public abstract class TocGoldenPathTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : TocGoldenPathTestRunnerxUnit(fixture.GrainFactory, output);

public abstract class TocFaultTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : TocFaultTransactionTestRunnerxUnit(fixture.GrainFactory, output);

public abstract class ConcurrencyTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : TransactionConcurrencyTestRunnerxUnit(fixture.GrainFactory, output);

public abstract class ExclusiveLockTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : ExclusiveLockTransactionTestRunnerxUnit(fixture.GrainFactory, output);

public abstract class ScopedTransactionTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : ScopedTransactionsTestRunnerxUnit(fixture.GrainFactory, fixture.ClientServices.GetRequiredService<global::Orleans.ITransactionClient>(), output);

/// <summary>
/// The randomized consistency runner: about a hundred workloads, minutes per engine, so it runs only when
/// <c>SCARLET_TEST_CONSISTENCY</c> is <c>1</c>, as upstream runs it nightly.
/// </summary>
public abstract class ConsistencyTests(TransactionsClusterFixture fixture, ITestOutputHelper output)
    : ConsistencyTransactionTestRunnerxUnit(RequireOptIn(fixture), output)
{
    // Every store is one database transaction with no size limit, and no faults are injected.
    protected override bool StorageAdaptorHasLimitedCommitSpace => false;

    protected override bool StorageErrorInjectionActive => false;

    private static global::Orleans.IGrainFactory RequireOptIn(TransactionsClusterFixture fixture)
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable("SCARLET_TEST_CONSISTENCY") == "1", "Set SCARLET_TEST_CONSISTENCY=1 to run the consistency workloads.");
        return fixture.GrainFactory;
    }
}
