using Orleans;
using Orleans.Concurrency;
using Orleans.Transactions.Abstractions;

namespace Scarlet.Orleans.Transactions.Benchmarks;

public interface IAccountGrain : IGrainWithIntegerKey
{
    /// <summary>A transaction on this account alone, which Orleans prepares and commits in one store.</summary>
    [Transaction(TransactionOption.Create)]
    Task AddAsync(int amount);

    /// <summary>Joins the caller's transaction, such as a transfer between two accounts.</summary>
    [Transaction(TransactionOption.Join)]
    Task ChangeAsync(int amount);

    [Transaction(TransactionOption.Create)]
    [ReadOnly]
    Task<int> GetAsync();
}

[GenerateSerializer]
public sealed class Balance
{
    [Id(0)]
    public int Value { get; set; }

    /// <summary>The rest of a realistic state, which every stored copy repeats.</summary>
    [Id(1)]
    public byte[] Data { get; set; } = new byte[Storages.StateSize];
}

/// <summary>
/// The same account on each provider: each class names its own storage, since the storage of a transactional state is
/// chosen by the grain.
/// </summary>
public abstract class AccountGrain(ITransactionalState<Balance> balance) : Grain, IAccountGrain
{
    public Task AddAsync(int amount) => balance.PerformUpdate(b => b.Value += amount);

    public Task ChangeAsync(int amount) => balance.PerformUpdate(b => b.Value += amount);

    public Task<int> GetAsync() => balance.PerformRead(b => b.Value);
}

public sealed class ScarletAdoNetAccount([TransactionalState("balance", Storages.ScarletAdoNet)] ITransactionalState<Balance> balance) : AccountGrain(balance);

public sealed class ScarletEntityFrameworkCoreAccount([TransactionalState("balance", Storages.ScarletEntityFrameworkCore)] ITransactionalState<Balance> balance) : AccountGrain(balance);

public sealed class ScarletWrapperAccount([TransactionalState("balance", Storages.ScarletWrapper)] ITransactionalState<Balance> balance) : AccountGrain(balance);

public sealed class InBoxWrapperAccount([TransactionalState("balance", Storages.InBoxWrapper)] ITransactionalState<Balance> balance) : AccountGrain(balance);

internal static class Storages
{
    public const int StateSize = 1024;

    public const string ScarletAdoNet = "Scarlet ADO.NET";

    public const string ScarletEntityFrameworkCore = "Scarlet EF Core";

    public const string ScarletWrapper = "Wrapper on Scarlet persistence";

    public const string InBoxWrapper = "Wrapper on Orleans ADO.NET persistence";

    public static string GrainClass(string provider) => provider switch
    {
        ScarletAdoNet => typeof(ScarletAdoNetAccount).FullName!,
        ScarletEntityFrameworkCore => typeof(ScarletEntityFrameworkCoreAccount).FullName!,
        ScarletWrapper => typeof(ScarletWrapperAccount).FullName!,
        _ => typeof(InBoxWrapperAccount).FullName!,
    };
}
