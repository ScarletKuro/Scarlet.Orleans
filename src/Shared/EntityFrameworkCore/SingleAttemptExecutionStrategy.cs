using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Scarlet.Orleans.Relational.EntityFrameworkCore;

/// <summary>
/// Runs an operation exactly once. EF Core does not retry operations nested in a running execution strategy, so the
/// strategy configured on the context is suspended for the operation.
/// </summary>
/// <remarks>
/// Writes use it because a repeat after a connection failure would run after an attempt of unknown outcome. Writes are
/// repeated only when the server reported that the attempt was not applied (<see cref="TransientRetry"/>).
/// </remarks>
internal sealed class SingleAttemptExecutionStrategy(DbContext context) : ExecutionStrategy(context, maxRetryCount: 0, maxRetryDelay: TimeSpan.Zero)
{
    protected override bool ShouldRetryOn(Exception exception) => false;
}
