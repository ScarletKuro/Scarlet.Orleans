using System.Data.Common;

namespace Scarlet.Orleans.Relational;

/// <summary>
/// Retries a single-statement operation the server rejected as transient.
/// </summary>
/// <remarks>
/// Callers pass only operations that run as one autocommitted statement, so a transient error proves nothing was
/// applied and repeating the whole operation is safe. Multi-statement operations must roll back explicitly before
/// retrying, because engines differ in how much of a transaction an error undoes (design section 4.7).
/// </remarks>
internal static class TransientRetry
{
    private const int MaxAttempts = 4;
    private static readonly TimeSpan BaseDelay = TimeSpan.FromMilliseconds(20);

    public static async Task<T> ExecuteAsync<T>(
        TimeProvider timeProvider,
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (attempt < MaxAttempts && ServerError(exception) is { } error && DbErrors.IsTransient(error))
            {
                Diagnostics.RecordRetry(error.SqlState ?? error.GetType().Name);

                // Full jitter keeps concurrent losers of the same deadlock from colliding again in lockstep.
                var delay = BaseDelay * (1 << attempt) * Random.Shared.NextDouble();
                await Task.Delay(delay, timeProvider, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The server error behind <paramref name="exception"/>, which EF Core wraps in <c>DbUpdateException</c>.
    /// </summary>
    private static DbException? ServerError(Exception exception) => exception as DbException ?? exception.InnerException as DbException;
}
