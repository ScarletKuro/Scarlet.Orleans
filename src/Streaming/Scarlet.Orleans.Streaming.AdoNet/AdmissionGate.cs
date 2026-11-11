namespace Scarlet.Orleans.Streaming.AdoNet;

/// <summary>
/// Admits operations until closed, and lets the closer wait until the admitted ones have finished.
/// </summary>
internal sealed class AdmissionGate
{
    private readonly Lock gate = new();
    private readonly TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int active;
    private bool closed;

    /// <summary>Admits an operation, unless the gate is closed. An admitted operation must call <see cref="Exit"/>.</summary>
    public bool TryEnter()
    {
        lock (gate)
        {
            if (closed)
            {
                return false;
            }

            active++;
            return true;
        }
    }

    public void Exit()
    {
        lock (gate)
        {
            if (--active == 0 && closed)
            {
                drained.TrySetResult();
            }
        }
    }

    /// <summary>Admits nothing more, and completes when every admitted operation has exited.</summary>
    public Task CloseAsync()
    {
        lock (gate)
        {
            closed = true;
            if (active == 0)
            {
                drained.TrySetResult();
            }

            return drained.Task;
        }
    }
}
