using System.Net;
using System.Net.Sockets;

namespace Scarlet.Orleans.Tests;

/// <summary>
/// A TCP relay between a driver and its database that loses responses: with <see cref="DropProbability"/>, it discards a
/// chunk the server sent and cuts the connection. A dropped response to a statement the server already ran, and for an
/// autocommitted statement committed, is a lost acknowledgement, the failure whose outcome the client cannot know.
/// </summary>
/// <remarks>
/// Bytes are relayed unchanged, including encrypted traffic. Network continuations must not require the caller's
/// synchronization context: a driver may synchronously wait for its login response on that context.
/// </remarks>
public sealed class ChaosProxy : IAsyncDisposable
{
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource stopping = new();
    private readonly string targetHost;
    private readonly int targetPort;
    private readonly Task accepting;
    private int drops;

    public ChaosProxy(string targetHost, int targetPort)
    {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        listener.Start();
        DatabaseDiagnostics.Record("proxy.begin", "TCP", $"{targetHost}:{targetPort}", $"listen={Port}");
        accepting = AcceptAsync();
    }

    public int Port => ((IPEndPoint)listener.LocalEndpoint).Port;

    /// <summary>The chance that a response chunk is dropped, cutting its connection. Zero relays everything.</summary>
    public double DropProbability { get; set; }

    public int Drops => Volatile.Read(ref drops);

    public async ValueTask DisposeAsync()
    {
        await stopping.CancelAsync().ConfigureAwait(false);
        listener.Stop();
        try
        {
            await accepting.ConfigureAwait(false);
        }
        catch (Exception)
        {
        }

        stopping.Dispose();
        DatabaseDiagnostics.Record("proxy.end", "TCP", $"{targetHost}:{targetPort}", $"drops={Drops}");
    }

    private async Task AcceptAsync()
    {
        while (!stopping.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await listener.AcceptTcpClientAsync(stopping.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                return;
            }

            _ = RelayAsync(client);
        }
    }

    private async Task RelayAsync(TcpClient client)
    {
        using (client)
        using (var server = new TcpClient())
        {
            try
            {
                await server.ConnectAsync(targetHost, targetPort, stopping.Token).ConfigureAwait(false);
                DatabaseDiagnostics.Record("proxy.connected", "TCP", $"{targetHost}:{targetPort}");
                using var cut = CancellationTokenSource.CreateLinkedTokenSource(stopping.Token);
                var requests = PumpAsync(client.GetStream(), server.GetStream(), lossy: false, cut);
                var responses = PumpAsync(server.GetStream(), client.GetStream(), lossy: true, cut);
                await Task.WhenAny(requests, responses).ConfigureAwait(false);
                await cut.CancelAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                DatabaseDiagnostics.Record("proxy.error", "TCP", $"{targetHost}:{targetPort}", exception.ToString());
                // The driver sees a broken connection, which is the point.
            }
        }
    }

    private async Task PumpAsync(NetworkStream from, NetworkStream to, bool lossy, CancellationTokenSource cut)
    {
        var buffer = new byte[16 * 1024];
        long forwarded = 0;
        try
        {
            while (true)
            {
                var read = await from.ReadAsync(buffer, cut.Token).ConfigureAwait(false);
                if (read == 0)
                {
                    return;
                }

                if (lossy && Random.Shared.NextDouble() < DropProbability)
                {
                    Interlocked.Increment(ref drops);
                    return;
                }

                await to.WriteAsync(buffer.AsMemory(0, read), cut.Token).ConfigureAwait(false);
                forwarded += read;
            }
        }
        catch (Exception exception)
        {
            if (!cut.IsCancellationRequested)
                DatabaseDiagnostics.Record("proxy.pump.error", "TCP", $"{targetHost}:{targetPort}", exception.ToString());
        }
        finally
        {
            DatabaseDiagnostics.Record("proxy.forwarded", "TCP", $"{targetHost}:{targetPort}", $"direction={(lossy ? "response" : "request")} bytes={forwarded}");
        }
    }
}
