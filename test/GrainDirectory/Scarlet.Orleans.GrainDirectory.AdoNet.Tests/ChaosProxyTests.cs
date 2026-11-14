using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Scarlet.Orleans.GrainDirectory.Tests;

public sealed class ChaosProxyTests
{
    [Fact]
    public async Task Relay_ProgressesWithoutTheCallersSynchronizationContext()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var target = new TcpListener(IPAddress.Loopback, 0);
        target.Start();
        var echo = Task.Run(async () =>
        {
            using var connection = await target.AcceptTcpClientAsync(timeout.Token);
            var stream = connection.GetStream();
            var buffer = new byte[1];
            await stream.ReadExactlyAsync(buffer, timeout.Token);
            await stream.WriteAsync(buffer, timeout.Token);
        }, TestContext.Current.CancellationToken);

        // SqlClient can synchronously wait for a login response. The relay must progress independently of the
        // caller's context, even while that context is not dispatching work.
        var context = new QueuedContext();
        var original = SynchronizationContext.Current;
        ChaosProxy proxy;
        try
        {
            SynchronizationContext.SetSynchronizationContext(context);
            proxy = new ChaosProxy("127.0.0.1", ((IPEndPoint)target.LocalEndpoint).Port);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(original);
        }

        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, proxy.Port, timeout.Token);
            await client.GetStream().WriteAsync(new byte[] { 42 }, timeout.Token);
            var response = new byte[1];
            await client.GetStream().ReadExactlyAsync(response, timeout.Token);
            Assert.Equal(42, response[0]);
        }
        finally
        {
            var disposal = proxy.DisposeAsync();
            context.Drain();
            await disposal;
            await timeout.CancelAsync();
            try { await echo; }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested) { }
        }
    }

    private sealed class QueuedContext : SynchronizationContext
    {
        private readonly ConcurrentQueue<(SendOrPostCallback Callback, object? State)> pending = new();

        public override void Post(SendOrPostCallback d, object? state) => pending.Enqueue((d, state));

        public void Drain()
        {
            while (pending.TryDequeue(out var work)) work.Callback(work.State);
        }
    }
}
