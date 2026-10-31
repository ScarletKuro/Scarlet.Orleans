using System.Transactions;
using Orleans.Messaging;
using Orleans.Runtime;

namespace Scarlet.Orleans.Clustering;

/// <summary>
/// Lists a cluster's gateways for clients: its active silos that accept client connections.
/// </summary>
/// <remarks>
/// Clients never change the schema; they only check it, or not at all, as configured on the store.
/// </remarks>
internal sealed class RelationalGatewayListProvider(string clusterId, IMembershipStore store, TimeSpan maxStaleness) : IGatewayListProvider
{
    private readonly ClusterScope scope = new(clusterId);

    public TimeSpan MaxStaleness => maxStaleness;

    [Obsolete("This attribute is no longer used and all providers are considered updatable")]
    public bool IsUpdatable => true;

    public async Task InitializeGatewayListProvider()
    {
        using var transaction = SuppressAmbientTransaction();
        await store.InitializeAsync(CancellationToken.None).ConfigureAwait(false);
    }

    public async Task<IList<Uri>> GetGateways()
    {
        using var transaction = SuppressAmbientTransaction();
        var gateways = await store.ReadGatewaysAsync(scope, CancellationToken.None).ConfigureAwait(false);
        var uris = new List<Uri>(gateways.Count);
        foreach (var gateway in gateways)
        {
            if (!scope.OwnsMember(gateway.Identity))
            {
                throw new MembershipKeyCollisionException(
                    $"A member row found under {scope} belongs to a different cluster. This is a 128-bit hash collision; please report it.");
            }

            var silo = SiloAddress.FromParsableString(gateway.SiloAddress);
            uris.Add(SiloAddress.New(silo.Endpoint.Address, gateway.ProxyPort, silo.Generation).ToGatewayUri());
        }

        return uris;
    }

    private static TransactionScope SuppressAmbientTransaction() =>
        new(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
}
