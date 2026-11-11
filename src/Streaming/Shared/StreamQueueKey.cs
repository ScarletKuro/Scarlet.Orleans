using Scarlet.Orleans.Relational;

namespace Scarlet.Orleans.Streaming;

/// <summary>
/// The storage identity of one stream queue: its exact bytes, and the 128-bit hash that keys its rows.
/// </summary>
/// <remarks>
/// The identity is the service ID, stream provider name and queue ID (<see cref="StorageIdentity"/>). Message rows carry
/// only the hash; the queue row keeps the identity, and a receiver compares it when it starts, so a hash collision fails
/// loudly instead of mixing two queues' messages.
/// </remarks>
internal sealed class StreamQueueKey
{
    private StreamQueueKey(byte[] identity, string serviceId, string providerName, string queueId)
    {
        Identity = identity;
        Hash = StorageIdentity.Hash(identity);
        ServiceId = serviceId;
        ProviderName = providerName;
        QueueId = queueId;
    }

    public byte[] Identity { get; }

    public byte[] Hash { get; }

    public string ServiceId { get; }

    public string ProviderName { get; }

    public string QueueId { get; }

    /// <param name="serviceId">The cluster's service ID.</param>
    /// <param name="providerName">The stream provider's name.</param>
    /// <param name="queueId">The queue's ID as Orleans formats it.</param>
    public static StreamQueueKey Create(string serviceId, string providerName, string queueId) =>
        new(
            StorageIdentity.Encode(StorageIdentity.StrictUtf8.GetBytes(serviceId), StorageIdentity.StrictUtf8.GetBytes(providerName), StorageIdentity.StrictUtf8.GetBytes(queueId)),
            serviceId,
            providerName,
            queueId);

    /// <summary>The service ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string ServiceIdText => StorageIdentity.DisplayText(ServiceId);

    /// <summary>The provider name as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string ProviderNameText => StorageIdentity.DisplayText(ProviderName);

    /// <summary>The queue ID as storable display text (<see cref="StorageIdentity.DisplayText"/>).</summary>
    public string QueueIdText => StorageIdentity.DisplayText(QueueId);

    public override string ToString() => $"ServiceId={ServiceId} Provider={ProviderName} Queue={QueueId}";
}
