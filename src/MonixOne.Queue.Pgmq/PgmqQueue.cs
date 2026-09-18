namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqQueue(
    PgmqClient client,
    QueueJsonSerializer serializer) : IQueue
{
    public Task<long> SendAsync<T>(
        string queue, T message,
        QueueSendOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        return client.SendAsync(queue, serializer.Serialize(message, options), cancellationToken);
    }

    public async Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<QueueBatchItem<T>> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(messages);
        // PGMQ has no batch-send primitive. Keeping sends sequential preserves cancellation semantics and avoids
        // pretending that this operation is atomic: callers that need atomic domain-write plus enqueue use an outbox.
        foreach (var message in messages)
        {
            await client.SendAsync(
                queue,
                serializer.Serialize(message.Message, message.Options),
                cancellationToken);
        }
    }
}
