using Microsoft.Extensions.Logging;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqQueue(
    PgmqClient client,
    QueueJsonSerializer serializer,
    ILogger<PgmqQueue> logger) : IQueue
{
    public async Task<long> SendAsync<T>(
        string queue, T message,
        QueueSendOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        var messageId = await client.SendAsync(queue, serializer.Serialize(message, options), cancellationToken);
        logger.LogDebug(
            "PGMQ message sent. Queue {Queue}, MessageId {MessageId}, MessageType {MessageType}.",
            queue,
            messageId,
            typeof(T).FullName ?? typeof(T).Name);
        return messageId;
    }

    public async Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<QueueBatchItem<T>> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(messages);
        logger.LogDebug("PGMQ batch send started. Queue {Queue}, Count {Count}.", queue, messages.Count);
        // PGMQ has no batch-send primitive. Keeping sends sequential preserves cancellation semantics and avoids
        // pretending that this operation is atomic: callers that need atomic domain-write plus enqueue use an outbox.
        foreach (var message in messages)
        {
            await client.SendAsync(
                queue,
                serializer.Serialize(message.Message, message.Options),
                cancellationToken);
        }

        logger.LogDebug("PGMQ batch sent. Queue {Queue}, Count {Count}.", queue, messages.Count);
    }
}
