using Microsoft.Extensions.Logging;
using System.Data.Common;

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

    public async Task<long> SendAsync<T>(
        string queue,
        T message,
        QueueSendOptions options,
        DbTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(transaction);
        var messageId = await client.SendAsync(
            queue,
            serializer.Serialize(message, options),
            transaction,
            cancellationToken);
        logger.LogDebug(
            "PGMQ message added to caller transaction. Queue {Queue}, MessageId {MessageId}, MessageType {MessageType}.",
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
        // PGMQ has no batch-send primitive. Keeping sends sequential preserves cancellation semantics.
        foreach (var message in messages)
        {
            await client.SendAsync(
                queue,
                serializer.Serialize(message.Message, message.Options),
                cancellationToken);
        }

        logger.LogDebug("PGMQ batch sent. Queue {Queue}, Count {Count}.", queue, messages.Count);
    }

    public async Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<QueueBatchItem<T>> messages,
        DbTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(transaction);
        logger.LogDebug("PGMQ transactional batch send started. Queue {Queue}, Count {Count}.", queue, messages.Count);

        foreach (var message in messages)
        {
            await client.SendAsync(
                queue,
                serializer.Serialize(message.Message, message.Options),
                transaction,
                cancellationToken);
        }

        logger.LogDebug("PGMQ transactional batch send added. Queue {Queue}, Count {Count}.", queue, messages.Count);
    }
}
