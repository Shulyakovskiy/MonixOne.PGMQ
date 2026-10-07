using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Data.Common;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqQueue(
    PgmqClient client,
    QueueJsonSerializer serializer,
    ILogger<PgmqQueue> logger,
    IOptions<PgmqOptions> options) : IQueue
{
    public async Task<QueueDeadLetterPage> GetDeadLetterMessagesAsync(
        QueueDeadLetterQuery? query = null, CancellationToken cancellationToken = default)
    {
        query ??= new QueueDeadLetterQuery();
        ArgumentOutOfRangeException.ThrowIfNegative(query.AfterId);
        if (query.PageSize is < 1 or > 500)
            throw new ArgumentOutOfRangeException(nameof(query), "DLQ PageSize must be between 1 and 500.");
        if (query.Queue is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(query.Queue);
        if (query.Topic is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(query.Topic);
        cancellationToken.ThrowIfCancellationRequested();

        var rows = await client.GetDeadLetterMessagesAsync(options.Value.DeadLetterQueue, query, cancellationToken);
        var messages = new List<QueueDeadLetterMessage>(Math.Min(rows.Count, query.PageSize));
        foreach (var row in rows.Take(query.PageSize))
        {
            cancellationToken.ThrowIfCancellationRequested();
            messages.Add(serializer.DeserializeDeadLetter(row.Id, row.Body));
        }
        return new QueueDeadLetterPage(messages,
            rows.Count > query.PageSize ? messages[^1].Id : null);
    }

    public async Task<long> SendAsync<T>(
        string queue, T message,
        QueueSendOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        cancellationToken.ThrowIfCancellationRequested();
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
        cancellationToken.ThrowIfCancellationRequested();
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
        var bodies = SerializeBatch(messages, cancellationToken);
        if (bodies.Length == 0)
            return;
        // PGMQ 1.13 supports atomic batch insertion through a single command and pooled connection.
        await client.SendBatchAsync(queue, bodies, cancellationToken);
        logger.LogDebug("PGMQ batch sent. Queue {Queue}, Count {Count}.", queue, bodies.Length);
    }

    public async Task SendBatchAsync<T>(
        string queue,
        IReadOnlyCollection<QueueBatchItem<T>> messages,
        DbTransaction transaction,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queue);
        ArgumentNullException.ThrowIfNull(transaction);
        var bodies = SerializeBatch(messages, cancellationToken);
        if (bodies.Length == 0)
            return;
        await client.SendBatchAsync(queue, bodies, transaction, cancellationToken);
        logger.LogDebug("PGMQ transactional batch send added. Queue {Queue}, Count {Count}.", queue, bodies.Length);
    }

    private string[] SerializeBatch<T>(IReadOnlyCollection<QueueBatchItem<T>> messages, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        cancellationToken.ThrowIfCancellationRequested();
        var bodies = new List<string>(messages.Count);
        foreach (var item in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bodies.Add(serializer.Serialize(item.Message, item.Options));
        }
        return bodies.ToArray();
    }
}
