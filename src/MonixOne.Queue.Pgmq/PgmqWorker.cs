using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqWorker<T>(
    string consumerName,
    PgmqClient client,
    PgmqIdempotencyStore idempotencyStore,
    QueueJsonSerializer serializer,
    IServiceScopeFactory scopeFactory,
    IOptions<PgmqOptions> options,
    ILogger<PgmqWorker<T>> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = GetConsumer();
        logger.LogInformation(
            "PGMQ worker started. Worker {Worker}, Queue {Queue}, BatchSize {BatchSize}, Concurrency {Concurrency}.",
            consumerName,
            consumer.Queue,
            consumer.BatchSize,
            consumer.Concurrency);
        while (!stoppingToken.IsCancellationRequested)
        {
            IReadOnlyList<PgmqMessage> messages;
            try
            {
                // pgmq.read claims messages before handlers run. VisibilityTimeout must therefore cover normal
                // handler execution; this package deliberately does not extend an in-flight lease automatically.
                messages = await client.ReadAsync(consumer.Queue, (int)Math.Ceiling(consumer.VisibilityTimeout.TotalSeconds), consumer.BatchSize, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "PGMQ read failed for queue {Queue} and worker {Worker}.", consumer.Queue, consumerName);
                await Task.Delay(consumer.PollingInterval, stoppingToken);
                continue;
            }

            if (messages.Count == 0)
            {
                await Task.Delay(consumer.PollingInterval, stoppingToken);
                continue;
            }

            logger.LogDebug(
                "PGMQ messages received. Worker {Worker}, Queue {Queue}, Count {Count}.",
                consumerName,
                consumer.Queue,
                messages.Count);

            await Parallel.ForEachAsync(messages, new ParallelOptions
            {
                CancellationToken = stoppingToken,
                MaxDegreeOfParallelism = consumer.Concurrency
            }, (message, cancellationToken) => ProcessAsync(consumer, message, cancellationToken));
        }
    }

    private async ValueTask ProcessAsync(
        PgmqConsumerOptions consumer,
        PgmqMessage message,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        QueueEnvelope<T>? envelope = null;
        Guid? leaseToken = null;
        try
        {
            envelope = serializer.Deserialize<T>(message.Body);
            var claim = await idempotencyStore.TryAcquireAsync(
                consumerName,
                envelope.IdempotencyKey,
                consumer.IdempotencyLease,
                cancellationToken);
            if (claim.IsCompleted)
            {
                // A previous delivery completed this logical message. Acknowledge only the duplicate PGMQ delivery.
                if (await TryQueueOperationAsync(
                        "acknowledgement of a completed duplicate",
                        () => client.DeleteAsync(consumer.Queue, message.Id, cancellationToken),
                        consumer.Queue,
                        message.Id,
                        cancellationToken))
                {
                    logger.LogDebug("PGMQ duplicate skipped. Queue {Queue}, IdempotencyKey {IdempotencyKey}, MessageId {MessageId}.", consumer.Queue, envelope.IdempotencyKey, message.Id);
                }
                return;
            }

            if (claim.IsInProgress)
            {
                var retryDelay = claim.LeaseExpiresAt is { } leaseExpiresAt
                    ? leaseExpiresAt - DateTime.UtcNow
                    : consumer.PollingInterval;
                if (await TryQueueOperationAsync(
                    "visibility update for a concurrent delivery",
                    () => client.SetVisibilityAsync(
                        consumer.Queue,
                        message.Id,
                        retryDelay > TimeSpan.Zero ? retryDelay : consumer.PollingInterval,
                        cancellationToken),
                    consumer.Queue,
                    message.Id,
                    cancellationToken))
                {
                    logger.LogDebug(
                        "PGMQ concurrent delivery deferred. Worker {Worker}, Queue {Queue}, IdempotencyKey {IdempotencyKey}, MessageId {MessageId}, RetryDelayMs {RetryDelayMs}.",
                        consumerName,
                        consumer.Queue,
                        envelope.IdempotencyKey,
                        message.Id,
                        retryDelay.TotalMilliseconds);
                }
                return;
            }

            leaseToken = claim.LeaseToken
                ?? throw new QueueException($"The idempotency claim for key '{envelope.IdempotencyKey}' has no lease token.");
            // A scope per message isolates DbContext and other scoped dependencies across concurrent deliveries.
            await using var scope = scopeFactory.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetRequiredService<IQueueHandler<T>>();
            await handler.HandleAsync(envelope.Payload, cancellationToken);
            if (!await idempotencyStore.CompleteAsync(consumerName, envelope.IdempotencyKey, leaseToken.Value, cancellationToken))
            {
                throw new QueueException($"The idempotency lease was lost for key '{envelope.IdempotencyKey}'.");
            }

            // Completion is durable. From this point an acknowledgement failure must not enter retry or DLQ handling.
            leaseToken = null;
            if (await TryQueueOperationAsync(
                    "acknowledgement of a completed message",
                    () => client.DeleteAsync(consumer.Queue, message.Id, cancellationToken),
                    consumer.Queue,
                    message.Id,
                    cancellationToken))
            {
                logger.LogDebug("PGMQ message processed. Queue {Queue}, MessageId {MessageId}, MessageType {MessageType}, DeliveryCount {DeliveryCount}, DurationMs {DurationMs}.", consumer.Queue, envelope.Id, envelope.Type, message.ReadCount, stopwatch.ElapsedMilliseconds);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryReleaseLeaseAsync(envelope, leaseToken);
            throw;
        }
        catch (Exception exception)
        {
            await TryReleaseLeaseAsync(envelope, leaseToken);
            await HandleFailureAsync(consumer, message, envelope, exception, stopwatch.ElapsedMilliseconds, cancellationToken);
        }
    }

    private PgmqConsumerOptions GetConsumer() => options.Value.Consumers.TryGetValue(consumerName, out var consumer)
        ? consumer
        : throw new InvalidOperationException($"Queue consumer configuration '{consumerName}' was not found.");

    private async Task HandleFailureAsync(
        PgmqConsumerOptions consumer,
        PgmqMessage message,
        QueueEnvelope<T>? envelope,
        Exception exception,
        long durationMilliseconds,
        CancellationToken cancellationToken)
    {
        // pgmq.read has already made this message invisible; a failed handler must not acknowledge it.
        logger.LogError(exception, "PGMQ message processing failed. Queue {Queue}, MessageId {MessageId}, MessageType {MessageType}, DeliveryCount {DeliveryCount}, DurationMs {DurationMs}.", consumer.Queue, envelope?.Id, envelope?.Type, message.ReadCount, durationMilliseconds);
        if (message.ReadCount >= consumer.MaxAttempts)
        {
            var deadLetter = new DeadLetterMessage(
                message.Body,
                consumer.Queue,
                message.Id,
                message.ReadCount,
                DateTimeOffset.UtcNow,
                exception.GetType().FullName ?? exception.GetType().Name,
                exception.Message);
            if (await TryQueueOperationAsync(
                "dead-letter persistence",
                () => client.MoveToDeadLetterAsync(
                    consumer.Queue,
                    message.Id,
                    $"{consumer.Queue}-dlq",
                    serializer.Serialize(deadLetter),
                    cancellationToken),
                consumer.Queue,
                message.Id,
                cancellationToken))
            {
                logger.LogWarning(
                    "PGMQ message moved to DLQ. Queue {Queue}, MessageId {MessageId}, DeliveryCount {DeliveryCount}, MaxAttempts {MaxAttempts}.",
                    consumer.Queue,
                    message.Id,
                    message.ReadCount,
                    consumer.MaxAttempts);
            }
            return;
        }

        var delayIndex = Math.Min(message.ReadCount - 1, consumer.RetryDelays.Count - 1);
        var retryDelay = delayIndex >= 0 ? consumer.RetryDelays[delayIndex] : consumer.PollingInterval;
        if (await TryQueueOperationAsync(
            "retry scheduling",
            () => client.SetVisibilityAsync(consumer.Queue, message.Id, retryDelay, cancellationToken),
            consumer.Queue,
            message.Id,
            cancellationToken))
        {
            logger.LogWarning(
                "PGMQ message retry scheduled. Queue {Queue}, MessageId {MessageId}, DeliveryCount {DeliveryCount}, RetryDelayMs {RetryDelayMs}.",
                consumer.Queue,
                message.Id,
                message.ReadCount,
                retryDelay.TotalMilliseconds);
        }
    }

    private async Task TryReleaseLeaseAsync(QueueEnvelope<T>? envelope, Guid? leaseToken)
    {
        if (envelope is null || leaseToken is not { } token) return;

        try
        {
            await idempotencyStore.ReleaseAsync(consumerName, envelope.IdempotencyKey, token);
        }
        catch (Exception exception)
        {
            // The lease expires on its own; a cleanup failure must not hide the original processing error.
            logger.LogError(exception, "PGMQ idempotency lease release failed. Worker {Worker}, IdempotencyKey {IdempotencyKey}.", consumerName, envelope.IdempotencyKey);
        }
    }

    private async Task<bool> TryQueueOperationAsync(
        string operationName,
        Func<Task> operation,
        string queue,
        long messageId,
        CancellationToken cancellationToken)
    {
        try
        {
            await operation();
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            // The message stays unacknowledged and will reappear after its original visibility timeout.
            logger.LogError(exception, "PGMQ {OperationName} failed. Queue {Queue}, MessageId {MessageId}.", operationName, queue, messageId);
            return false;
        }
    }

    private sealed record DeadLetterMessage(
        string OriginalMessage,
        string Queue,
        long MessageId,
        int DeliveryCount,
        DateTimeOffset FailedAt,
        string ExceptionType,
        string ErrorMessage);
}
