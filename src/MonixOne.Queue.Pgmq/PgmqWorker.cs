using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqWorker<T>(
    string consumerName,
    PgmqClient client,
    QueueJsonSerializer serializer,
    IServiceScopeFactory scopeFactory,
    IOptions<PgmqOptions> options,
    ILogger<PgmqWorker<T>> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = GetConsumer();
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
        try
        {
            envelope = serializer.Deserialize<T>(message.Body);
            // A scope per message isolates DbContext and other scoped dependencies across concurrent deliveries.
            await using var scope = scopeFactory.CreateAsyncScope();
            var handler = scope.ServiceProvider.GetRequiredService<IQueueHandler<T>>();
            await handler.HandleAsync(envelope.Payload, cancellationToken);
            // Deletion is the acknowledgement. A failure between HandleAsync and delete can redeliver the message,
            // so handlers must be idempotent and should use the envelope id as their deduplication key.
            await client.DeleteAsync(consumer.Queue, message.Id, cancellationToken);
            logger.LogDebug("PGMQ message processed. Queue {Queue}, MessageId {MessageId}, MessageType {MessageType}, DeliveryCount {DeliveryCount}, DurationMs {DurationMs}.", consumer.Queue, envelope.Id, envelope.Type, message.ReadCount, stopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            // pgmq.read has already made this message invisible; a failed handler must not acknowledge it.
            logger.LogError(exception, "PGMQ message processing failed. Queue {Queue}, MessageId {MessageId}, MessageType {MessageType}, DeliveryCount {DeliveryCount}, DurationMs {DurationMs}.", consumer.Queue, envelope?.Id, envelope?.Type, message.ReadCount, stopwatch.ElapsedMilliseconds);
            if (message.ReadCount >= consumer.MaxAttempts)
            {
                var deadLetter = new DeadLetterMessage(message.Body, consumer.Queue, message.Id, message.ReadCount, DateTimeOffset.UtcNow, exception.GetType().FullName ?? exception.GetType().Name, exception.Message);
                await PersistFailureAsync(
                    () => client.MoveToDeadLetterAsync(consumer.Queue, message.Id, $"{consumer.Queue}-dlq", serializer.Serialize(deadLetter), cancellationToken),
                    consumer,
                    message,
                    cancellationToken);
                return;
            }

            var delayIndex = Math.Min(message.ReadCount - 1, consumer.RetryDelays.Count - 1);
            var retryDelay = delayIndex >= 0 ? consumer.RetryDelays[delayIndex] : consumer.PollingInterval;
            await PersistFailureAsync(
                () => client.SetVisibilityAsync(consumer.Queue, message.Id, retryDelay, cancellationToken),
                consumer,
                message,
                cancellationToken);
        }
    }

    private PgmqConsumerOptions GetConsumer() => options.Value.Consumers.TryGetValue(consumerName, out var consumer)
        ? consumer
        : throw new InvalidOperationException($"Queue consumer configuration '{consumerName}' was not found.");

    private async Task PersistFailureAsync(
        Func<Task> operation,
        PgmqConsumerOptions consumer,
        PgmqMessage message,
        CancellationToken cancellationToken)
    {
        try
        {
            await operation();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            // The message stays unacknowledged and will reappear after its original visibility timeout.
            logger.LogError(exception, "PGMQ failure state could not be persisted. Queue {Queue}, MessageId {MessageId}.", consumer.Queue, message.Id);
        }
    }

    private sealed record DeadLetterMessage(string OriginalMessage, string Queue, long MessageId, int DeliveryCount, DateTimeOffset FailedAt, string ExceptionType, string ErrorMessage);
}
