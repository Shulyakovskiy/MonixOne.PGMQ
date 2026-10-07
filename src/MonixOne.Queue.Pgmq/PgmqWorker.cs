using System.Diagnostics;
using System.Reflection;
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
    IHostApplicationLifetime applicationLifetime,
    IOptions<PgmqOptions> options,
    ILogger<PgmqWorker<T>> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = GetConsumer();
        // Claim only immediately executable messages; no local batch backlog spends their
        // visibility budget.
        var readBatchSize = Math.Min(consumer.BatchSize, consumer.Concurrency);
        logger.LogInformation(
            "PGMQ worker started. Worker {Worker}, Queue {Queue}, BatchSize {BatchSize}, Concurrency {Concurrency}, MaxProcessingDuration {MaxProcessingDuration}.",
            consumerName, consumer.Queue, readBatchSize, consumer.Concurrency, consumer.MaxProcessingDuration);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                IReadOnlyList<PgmqMessage> messages;
                try
                {
                    using var readCancellation = CreateOperationCancellation(consumer, stoppingToken);
                    messages = await client.ReadAsync(consumer.Queue,
                        (int)Math.Ceiling(consumer.VisibilityTimeout.TotalSeconds), readBatchSize, readCancellation.Token);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
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

                logger.LogDebug("PGMQ messages received. Worker {Worker}, Queue {Queue}, Count {Count}.",
                    consumerName, consumer.Queue, messages.Count);
                await Parallel.ForEachAsync(messages, new ParallelOptions
                {
                    CancellationToken = stoppingToken,
                    MaxDegreeOfParallelism = consumer.Concurrency
                }, (message, cancellationToken) => ProcessAsync(consumer, message, cancellationToken));
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
    }

    private async ValueTask ProcessAsync(PgmqConsumerOptions consumer, PgmqMessage message, CancellationToken stoppingToken)
    {
        // Use monotonic time for local cancellation and database time for persisted visibility;
        // host clock skew cannot extend ownership.
        var remaining = consumer.MaxProcessingDuration - Stopwatch.GetElapsedTime(message.ReadStartedTimestamp);
        if (remaining <= TimeSpan.Zero)
            return;
        var deadline = message.ReadAt + consumer.MaxProcessingDuration;
        using var attemptCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        attemptCancellation.CancelAfter(remaining);
        var attemptToken = attemptCancellation.Token;
        QueueEnvelope<T>? envelope = null;
        Guid? leaseToken = null;
        try
        {
            attemptToken.ThrowIfCancellationRequested();
            envelope = serializer.Deserialize<T>(message.Body);
            IdempotencyClaim claim;
            try
            {
                using var acquireCancellation = CreateOperationCancellation(consumer, attemptToken);
                claim = await idempotencyStore.TryAcquireAsync(consumerName, envelope.IdempotencyKey,
                    consumer.IdempotencyLease, acquireCancellation.Token);
            }
            catch (OperationCanceledException) when (attemptToken.IsCancellationRequested) { throw; }
            catch (Exception exception)
            {
                // No handler ran and ownership is unknown. Leave this delivery for a later read.
                logger.LogError(exception, "PGMQ idempotency acquisition failed. Worker {Worker}, Queue {Queue}, MessageId {MessageId}.",
                    consumerName, consumer.Queue, message.Id);
                return;
            }

            if (claim.IsCompleted)
            {
                await TryQueueOperationAsync("acknowledgement of a completed duplicate", consumer, message,
                    ct => client.TryDeleteCompletedAsync(consumer.Queue, message, consumerName, envelope.IdempotencyKey, ct), attemptToken);
                return;
            }
            if (claim.IsInProgress)
            {
                await TryQueueOperationAsync("visibility update for a concurrent delivery", consumer, message,
                    ct => client.TryDeferConcurrentAsync(consumer.Queue, message, consumerName, envelope.IdempotencyKey,
                        consumer.PollingInterval, ct), attemptToken);
                return;
            }

            leaseToken = claim.LeaseToken ?? throw new QueueException("The acquired idempotency claim has no lease token.");
            // Validate the physical delivery before dispatch; a slow read response may already have
            // lost visibility.
            await RenewAsync(attemptToken);
            await PgmqHandlerExecution.RunAsync(
                async ct =>
                {
                    ct.ThrowIfCancellationRequested();
                    // Scope disposal is part of the handler task, so a detached task never uses
                    // prematurely disposed dependencies.
                    await using var scope = scopeFactory.CreateAsyncScope();
                    var handler = scope.ServiceProvider.GetRequiredKeyedService<IQueueHandler<T>>(consumerName);
                    await handler.HandleAsync(envelope.Payload, ct);
                },
                RenewAsync, consumer.VisibilityRenewalInterval, consumer.HandlerCancellationGracePeriod, logger, attemptToken,
                consumer.QueueOperationTimeout);

            attemptToken.ThrowIfCancellationRequested();
            using (var completeCancellation = CreateOperationCancellation(consumer, attemptToken))
            {
                if (!await idempotencyStore.CompleteAsync(consumerName, envelope.IdempotencyKey, leaseToken.Value,
                        completeCancellation.Token, deadline))
                    throw new PgmqLeaseLostException();
            }

            // Completion is durable. A failed acknowledgement must never enter retry/DLQ handling.
            leaseToken = null;
            if (await TryQueueOperationAsync("acknowledgement of a completed message", consumer, message,
                    ct => client.TryDeleteCompletedAsync(consumer.Queue, message, consumerName, envelope.IdempotencyKey, ct), attemptToken))
                logger.LogDebug(
                    "PGMQ message processed. Worker {Worker}, Queue {Queue}, MessageId {MessageId}, MessageType {MessageType}, DeliveryCount {DeliveryCount}, DurationMs {DurationMs}.",
                    consumerName, consumer.Queue, message.Id, envelope.Type, message.ReadCount,
                    Stopwatch.GetElapsedTime(message.ReadStartedTimestamp).TotalMilliseconds);
        }
        catch (PgmqHandlerUnresponsiveException exception)
        {
            // Keep the lease: the task still owns its scope and may still perform an external side
            // effect.
            logger.LogCritical(exception, "PGMQ processing did not stop after cancellation. Stopping application. Worker {Worker}, Queue {Queue}, MessageId {MessageId}.",
                consumerName, consumer.Queue, message.Id);
            applicationLifetime.StopApplication();
            throw;
        }
        catch (PgmqLeaseLostException exception)
        {
            logger.LogWarning(exception, "PGMQ delivery ownership lost. Worker {Worker}, Queue {Queue}, MessageId {MessageId}.",
                consumerName, consumer.Queue, message.Id);
            // The handler is either unstarted or fully stopped. Release only our own claim; leave
            // the physical delivery untouched.
            if (envelope is not null && leaseToken is { } token)
                await TryReleaseOwnLeaseAsync(consumer, envelope.IdempotencyKey, token);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Handler and scope are already stopped. Shorten visibility and release ownership
            // atomically, using an independent cleanup budget.
            if (leaseToken is not null)
                await TryQueueOperationAsync("release on shutdown", consumer, message,
                    ct => client.TryRetryAsync(consumer.Queue, message, consumerName, envelope!.IdempotencyKey,
                        leaseToken, TimeSpan.Zero, ct), CancellationToken.None);
        }
        catch (Exception exception)
        {
            // Completed or never acquired; no failure transition.
            if (envelope is not null && leaseToken is null)
                return;
            if (exception is OperationCanceledException && attemptToken.IsCancellationRequested)
                exception = new TimeoutException($"PGMQ processing exceeded {consumer.MaxProcessingDuration}.", exception);
            // A deadline cancels the attempt token, but retry/DLQ cleanup still needs its own short
            // cancellation budget.
            await HandleFailureAsync(consumer, message, envelope, leaseToken, exception);
        }

        async Task RenewAsync(CancellationToken ct)
        {
            try
            {
                using var renewalCancellation = CreateOperationCancellation(consumer, ct);
                if (!await client.TryRenewVisibilityAsync(consumer.Queue, message, consumerName, envelope!.IdempotencyKey,
                        leaseToken!.Value, consumer.VisibilityTimeout, deadline, renewalCancellation.Token))
                    throw new PgmqLeaseLostException();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch (PgmqLeaseLostException) { throw; }
            catch (Exception exception) { throw new PgmqLeaseLostException(exception); }
        }
    }

    private PgmqConsumerOptions GetConsumer() => options.Value.Consumers.TryGetValue(consumerName, out var consumer)
        ? consumer : throw new InvalidOperationException($"Queue consumer configuration '{consumerName}' was not found.");

    private async Task HandleFailureAsync(PgmqConsumerOptions consumer, PgmqMessage message,
        QueueEnvelope<T>? envelope, Guid? leaseToken, Exception exception)
    {
        logger.LogError(exception, "PGMQ message processing failed. Worker {Worker}, Queue {Queue}, MessageId {MessageId}, DeliveryCount {DeliveryCount}.",
            consumerName, consumer.Queue, message.Id, message.ReadCount);
        if (message.ReadCount >= consumer.MaxAttempts)
        {
            var topic = envelope?.Type ?? serializer.GetMessageType(message.Body)
                ?? typeof(T).GetCustomAttribute<QueueMessageAttribute>()?.Type ?? typeof(T).FullName ?? typeof(T).Name;
            var deadLetter = new DeadLetterMessage(message.Body, consumer.Queue, topic, consumerName, message.Id, message.ReadCount,
                DateTimeOffset.UtcNow, exception.GetType().FullName ?? exception.GetType().Name, exception.Message);
            if (await TryQueueOperationAsync("dead-letter persistence", consumer, message,
                    ct => client.TryMoveToDeadLetterAsync(consumer.Queue, options.Value.DeadLetterQueue, message, consumerName, envelope?.IdempotencyKey,
                        leaseToken, serializer.Serialize(deadLetter), ct), CancellationToken.None))
                logger.LogWarning("PGMQ message moved to DLQ. Queue {Queue}, MessageId {MessageId}, DeliveryCount {DeliveryCount}.",
                    consumer.Queue, message.Id, message.ReadCount);
            return;
        }

        var delayIndex = Math.Min(message.ReadCount - 1, consumer.RetryDelays.Count - 1);
        var retryDelay = delayIndex >= 0 ? consumer.RetryDelays[delayIndex] : consumer.PollingInterval;
        if (await TryQueueOperationAsync("retry scheduling", consumer, message,
                ct => client.TryRetryAsync(consumer.Queue, message, consumerName, envelope?.IdempotencyKey,
                    leaseToken, retryDelay, ct), CancellationToken.None))
            logger.LogWarning("PGMQ message retry scheduled. Queue {Queue}, MessageId {MessageId}, RetryDelay {RetryDelay}.",
                consumer.Queue, message.Id, retryDelay);
    }

    private async Task TryReleaseOwnLeaseAsync(PgmqConsumerOptions consumer, string key, Guid token)
    {
        try
        {
            using var cleanupCancellation = CreateOperationCancellation(consumer, CancellationToken.None);
            await idempotencyStore.ReleaseAsync(consumerName, key, token, cleanupCancellation.Token);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "PGMQ idempotency release failed. Worker {Worker}.", consumerName);
        }
    }

    private async Task<bool> TryQueueOperationAsync(string name, PgmqConsumerOptions consumer, PgmqMessage message,
        Func<CancellationToken, Task<bool>> operation, CancellationToken cancellationToken)
    {
        try
        {
            using var operationCancellation = CreateOperationCancellation(consumer, cancellationToken);
            var applied = await operation(operationCancellation.Token);
            if (!applied)
                logger.LogDebug("PGMQ {OperationName} skipped for a stale delivery. Queue {Queue}, MessageId {MessageId}.",
                    name, consumer.Queue, message.Id);
            return applied;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception exception)
        {
            logger.LogError(exception, "PGMQ {OperationName} failed. Queue {Queue}, MessageId {MessageId}.", name, consumer.Queue, message.Id);
            return false;
        }
    }

    private static CancellationTokenSource CreateOperationCancellation(PgmqConsumerOptions consumer, CancellationToken token)
    {
        // Every caller disposes this source. It limits pool waits as well as commands; it holds no
        // connection between operations.
        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        cancellation.CancelAfter(consumer.QueueOperationTimeout);
        return cancellation;
    }

    private sealed record DeadLetterMessage(string OriginalMessage, string Queue, string Topic, string ConsumerName,
        long MessageId, int DeliveryCount,
        DateTimeOffset FailedAt, string ExceptionType, string ErrorMessage);
}
