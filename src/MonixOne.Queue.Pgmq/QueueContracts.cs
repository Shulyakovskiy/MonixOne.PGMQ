namespace MonixOne.Queue.Pgmq;

public sealed record QueueMessage<T>(
    Guid Id,
    string Type,
    int Version,
    DateTimeOffset CreatedAt,
    T Payload);

public sealed class QueueSendOptions
{
    public required string IdempotencyKey { get; init; }
    public string? Source { get; init; }
    public string? CorrelationId { get; init; }
}

public sealed record QueueBatchItem<T>(T Message, QueueSendOptions Options);

/// <summary>
/// Processing limits shared by defaults and individual PGMQ consumers.
/// </summary>
public class QueueConsumerOptions
{
    /// <summary>
    /// Upper bound for a read. The worker claims at most <see cref="Concurrency"/> messages to
    /// avoid a local backlog.
    /// </summary>
    public int BatchSize { get; set; } = 10;
    /// <summary>
    /// Initial and renewed invisibility window. Must cover normal processing, publication retries
    /// and acknowledgement.
    /// </summary>
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromMinutes(1);
    /// <summary>
    /// Heartbeat period. This interval plus <see cref="QueueOperationTimeout"/> must be shorter
    /// than the visibility window.
    /// </summary>
    public TimeSpan VisibilityRenewalInterval { get; set; } = TimeSpan.FromSeconds(20);
    /// <summary>
    /// Maximum total time for one delivery attempt, including acquisition, handler execution and
    /// acknowledgement.
    /// </summary>
    /// <remarks>
    /// Measured from the read; visibility renewal never extends beyond this attempt's absolute
    /// deadline.
    /// </remarks>
    public TimeSpan MaxProcessingDuration { get; set; } = TimeSpan.FromMinutes(2);
    /// <summary>
    /// Maximum wait for a cancelled handler and its scope cleanup before requesting application
    /// shutdown.
    /// </summary>
    public TimeSpan HandlerCancellationGracePeriod { get; set; } = TimeSpan.FromSeconds(5);
    /// <summary>
    /// Cancellation budget for each queue operation, including connection-pool acquisition and SQL
    /// lock waits.
    /// </summary>
    public TimeSpan QueueOperationTimeout { get; set; } = TimeSpan.FromSeconds(5);
    /// <summary>
    /// Delay between reads when the queue is empty or a read fails.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);
    /// <summary>
    /// PGMQ delivery count at which a failed delivery is moved to the dead-letter queue.
    /// </summary>
    public int MaxAttempts { get; set; } = 5;
    /// <summary>
    /// Maximum number of handlers running concurrently in this worker instance.
    /// </summary>
    public int Concurrency { get; set; } = 1;
    /// <summary>
    /// Fixed ownership lease for a logical message; this lease is never renewed by heartbeat.
    /// </summary>
    /// <remarks>
    /// Must cover processing, the longer handler/heartbeat shutdown budget, and one cleanup
    /// operation. Zero derives that sum plus 20 seconds.
    /// </remarks>
    public TimeSpan IdempotencyLease { get; set; }
    /// <summary>
    /// Backoff delays selected by delivery count. The last delay is reused when the list is
    /// exhausted.
    /// </summary>
    public IReadOnlyList<TimeSpan> RetryDelays { get; set; } = [TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(10)];
}

public sealed class QueueException(
    string message,
    Exception? innerException = null) : Exception(message, innerException);

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class QueueMessageAttribute(string type) : Attribute
{
    public string Type { get; } = type;
    public int Version { get; init; } = 1;
}
