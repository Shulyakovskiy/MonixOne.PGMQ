namespace MonixOne.Queue;

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

public class QueueConsumerOptions
{
    public int BatchSize { get; set; } = 10;
    // This is a delivery lease, not a handler timeout. A message can be delivered again when it expires.
    public TimeSpan VisibilityTimeout { get; set; } = TimeSpan.FromMinutes(1);
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);
    public int MaxAttempts { get; set; } = 5;
    public int Concurrency { get; set; } = 1;
    // Must exceed VisibilityTimeout, otherwise a redelivery can acquire an expired idempotency lease while the first handler is still running.
    public TimeSpan IdempotencyLease { get; set; }
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
