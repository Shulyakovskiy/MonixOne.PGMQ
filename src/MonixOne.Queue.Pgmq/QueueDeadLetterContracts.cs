namespace MonixOne.Queue.Pgmq;

/// <summary>
/// Filters and cursor for inspecting the shared dead-letter queue without claiming deliveries.
/// </summary>
public sealed class QueueDeadLetterQuery
{
    /// <summary>
    /// Optional original source queue name. Omit to include all source queues.
    /// </summary>
    public string? Queue { get; init; }
    /// <summary>
    /// Optional stable message type from QueueMessageAttribute. Omit to include all topics.
    /// </summary>
    public string? Topic { get; init; }
    /// <summary>
    /// Return entries with shared DLQ identifiers greater than this value.
    /// Zero starts at the beginning.
    /// </summary>
    public long AfterId { get; init; }
    /// <summary>
    /// Maximum returned entries, from 1 to 500. Each call releases its database connection.
    /// </summary>
    public int PageSize { get; init; } = 100;
}

/// <summary>
/// A page ordered by shared DLQ identifier. A null cursor means this query has no further entries.
/// </summary>
/// <param name="Messages">
/// Failed deliveries from any source queue and topic matching the query.
/// </param>
/// <param name="NextAfterId">
/// Pass as AfterId with the same filters to request the next page.
/// </param>
public sealed record QueueDeadLetterPage(IReadOnlyList<QueueDeadLetterMessage> Messages, long? NextAfterId);

/// <summary>
/// A failed delivery stored in the shared dead-letter queue, including routing and failure details.
/// </summary>
/// <param name="Id">
/// Unique PGMQ identifier in the shared DLQ, used for pagination.
/// </param>
/// <param name="OriginalMessage">
/// Original serialized application envelope, retained even when envelope validation fails.
/// </param>
/// <param name="Queue">
/// Original source queue name.
/// </param>
/// <param name="Topic">
/// Stable message type from the envelope or handler contract.
/// Unknown for unidentifiable legacy data.
/// </param>
/// <param name="ConsumerName">
/// Name of the consumer that exhausted retries; may be absent in legacy entries.
/// </param>
/// <param name="MessageId">
/// Original source queue's PGMQ identifier; only unique together with Queue.
/// </param>
/// <param name="DeliveryCount">
/// Number of source-queue deliveries at the time of failure.
/// </param>
/// <param name="FailedAt">
/// UTC time when failure handling persisted the entry.
/// </param>
/// <param name="ExceptionType">
/// Fully qualified exception type reported by the worker.
/// </param>
/// <param name="ErrorMessage">
/// Exception message reported by the worker.
/// </param>
public sealed record QueueDeadLetterMessage(
    long Id,
    string OriginalMessage,
    string Queue,
    string Topic,
    string? ConsumerName,
    long MessageId,
    int DeliveryCount,
    DateTimeOffset FailedAt,
    string ExceptionType,
    string ErrorMessage);
