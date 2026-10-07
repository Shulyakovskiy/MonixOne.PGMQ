namespace MonixOne.Queue.Pgmq;

internal sealed record QueueEnvelope<T>(
    Guid Id,
    string IdempotencyKey,
    string Type,
    int Version,
    string? Source,
    string? CorrelationId,
    string? TraceId,
    DateTimeOffset CreatedAt,
    T Payload);

/// <summary>
/// A physical delivery and its database timestamps; ReadCount fences stale workers after
/// redelivery.
/// </summary>
/// <param name="Id">
/// PGMQ message identifier, stable across redeliveries.
/// </param>
/// <param name="ReadCount">
/// Delivery generation incremented atomically by pgmq.read.
/// </param>
/// <param name="Body">
/// Serialized application envelope.
/// </param>
/// <param name="ReadAt">
/// Database time when this delivery was claimed; anchors the absolute processing deadline.
/// </param>
/// <param name="VisibleUntil">
/// Visibility expiry returned by the read.
/// </param>
internal sealed record PgmqMessage(
    long Id,
    int ReadCount,
    string Body,
    DateTime ReadAt,
    DateTime VisibleUntil)
{
    /// <summary>
    /// Local monotonic timestamp captured before read connection acquisition, conservatively
    /// including its round trip.
    /// </summary>
    public long ReadStartedTimestamp { get; init; }
}
