namespace MonixOne.Queue.Pgmq;

internal sealed record QueueEnvelope<T>(
    Guid Id,
    string Type,
    int Version,
    string? Source,
    string? CorrelationId,
    string? TraceId,
    DateTimeOffset CreatedAt,
    T Payload);

internal sealed record PgmqMessage(
    long Id,
    int ReadCount,
    string Body);
