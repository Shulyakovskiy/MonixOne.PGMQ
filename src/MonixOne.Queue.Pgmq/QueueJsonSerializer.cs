using System.Reflection;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace MonixOne.Queue.Pgmq;

internal sealed class QueueJsonSerializer
{
    private readonly JsonSerializerOptions _options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public string Serialize<T>(T message, QueueSendOptions sendOptions)
    {
        ArgumentNullException.ThrowIfNull(sendOptions);
        ArgumentException.ThrowIfNullOrWhiteSpace(sendOptions.IdempotencyKey);
        if (sendOptions.IdempotencyKey.Length > 512)
        {
            throw new ArgumentOutOfRangeException(nameof(sendOptions), "IdempotencyKey must not exceed 512 characters.");
        }

        var attribute = typeof(T).GetCustomAttribute<QueueMessageAttribute>();
        var envelope = new QueueEnvelope<T>(
            Guid.CreateVersion7(),
            sendOptions.IdempotencyKey,
            attribute?.Type ?? typeof(T).FullName ?? typeof(T).Name,
            attribute?.Version ?? 1,
            sendOptions?.Source,
            sendOptions?.CorrelationId,
            // Trace identity is taken from the active W3C Activity rather than a caller-controlled option.
            // CorrelationId remains application metadata and is intentionally stored separately.
            Activity.Current?.TraceId.ToString(),
            DateTimeOffset.UtcNow,
            message);
        return JsonSerializer.Serialize(envelope, _options);
    }

    public QueueEnvelope<T> Deserialize<T>(string body)
    {
        var envelope = JsonSerializer.Deserialize<QueueEnvelope<T>>(body, _options)
            ?? throw new QueueException("Queue message envelope is empty.");
        if (string.IsNullOrWhiteSpace(envelope.IdempotencyKey))
        {
            throw new QueueException("Queue message idempotency key is required.");
        }

        return envelope;
    }

    public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);
}
