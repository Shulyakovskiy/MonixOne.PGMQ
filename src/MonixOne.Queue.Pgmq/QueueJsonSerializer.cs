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

    public string Serialize<T>(T message, QueueSendOptions? sendOptions)
    {
        var attribute = typeof(T).GetCustomAttribute<QueueMessageAttribute>();
        var envelope = new QueueEnvelope<T>(
            Guid.CreateVersion7(),
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

    public QueueEnvelope<T> Deserialize<T>(string body) =>
        JsonSerializer.Deserialize<QueueEnvelope<T>>(body, _options)
        ?? throw new QueueException("Queue message envelope is empty.");

    public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);
}
