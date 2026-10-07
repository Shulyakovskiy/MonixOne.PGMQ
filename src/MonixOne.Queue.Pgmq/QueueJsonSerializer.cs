using System.Reflection;
using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Nodes;

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
            sendOptions.Source,
            sendOptions.CorrelationId,
            // Trace identity is taken from the active W3C Activity, independently of caller metadata.
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

        // Reject poison messages before acquisition so they follow the bounded retry/DLQ path.
        if (envelope.IdempotencyKey.Length > 512)
            throw new QueueException("Queue message idempotency key must not exceed 512 characters.");

        return envelope;
    }

    public string Serialize<T>(T value) => JsonSerializer.Serialize(value, _options);

    public QueueDeadLetterMessage DeserializeDeadLetter(long id, string body)
    {
        var entry = JsonSerializer.Deserialize<QueueDeadLetterMessage>(body, _options)
            ?? throw new QueueException("Dead-letter message is empty.");
        return entry with
        {
            Id = id,
            OriginalMessage = entry.OriginalMessage ?? body,
            Queue = entry.Queue ?? "unknown",
            Topic = entry.Topic ?? GetMessageType(entry.OriginalMessage) ?? "unknown",
            ExceptionType = entry.ExceptionType ?? string.Empty,
            ErrorMessage = entry.ErrorMessage ?? string.Empty
        };
    }

    internal string NormalizeLegacyDeadLetter(string body, string sourceQueue)
    {
        // Preserve the complete failure payload; only add routing metadata missing in old entries.
        var entry = JsonNode.Parse(body) as JsonObject ?? new JsonObject { ["originalMessage"] = body };
        entry["queue"] ??= sourceQueue;
        entry["topic"] ??= GetMessageType(entry["originalMessage"]?.GetValue<string>()) ?? "unknown";
        return entry.ToJsonString(_options);
    }

    internal string? GetMessageType(string? body)
    {
        if (body is null)
            return null;
        try
        {
            using var document = JsonDocument.Parse(body);
            return document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty("type", out var type)
                && type.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(type.GetString())
                ? type.GetString() : null;
        }
        catch (JsonException) { return null; }
    }
}
