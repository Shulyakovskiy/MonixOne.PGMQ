using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Options;

namespace MonixOne.Queue.Pgmq;

public sealed class PgmqOptions
{
    /// <summary>
    /// Configuration section containing queue defaults and named consumers.
    /// </summary>
    public const string SectionName = "Queue";
    /// <summary>
    /// Upstream PGMQ SQL version bundled and managed by this package.
    /// </summary>
    public const string SupportedExtensionVersion = PgmqDeploymentScripts.PgmqVersion;
    /// <summary>
    /// Default shared dead-letter queue for every consumer in this queue database.
    /// </summary>
    public const string DefaultDeadLetterQueue = "monixone_dlq";
    /// <summary>
    /// Shared DLQ name. Entries retain their original source queue and stable message topic.
    /// </summary>
    public string DeadLetterQueue { get; set; } = DefaultDeadLetterQueue;
    /// <summary>
    /// Explicit queue database connection string; takes precedence over a named connection string.
    /// </summary>
    public string? ConnectionString { get; set; }
    /// <summary>
    /// Name resolved under ConnectionStrings when no explicit connection string is provided.
    /// </summary>
    public string ConnectionStringName { get; set; } = "Queue";
    /// <summary>
    /// Retention of completed logical-message keys to suppress delayed duplicate deliveries.
    /// </summary>
    public TimeSpan CompletedIdempotencyRetention { get; set; } = TimeSpan.FromDays(1);
    /// <summary>
    /// Interval between bounded cleanup passes for completed idempotency keys.
    /// </summary>
    public TimeSpan IdempotencyCleanupInterval { get; set; } = TimeSpan.FromHours(1);
    /// <summary>
    /// Maximum number of completed keys deleted by each cleanup transaction.
    /// </summary>
    public int IdempotencyCleanupBatchSize { get; set; } = 1_000;
    /// <summary>
    /// Processing settings inherited by named consumers unless overridden.
    /// </summary>
    public QueueConsumerOptions Defaults { get; set; } = new();
    /// <summary>
    /// Consumer configurations keyed by the name passed to AddQueueConsumer.
    /// </summary>
    public Dictionary<string, PgmqConsumerOptions> Consumers { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class PgmqConsumerOptions : QueueConsumerOptions
{
    public PgmqConsumerOptions()
    {
        // Zero distinguishes an omitted consumer value from QueueConsumerOptions' public defaults
        // during IConfiguration binding.
        BatchSize = 0;
        VisibilityTimeout = TimeSpan.Zero;
        VisibilityRenewalInterval = TimeSpan.Zero;
        MaxProcessingDuration = TimeSpan.Zero;
        HandlerCancellationGracePeriod = TimeSpan.Zero;
        QueueOperationTimeout = TimeSpan.Zero;
        PollingInterval = TimeSpan.Zero;
        MaxAttempts = 0;
        Concurrency = 0;
        IdempotencyLease = TimeSpan.Zero;
        RetryDelays = [];
    }

    /// <summary>
    /// Physical source queue name. Failed deliveries go to PgmqOptions.DeadLetterQueue.
    /// </summary>
    [Required]
    public string Queue { get; set; } = string.Empty;
    /// <summary>
    /// Configuration-friendly backoff values in seconds; overrides the inherited RetryDelays list.
    /// </summary>
    public List<int> RetryDelaysSeconds { get; set; } = [];

    internal void ApplyDefaults(QueueConsumerOptions defaults, TimeSpan defaultIdempotencyLease)
    {
        // Zero/empty values distinguish omitted settings until defaults are merged at startup.
        BatchSize = BatchSize == 0 ? defaults.BatchSize : BatchSize;
        VisibilityTimeout = VisibilityTimeout == TimeSpan.Zero ? defaults.VisibilityTimeout : VisibilityTimeout;
        VisibilityRenewalInterval = VisibilityRenewalInterval == TimeSpan.Zero ? defaults.VisibilityRenewalInterval : VisibilityRenewalInterval;
        MaxProcessingDuration = MaxProcessingDuration == TimeSpan.Zero ? defaults.MaxProcessingDuration : MaxProcessingDuration;
        HandlerCancellationGracePeriod = HandlerCancellationGracePeriod == TimeSpan.Zero ? defaults.HandlerCancellationGracePeriod : HandlerCancellationGracePeriod;
        QueueOperationTimeout = QueueOperationTimeout == TimeSpan.Zero ? defaults.QueueOperationTimeout : QueueOperationTimeout;
        PollingInterval = PollingInterval == TimeSpan.Zero ? defaults.PollingInterval : PollingInterval;
        MaxAttempts = MaxAttempts == 0 ? defaults.MaxAttempts : MaxAttempts;
        Concurrency = Concurrency == 0 ? defaults.Concurrency : Concurrency;
        // A consumer may increase its processing limit; derive its lease from its effective limits
        // in that case.
        IdempotencyLease = IdempotencyLease == TimeSpan.Zero ? defaultIdempotencyLease : IdempotencyLease;
        if (RetryDelaysSeconds.Count > 0)
        {
            RetryDelays = RetryDelaysSeconds.Select(seconds => TimeSpan.FromSeconds(seconds)).ToArray();
        }
        else if (RetryDelays.Count == 0)
        {
            RetryDelays = defaults.RetryDelays;
        }
    }
}

internal sealed class PgmqOptionsValidator : IValidateOptions<PgmqOptions>
{
    public ValidateOptionsResult Validate(string? name, PgmqOptions options)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            errors.Add("Queue connection string is required.");
        if (string.IsNullOrWhiteSpace(options.DeadLetterQueue))
            errors.Add("Queue:DeadLetterQueue is required.");
        if (options.CompletedIdempotencyRetention <= TimeSpan.Zero)
            errors.Add("Queue:CompletedIdempotencyRetention must be greater than zero.");
        if (options.CompletedIdempotencyRetention >= DateTimeOffset.UtcNow - DateTimeOffset.MinValue)
            errors.Add("Queue:CompletedIdempotencyRetention exceeds the supported timestamp range.");
        ValidateTimer("Queue", nameof(options.IdempotencyCleanupInterval), options.IdempotencyCleanupInterval, errors);
        if (options.IdempotencyCleanupBatchSize <= 0)
            errors.Add("Queue:IdempotencyCleanupBatchSize must be greater than zero.");
        // Preserve an omitted lease before deriving the defaults' value: a consumer with a longer
        // budget needs its own lease.
        var defaultIdempotencyLease = options.Defaults.IdempotencyLease;
        ValidateConsumer("Queue:Defaults", options.Defaults, errors);
        foreach (var (consumerName, consumer) in options.Consumers)
        {
            // Resolve and validate effective settings before any messages are claimed.
            consumer.ApplyDefaults(options.Defaults, defaultIdempotencyLease);
            if (string.IsNullOrWhiteSpace(consumer.Queue))
                errors.Add($"Queue:Consumers:{consumerName}:Queue is required.");
            if (string.Equals(consumer.Queue, options.DeadLetterQueue, StringComparison.OrdinalIgnoreCase))
                errors.Add($"Queue:Consumers:{consumerName}:Queue must differ from the shared DeadLetterQueue.");
            ValidateConsumer($"Queue:Consumers:{consumerName}", consumer, errors);
        }
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }

    private static void ValidateConsumer(string path, QueueConsumerOptions options, List<string> errors)
    {
        if (options.BatchSize <= 0)
            errors.Add($"{path}:BatchSize must be greater than zero.");
        if (options.VisibilityTimeout <= TimeSpan.Zero)
            errors.Add($"{path}:VisibilityTimeout must be greater than zero.");
        if (options.VisibilityTimeout.TotalSeconds > int.MaxValue)
            errors.Add($"{path}:VisibilityTimeout exceeds the PGMQ integer limit.");
        ValidateTimer(path, nameof(options.VisibilityRenewalInterval), options.VisibilityRenewalInterval, errors);
        var validProcessingDuration = ValidateTimer(path, nameof(options.MaxProcessingDuration), options.MaxProcessingDuration, errors);
        var validGracePeriod = ValidateTimer(path, nameof(options.HandlerCancellationGracePeriod), options.HandlerCancellationGracePeriod, errors);
        var validOperationTimeout = ValidateTimer(path, nameof(options.QueueOperationTimeout), options.QueueOperationTimeout, errors);
        if (options.MaxProcessingDuration < options.VisibilityTimeout)
            errors.Add($"{path}:MaxProcessingDuration must be at least VisibilityTimeout.");
        if (options.VisibilityRenewalInterval.TotalMilliseconds + options.QueueOperationTimeout.TotalMilliseconds >= options.VisibilityTimeout.TotalMilliseconds)
            errors.Add($"{path}:VisibilityRenewalInterval plus QueueOperationTimeout must be less than VisibilityTimeout.");
        ValidateTimer(path, nameof(options.PollingInterval), options.PollingInterval, errors);
        if (options.MaxAttempts <= 0)
            errors.Add($"{path}:MaxAttempts must be greater than zero.");
        if (options.Concurrency <= 0)
            errors.Add($"{path}:Concurrency must be greater than zero.");
        // Handler/heartbeat drains run in parallel; cleanup starts after the longer drain. Check
        // timer ranges first so invalid configuration cannot overflow the lease calculation.
        var minimumLease = validProcessingDuration && validGracePeriod && validOperationTimeout
            ? options.MaxProcessingDuration
                + (options.HandlerCancellationGracePeriod > options.QueueOperationTimeout
                    ? options.HandlerCancellationGracePeriod : options.QueueOperationTimeout)
                + options.QueueOperationTimeout
            : TimeSpan.Zero;
        if (options.IdempotencyLease == TimeSpan.Zero && minimumLease > TimeSpan.Zero)
            options.IdempotencyLease = minimumLease + TimeSpan.FromSeconds(20);
        if (options.IdempotencyLease <= minimumLease)
            errors.Add($"{path}:IdempotencyLease must exceed MaxProcessingDuration plus max(HandlerCancellationGracePeriod, QueueOperationTimeout) plus QueueOperationTimeout.");
        if (options.RetryDelays.Any(delay => delay <= TimeSpan.Zero))
            errors.Add($"{path}:RetryDelays must contain positive values.");
    }

    private static bool ValidateTimer(string path, string property, TimeSpan value, List<string> errors)
    {
        if (value <= TimeSpan.Zero)
        {
            errors.Add($"{path}:{property} must be greater than zero.");
            return false;
        }
        if (value < TimeSpan.FromMilliseconds(1) || value > TimeSpan.FromMilliseconds(uint.MaxValue - 1))
        {
            errors.Add($"{path}:{property} must be between 1 millisecond and {uint.MaxValue - 1} milliseconds.");
            return false;
        }
        return true;
    }
}
