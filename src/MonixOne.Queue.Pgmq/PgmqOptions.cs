using System.ComponentModel.DataAnnotations;
using Microsoft.Extensions.Options;

namespace MonixOne.Queue.Pgmq;

public sealed class PgmqOptions
{
    public const string SectionName = "Queue";
    public const string SupportedExtensionVersion = "1.13.0";
    public string? ConnectionString { get; set; }
    public string ConnectionStringName { get; set; } = "Queue";
    public QueueConsumerOptions Defaults { get; set; } = new();
    public Dictionary<string, PgmqConsumerOptions> Consumers { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class PgmqConsumerOptions : QueueConsumerOptions
{
    public PgmqConsumerOptions()
    {
        // Zero distinguishes an omitted consumer value from QueueConsumerOptions' public defaults during IConfiguration binding.
        BatchSize = 0;
        VisibilityTimeout = TimeSpan.Zero;
        PollingInterval = TimeSpan.Zero;
        MaxAttempts = 0;
        Concurrency = 0;
        RetryDelays = [];
    }

    [Required]
    public string Queue { get; set; } = string.Empty;
    public List<int> RetryDelaysSeconds { get; set; } = [];

    internal void ApplyDefaults(QueueConsumerOptions defaults)
    {
        // Bind only supplies explicitly configured values; zero/empty values above retain that distinction until
        // validation, where package defaults are merged once for the worker's effective configuration.
        BatchSize = BatchSize == 0 ? defaults.BatchSize : BatchSize;
        VisibilityTimeout = VisibilityTimeout == TimeSpan.Zero ? defaults.VisibilityTimeout : VisibilityTimeout;
        PollingInterval = PollingInterval == TimeSpan.Zero ? defaults.PollingInterval : PollingInterval;
        MaxAttempts = MaxAttempts == 0 ? defaults.MaxAttempts : MaxAttempts;
        Concurrency = Concurrency == 0 ? defaults.Concurrency : Concurrency;
        if (RetryDelaysSeconds.Count > 0)
        {
            RetryDelays = RetryDelaysSeconds.Select(seconds => TimeSpan.FromSeconds(seconds)).ToArray();
        }
        else
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
        if (string.IsNullOrWhiteSpace(options.ConnectionString)) errors.Add("Queue connection string is required.");
        ValidateConsumer("Queue:Defaults", options.Defaults, errors);
        foreach (var (consumerName, consumer) in options.Consumers)
        {
            // Workers consume this validated, merged instance. Do not defer default resolution to message handling,
            // otherwise a malformed configuration could fail only after messages have already been claimed.
            consumer.ApplyDefaults(options.Defaults);
            if (string.IsNullOrWhiteSpace(consumer.Queue)) errors.Add($"Queue:Consumers:{consumerName}:Queue is required.");
            ValidateConsumer($"Queue:Consumers:{consumerName}", consumer, errors);
        }
        return errors.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(errors);
    }

    private static void ValidateConsumer(string path, QueueConsumerOptions options, List<string> errors)
    {
        if (options.BatchSize <= 0) errors.Add($"{path}:BatchSize must be greater than zero.");
        if (options.VisibilityTimeout <= TimeSpan.Zero) errors.Add($"{path}:VisibilityTimeout must be greater than zero.");
        if (options.PollingInterval <= TimeSpan.Zero) errors.Add($"{path}:PollingInterval must be greater than zero.");
        if (options.MaxAttempts <= 0) errors.Add($"{path}:MaxAttempts must be greater than zero.");
        if (options.Concurrency <= 0) errors.Add($"{path}:Concurrency must be greater than zero.");
        if (options.RetryDelays.Any(delay => delay <= TimeSpan.Zero)) errors.Add($"{path}:RetryDelays must contain positive values.");
    }
}
