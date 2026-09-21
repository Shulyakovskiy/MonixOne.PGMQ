using MonixOne.Queue;
using Shouldly;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

public sealed class PgmqOptionsValidatorTests
{
    [Fact]
    public void Validate_ConsumerUsesDefaults_ReturnsSuccessAndAppliesDefaults()
    {
        var consumer = new PgmqConsumerOptions { Queue = "notifications" };
        var options = new PgmqOptions
        {
            ConnectionString = "Host=localhost;Database=queue",
            Defaults = new QueueConsumerOptions
            {
                BatchSize = 25,
                VisibilityTimeout = TimeSpan.FromMinutes(3),
                PollingInterval = TimeSpan.FromSeconds(2),
                MaxAttempts = 7,
                Concurrency = 3,
                RetryDelays = [TimeSpan.FromSeconds(10)]
            },
            Consumers = new Dictionary<string, PgmqConsumerOptions> { ["Notifications"] = consumer }
        };

        var result = new PgmqOptionsValidator().Validate(null, options);

        result.Succeeded.ShouldBeTrue();
        consumer.BatchSize.ShouldBe(25);
        consumer.VisibilityTimeout.ShouldBe(TimeSpan.FromMinutes(3));
        consumer.MaxAttempts.ShouldBe(7);
        consumer.Concurrency.ShouldBe(3);
        consumer.IdempotencyLease.ShouldBe(TimeSpan.FromMinutes(6));
        consumer.RetryDelays.ShouldBe([TimeSpan.FromSeconds(10)]);
    }

    [Fact]
    public void Validate_MissingConnectionString_ReturnsFailure()
    {
        var options = new PgmqOptions
        {
            Consumers = new Dictionary<string, PgmqConsumerOptions>
            {
                ["Notifications"] = new() { Queue = "notifications" }
            }
        };

        var result = new PgmqOptionsValidator().Validate(null, options);

        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain("Queue connection string is required.");
    }

    [Fact]
    public void Validate_InvalidConsumer_ReturnsFailureWithConfigurationPath()
    {
        var options = new PgmqOptions
        {
            ConnectionString = "Host=localhost;Database=queue",
            Consumers = new Dictionary<string, PgmqConsumerOptions>
            {
                ["Notifications"] = new() { Queue = "", BatchSize = -1 }
            }
        };

        var result = new PgmqOptionsValidator().Validate(null, options);

        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain("Queue:Consumers:Notifications:Queue is required.");
        result.Failures.ShouldContain("Queue:Consumers:Notifications:BatchSize must be greater than zero.");
    }

    [Fact]
    public void Validate_InvalidIdempotencyCleanup_ReturnsFailureWithConfigurationPath()
    {
        var options = new PgmqOptions
        {
            ConnectionString = "Host=localhost;Database=queue",
            CompletedIdempotencyRetention = TimeSpan.Zero,
            IdempotencyCleanupInterval = TimeSpan.Zero,
            IdempotencyCleanupBatchSize = 0
        };

        var result = new PgmqOptionsValidator().Validate(null, options);

        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain("Queue:CompletedIdempotencyRetention must be greater than zero.");
        result.Failures.ShouldContain("Queue:IdempotencyCleanupInterval must be greater than zero.");
        result.Failures.ShouldContain("Queue:IdempotencyCleanupBatchSize must be greater than zero.");
    }
}
