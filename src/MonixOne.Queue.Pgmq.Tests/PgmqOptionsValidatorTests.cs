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
                MaxProcessingDuration = TimeSpan.FromMinutes(6),
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
        consumer.IdempotencyLease.ShouldBe(TimeSpan.FromMinutes(6) + TimeSpan.FromSeconds(30));
        consumer.RetryDelays.ShouldBe([TimeSpan.FromSeconds(10)]);
    }

    [Fact]
    public void Validate_SharedDlqCannotBeAConsumerSource_ReturnsFailure()
    {
        var options = new PgmqOptions
        {
            ConnectionString = "Host=localhost", DeadLetterQueue = "shared_dlq",
            Consumers = new() { ["Invalid"] = new() { Queue = "SHARED_DLQ" } }
        };

        var result = new PgmqOptionsValidator().Validate(null, options);

        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain("Queue:Consumers:Invalid:Queue must differ from the shared DeadLetterQueue.");
    }

    [Fact]
    public void Validate_EmptySharedDlq_ReturnsFailure()
    {
        var options = new PgmqOptions { ConnectionString = "Host=localhost", DeadLetterQueue = " " };

        var result = new PgmqOptionsValidator().Validate(null, options);
        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain("Queue:DeadLetterQueue is required.");
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

    [Fact]
    public void Validate_ConsumerIncreasesProcessingLimit_DerivesItsOwnLease()
    {
        var consumer = new PgmqConsumerOptions { Queue = "relay", MaxProcessingDuration = TimeSpan.FromMinutes(5) };
        var options = new PgmqOptions
        {
            ConnectionString = "Host=localhost",
            Consumers = new() { ["Relay"] = consumer }
        };

        new PgmqOptionsValidator().Validate(null, options).Succeeded.ShouldBeTrue();

        consumer.IdempotencyLease.ShouldBe(TimeSpan.FromMinutes(5) + TimeSpan.FromSeconds(30));
        consumer.VisibilityRenewalInterval.ShouldBe(TimeSpan.FromSeconds(20));
    }

    [Fact]
    public void Validate_LeaseDoesNotCoverCancellationAndCleanup_ReturnsFailure()
    {
        var options = new PgmqOptions { ConnectionString = "Host=localhost" };
        options.Defaults.IdempotencyLease = TimeSpan.FromMinutes(2);

        var result = new PgmqOptionsValidator().Validate(null, options);
        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain(
            "Queue:Defaults:IdempotencyLease must exceed MaxProcessingDuration plus max(HandlerCancellationGracePeriod, QueueOperationTimeout) plus QueueOperationTimeout.");
    }

    [Fact]
    public void Validate_HeartbeatShutdownLongerThanHandlerGrace_DerivesLeaseCoveringBothShutdownAndCleanup()
    {
        var options = new PgmqOptions { ConnectionString = "Host=localhost" };
        options.Defaults.HandlerCancellationGracePeriod = TimeSpan.FromSeconds(1);
        options.Defaults.QueueOperationTimeout = TimeSpan.FromSeconds(30);

        new PgmqOptionsValidator().Validate(null, options).Succeeded.ShouldBeTrue();

        options.Defaults.IdempotencyLease.ShouldBe(TimeSpan.FromSeconds(200));
    }

    [Fact]
    public void Validate_RenewalLeavesNoTimeForDatabaseOperation_ReturnsFailure()
    {
        var options = new PgmqOptions { ConnectionString = "Host=localhost" };
        options.Defaults.VisibilityRenewalInterval = TimeSpan.FromSeconds(55);

        var result = new PgmqOptionsValidator().Validate(null, options);
        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain(
            "Queue:Defaults:VisibilityRenewalInterval plus QueueOperationTimeout must be less than VisibilityTimeout.");
    }

    [Fact]
    public void Validate_ExplicitConsumerRetryDelays_PreservesOverride()
    {
        var consumer = new PgmqConsumerOptions
        {
            Queue = "relay", RetryDelays = [TimeSpan.FromSeconds(17)]
        };
        var options = new PgmqOptions
        {
            ConnectionString = "Host=localhost", Consumers = new() { ["Relay"] = consumer }
        };

        new PgmqOptionsValidator().Validate(null, options).Succeeded.ShouldBeTrue();

        consumer.RetryDelays.ShouldBe([TimeSpan.FromSeconds(17)]);
    }

    [Fact]
    public void Validate_IntervalsExceedTimerRange_ReturnsFailure()
    {
        var options = new PgmqOptions
        {
            ConnectionString = "Host=localhost", IdempotencyCleanupInterval = TimeSpan.MaxValue
        };
        options.Defaults.PollingInterval = TimeSpan.MaxValue;

        var result = new PgmqOptionsValidator().Validate(null, options);

        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain(failure => failure.Contains("IdempotencyCleanupInterval"));
        result.Failures.ShouldContain(failure => failure.Contains("PollingInterval"));
    }

    [Fact]
    public void Validate_InvalidTimersAndProcessingLimit_ReturnsFailure()
    {
        var options = new PgmqOptions { ConnectionString = "Host=localhost" };
        options.Defaults.MaxProcessingDuration = TimeSpan.FromSeconds(30);
        options.Defaults.HandlerCancellationGracePeriod = TimeSpan.FromMilliseconds(-1);
        options.Defaults.QueueOperationTimeout = TimeSpan.MaxValue;

        var result = new PgmqOptionsValidator().Validate(null, options);

        result.Failed.ShouldBeTrue();
        result.Failures.ShouldContain("Queue:Defaults:MaxProcessingDuration must be at least VisibilityTimeout.");
        result.Failures.ShouldContain(failure => failure.Contains("HandlerCancellationGracePeriod"));
        result.Failures.ShouldContain(failure => failure.Contains("QueueOperationTimeout"));
    }
}
