using Dapper;
using Shouldly;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

[Collection(PgmqIntegrationCollection.Name)]
public sealed class PgmqDeliveryLeaseIntegrationTests(PgmqContainerFixture fixture)
{
    private const string Consumer = "lease-tests";
    private CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Renewal_ClipsVisibilityToDeadlineAndCannotExtendAfterExpiry()
    {
        var delivery = await CreateDeliveryAsync();
        var deadline = delivery.Message.ReadAt.AddSeconds(1);

        (await fixture.Client.TryRenewVisibilityAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.FromSeconds(60), deadline, Token)).ShouldBeTrue();
        (await GetVisibilityAsync(delivery)).ShouldBe(deadline);

        await Task.Delay(TimeSpan.FromMilliseconds(1200), Token);

        (await fixture.Client.TryRenewVisibilityAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.FromSeconds(60), deadline, Token)).ShouldBeFalse();
        (await GetVisibilityAsync(delivery)).ShouldBe(deadline);
    }

    [Fact]
    public async Task StaleDelivery_CannotRenewRetryOrMoveCurrentDeliveryToDeadLetter()
    {
        var delivery = await CreateDeliveryAsync();
        await using (var connection = await fixture.DataSource.OpenConnectionAsync(Token))
        {
            await connection.ExecuteAsync(new CommandDefinition(
                $"UPDATE pgmq.q_{delivery.Queue} SET vt = clock_timestamp() - interval '1 second'",
                cancellationToken: Token));
        }
        var current = (await fixture.Client.ReadAsync(delivery.Queue, 30, 1, Token)).Single();
        current.ReadCount.ShouldBe(delivery.Message.ReadCount + 1);
        var visibility = await GetVisibilityAsync(delivery);

        (await fixture.Client.TryRenewVisibilityAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.FromSeconds(60), delivery.Message.ReadAt.AddMinutes(2), Token)).ShouldBeFalse();
        (await fixture.Client.TryRetryAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.Zero, Token)).ShouldBeFalse();
        (await fixture.Client.TryMoveToDeadLetterAsync(delivery.Queue, fixture.DeadLetterQueue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, "{}", Token)).ShouldBeFalse();

        (await GetVisibilityAsync(delivery)).ShouldBe(visibility);
        (await fixture.CreateQueue().GetDeadLetterMessagesAsync(new() { Queue = delivery.Queue }, Token)).Messages.ShouldBeEmpty();
    }

    [Fact]
    public async Task ExpiredIdempotencyLease_CannotCompleteRenewRetryOrDeadLetter()
    {
        var delivery = await CreateDeliveryAsync();
        await using (var connection = await fixture.DataSource.OpenConnectionAsync(Token))
        {
            await connection.ExecuteAsync(new CommandDefinition("""
                UPDATE monixone_queue.idempotency_keys SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE consumer_name = @Consumer AND idempotency_key = @Key;
                """, new { Consumer, delivery.Key }, cancellationToken: Token));
        }
        var store = new PgmqIdempotencyStore(fixture.DataSource);

        (await store.CompleteAsync(Consumer, delivery.Key, delivery.LeaseToken, Token)).ShouldBeFalse();
        (await fixture.Client.TryRenewVisibilityAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.FromSeconds(60), delivery.Message.ReadAt.AddMinutes(2), Token)).ShouldBeFalse();
        (await fixture.Client.TryRetryAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.Zero, Token)).ShouldBeFalse();
        (await fixture.Client.TryMoveToDeadLetterAsync(delivery.Queue, fixture.DeadLetterQueue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, "{}", Token)).ShouldBeFalse();
    }

    [Fact]
    public async Task Retry_ReleasesLeaseAndImmediatelyReturnsMessageOnShutdown()
    {
        var delivery = await CreateDeliveryAsync();

        (await fixture.Client.TryRetryAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.Zero, Token)).ShouldBeTrue();

        (await fixture.Client.ReadAsync(delivery.Queue, 30, 1, Token)).Single().ReadCount.ShouldBe(2);
        var next = await new PgmqIdempotencyStore(fixture.DataSource)
            .TryAcquireAsync(Consumer, delivery.Key, TimeSpan.FromSeconds(30), Token);
        next.IsAcquired.ShouldBeTrue();
        next.LeaseToken.ShouldNotBe(delivery.LeaseToken);
    }

    [Fact]
    public async Task DeadLetter_PersistsCopyDeletesSourceAndReleasesLease()
    {
        var delivery = await CreateDeliveryAsync();

        (await fixture.Client.TryMoveToDeadLetterAsync(delivery.Queue, fixture.DeadLetterQueue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, """{"reason":"timeout"}""", Token)).ShouldBeTrue();

        (await fixture.Client.ReadAsync(delivery.Queue, 30, 1, Token)).ShouldBeEmpty();
        (await fixture.CreateQueue().GetDeadLetterMessagesAsync(new() { Queue = delivery.Queue }, Token)).Messages.Count.ShouldBe(1);
        (await new PgmqIdempotencyStore(fixture.DataSource)
            .TryAcquireAsync(Consumer, delivery.Key, TimeSpan.FromSeconds(30), Token)).IsAcquired.ShouldBeTrue();
    }

    [Fact]
    public async Task CompletedMessage_CanOnlyBeAcknowledgedAndCannotEnterFailureHandling()
    {
        var delivery = await CreateDeliveryAsync();
        (await new PgmqIdempotencyStore(fixture.DataSource)
            .CompleteAsync(Consumer, delivery.Key, delivery.LeaseToken, Token)).ShouldBeTrue();

        (await fixture.Client.TryRetryAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, TimeSpan.Zero, Token)).ShouldBeFalse();
        (await fixture.Client.TryMoveToDeadLetterAsync(delivery.Queue, fixture.DeadLetterQueue, delivery.Message, Consumer, delivery.Key,
            delivery.LeaseToken, "{}", Token)).ShouldBeFalse();
        (await fixture.Client.TryDeleteCompletedAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key, Token)).ShouldBeTrue();

        (await fixture.Client.ReadAsync(delivery.Queue, 30, 1, Token)).ShouldBeEmpty();
    }

    [Theory]
    [InlineData("renew")]
    [InlineData("retry")]
    [InlineData("dead-letter")]
    [InlineData("defer")]
    [InlineData("ack")]
    public async Task BlockedDeliveryOperation_CancellationReturnsConnectionToSingleConnectionPool(string operation)
    {
        var delivery = await CreateDeliveryAsync();
        if (operation == "ack")
            (await new PgmqIdempotencyStore(fixture.DataSource)
                .CompleteAsync(Consumer, delivery.Key, delivery.LeaseToken, Token)).ShouldBeTrue();
        var builder = new Npgsql.NpgsqlConnectionStringBuilder(fixture.ConnectionString) { MaxPoolSize = 1 };
        await using var dataSource = Npgsql.NpgsqlDataSource.Create(builder.ConnectionString);
        var client = new PgmqClient(dataSource);
        await using var blocker = await fixture.DataSource.OpenConnectionAsync(Token);
        await using var transaction = await blocker.BeginTransactionAsync(Token);
        await blocker.ExecuteAsync(new CommandDefinition("""
            SELECT 1 FROM monixone_queue.idempotency_keys
            WHERE consumer_name = @Consumer AND idempotency_key = @Key FOR UPDATE;
            """, new { Consumer, delivery.Key }, transaction, cancellationToken: Token));

        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(150));
        await Should.ThrowAsync<OperationCanceledException>(() => operation switch
        {
            "renew" => client.TryRenewVisibilityAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
                delivery.LeaseToken, TimeSpan.FromSeconds(60), delivery.Message.ReadAt.AddMinutes(2), cancellation.Token),
            "retry" => client.TryRetryAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
                delivery.LeaseToken, TimeSpan.Zero, cancellation.Token),
            "dead-letter" => client.TryMoveToDeadLetterAsync(delivery.Queue, fixture.DeadLetterQueue, delivery.Message, Consumer, delivery.Key,
                delivery.LeaseToken, "{}", cancellation.Token),
            "defer" => client.TryDeferConcurrentAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key,
                TimeSpan.FromSeconds(1), cancellation.Token),
            "ack" => client.TryDeleteCompletedAsync(delivery.Queue, delivery.Message, Consumer, delivery.Key, cancellation.Token),
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        });

        // The blocker remains locked. A separate query must still get the sole pooled connection promptly.
        using var verificationCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await using var connection = await dataSource.OpenConnectionAsync(verificationCancellation.Token);
        (await connection.ExecuteScalarAsync<int>(new CommandDefinition("SELECT 1",
            cancellationToken: verificationCancellation.Token))).ShouldBe(1);
    }

    private async Task<TestDelivery> CreateDeliveryAsync()
    {
        var queue = $"lease_{Guid.NewGuid():N}";
        var key = $"key:{Guid.NewGuid():N}";
        await using (var connection = await fixture.DataSource.OpenConnectionAsync(Token))
        {
            await connection.ExecuteAsync(new CommandDefinition("SELECT pgmq.create(@queue);",
                new { queue }, cancellationToken: Token));
        }
        await fixture.Client.SendAsync(queue, "{}", Token);
        var message = (await fixture.Client.ReadAsync(queue, 30, 1, Token)).Single();
        var claim = await new PgmqIdempotencyStore(fixture.DataSource)
            .TryAcquireAsync(Consumer, key, TimeSpan.FromMinutes(3), Token);
        return new TestDelivery(queue, message, key, claim.LeaseToken!.Value);
    }

    private async Task<DateTime> GetVisibilityAsync(TestDelivery delivery)
    {
        await using var connection = await fixture.DataSource.OpenConnectionAsync(Token);
        return await connection.ExecuteScalarAsync<DateTime>(new CommandDefinition(
            $"SELECT vt FROM pgmq.q_{delivery.Queue} WHERE msg_id = @Id", new { delivery.Message.Id }, cancellationToken: Token));
    }

    private sealed record TestDelivery(string Queue, PgmqMessage Message, string Key, Guid LeaseToken);
}
