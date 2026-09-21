using Dapper;
using Shouldly;
using System.Text.Json;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

[Collection(PgmqIntegrationCollection.Name)]
public sealed class PgmqClientIntegrationTests(PgmqContainerFixture fixture)
{
    [Fact]
    public async Task StartupProvisioning_AppliesBundledPgmqSqlAndMetadata()
    {
        await using var command = fixture.DataSource.CreateCommand("""
            SELECT installed_version,
                   EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'),
                   EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'monixone_queue' AND table_name = 'infrastructure_metadata'),
                   EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'monixone_queue' AND indexname = 'ix_idempotency_keys_completed_at')
            FROM monixone_queue.infrastructure_metadata
            WHERE component = 'pgmq';
            """);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);

        (await reader.ReadAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
        reader.GetString(0).ShouldBe("1.13.0");
        reader.GetBoolean(1).ShouldBeTrue();
        reader.GetBoolean(2).ShouldBeTrue();
        reader.GetBoolean(3).ShouldBeTrue();
    }

    [Fact]
    public async Task StartupProvisioning_CreatesConfiguredQueueAndDeadLetterQueue()
    {
        await using var command = fixture.DataSource.CreateCommand("""
            SELECT EXISTS (SELECT 1 FROM pgmq.meta WHERE queue_name = @queue),
                   EXISTS (SELECT 1 FROM pgmq.meta WHERE queue_name = @deadLetterQueue);
            """);
        command.Parameters.AddWithValue("queue", fixture.Queue);
        command.Parameters.AddWithValue("deadLetterQueue", $"{fixture.Queue}-dlq");
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);

        (await reader.ReadAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
        reader.GetBoolean(0).ShouldBeTrue();
        reader.GetBoolean(1).ShouldBeTrue();
    }

    [Fact]
    public async Task SendReadDelete_MessageIsAcknowledged()
    {
        const string body = """{"kind":"roundtrip"}""";

        var messageId = await fixture.Client.SendAsync(fixture.Queue, body, TestContext.Current.CancellationToken);
        var messages = await fixture.Client.ReadAsync(fixture.Queue, 30, 10, TestContext.Current.CancellationToken);

        var message = messages.Single(item => item.Id == messageId);
        message.ReadCount.ShouldBe(1);
        using var messageJson = JsonDocument.Parse(message.Body);
        messageJson.RootElement.GetProperty("kind").GetString().ShouldBe("roundtrip");

        await fixture.Client.DeleteAsync(fixture.Queue, messageId, TestContext.Current.CancellationToken);
        var afterDelete = await fixture.Client.ReadAsync(fixture.Queue, 1, 10, TestContext.Current.CancellationToken);
        afterDelete.ShouldNotContain(item => item.Id == messageId);
    }

    [Fact]
    public async Task Read_VisibilityTimeoutExpires_MessageIsRedelivered()
    {
        var messageId = await fixture.Client.SendAsync(fixture.Queue, """{"kind":"visibility"}""", TestContext.Current.CancellationToken);
        var firstDelivery = (await fixture.Client.ReadAsync(fixture.Queue, 1, 10, TestContext.Current.CancellationToken))
            .Single(item => item.Id == messageId);

        await Task.Delay(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        var secondDelivery = (await fixture.Client.ReadAsync(fixture.Queue, 30, 10, TestContext.Current.CancellationToken))
            .Single(item => item.Id == messageId);

        firstDelivery.ReadCount.ShouldBe(1);
        secondDelivery.ReadCount.ShouldBe(2);
        await fixture.Client.DeleteAsync(fixture.Queue, messageId, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task IdempotencyStore_AllowsOneReplicaAndSkipsCompletedDuplicate()
    {
        var store = new PgmqIdempotencyStore(fixture.DataSource);
        const string consumer = "notifications";
        var key = $"notification:{Guid.NewGuid():N}";

        var claims = await Task.WhenAll(
            store.TryAcquireAsync(consumer, key, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken),
            store.TryAcquireAsync(consumer, key, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken));

        claims.Count(claim => claim.IsAcquired).ShouldBe(1);
        claims.Count(claim => claim.IsInProgress).ShouldBe(1);
        var acquired = claims.Single(claim => claim.IsAcquired);
        (await store.CompleteAsync(consumer, key, acquired.LeaseToken!.Value, TestContext.Current.CancellationToken)).ShouldBeTrue();

        var duplicate = await store.TryAcquireAsync(consumer, key, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);
        duplicate.IsCompleted.ShouldBeTrue();
    }

    [Fact]
    public async Task IdempotencyStore_DeletesCompletedKeysPastRetentionInBatches()
    {
        var store = new PgmqIdempotencyStore(fixture.DataSource);
        const string consumer = "cleanup";
        var firstKey = $"first:{Guid.NewGuid():N}";
        var secondKey = $"second:{Guid.NewGuid():N}";

        var firstClaim = await store.TryAcquireAsync(consumer, firstKey, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);
        var secondClaim = await store.TryAcquireAsync(consumer, secondKey, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);
        (await store.CompleteAsync(consumer, firstKey, firstClaim.LeaseToken!.Value, TestContext.Current.CancellationToken)).ShouldBeTrue();
        (await store.CompleteAsync(consumer, secondKey, secondClaim.LeaseToken!.Value, TestContext.Current.CancellationToken)).ShouldBeTrue();

        await using var connection = await fixture.DataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE monixone_queue.idempotency_keys
            SET completed_at = now() - interval '2 days'
            WHERE consumer_name = @consumer;
            """,
            new { consumer },
            cancellationToken: TestContext.Current.CancellationToken));

        var deleted = await store.DeleteCompletedAsync(DateTimeOffset.UtcNow.AddDays(-1), 1, TestContext.Current.CancellationToken);
        deleted.ShouldBe(1);

        var remaining = await connection.QuerySingleAsync<int>(new CommandDefinition("""
            SELECT count(*)
            FROM monixone_queue.idempotency_keys
            WHERE consumer_name = @consumer
              AND status = 'completed';
            """,
            new { consumer },
            cancellationToken: TestContext.Current.CancellationToken));
        remaining.ShouldBe(1);
    }
}
