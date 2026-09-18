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
                   EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'monixone_queue' AND table_name = 'infrastructure_metadata')
            FROM monixone_queue.infrastructure_metadata
            WHERE component = 'pgmq';
            """);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);

        (await reader.ReadAsync(TestContext.Current.CancellationToken)).ShouldBeTrue();
        reader.GetString(0).ShouldBe("1.13.0");
        reader.GetBoolean(1).ShouldBeTrue();
        reader.GetBoolean(2).ShouldBeTrue();
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

        var first = await store.TryAcquireAsync(consumer, key, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);
        var competing = await store.TryAcquireAsync(consumer, key, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);

        first.IsAcquired.ShouldBeTrue();
        competing.IsInProgress.ShouldBeTrue();
        (await store.CompleteAsync(consumer, key, first.LeaseToken!.Value, TestContext.Current.CancellationToken)).ShouldBeTrue();

        var duplicate = await store.TryAcquireAsync(consumer, key, TimeSpan.FromMinutes(1), TestContext.Current.CancellationToken);
        duplicate.IsCompleted.ShouldBeTrue();
    }
}
