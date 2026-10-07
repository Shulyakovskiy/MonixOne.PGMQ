using Dapper;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Shouldly;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

[Collection(PgmqIntegrationCollection.Name)]
public sealed class PgmqDeadLetterIntegrationTests(PgmqContainerFixture fixture)
{
    private CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SharedDlq_ReadsDifferentQueuesWithTopicFiltersAndStablePagination()
    {
        var firstQueue = NewQueue();
        var secondQueue = NewQueue();
        var topic = $"test.failed.{Guid.NewGuid():N}";
        await PersistFailureAsync(firstQueue, topic);
        await PersistFailureAsync(secondQueue, topic);
        await PersistFailureAsync(firstQueue, "other.topic");
        var api = fixture.CreateQueue();

        var first = await api.GetDeadLetterMessagesAsync(new() { Topic = topic, PageSize = 1 }, Token);
        first.Messages.Count.ShouldBe(1);
        first.Messages[0].Queue.ShouldBe(firstQueue);
        first.Messages[0].Topic.ShouldBe(topic);
        first.Messages[0].OriginalMessage.ShouldContain(topic);
        first.NextAfterId.ShouldBe(first.Messages[0].Id);
        var next = await api.GetDeadLetterMessagesAsync(new()
        {
            Topic = topic, PageSize = 1, AfterId = first.NextAfterId!.Value
        }, Token);
        next.Messages.Count.ShouldBe(1);
        next.Messages[0].Queue.ShouldBe(secondQueue);
        next.NextAfterId.ShouldBeNull();
        next.Messages[0].Id.ShouldBeGreaterThan(first.Messages[0].Id);
        // Source identifiers overlap across tables; shared DLQ identifiers remain globally unique.
        next.Messages[0].MessageId.ShouldBe(first.Messages[0].MessageId);

        var queueOnly = await api.GetDeadLetterMessagesAsync(new() { Queue = firstQueue }, Token);
        queueOnly.Messages.Count.ShouldBe(2);
        (await api.GetDeadLetterMessagesAsync(new() { Queue = firstQueue, Topic = topic }, Token)).Messages.Count.ShouldBe(1);
        (await api.GetDeadLetterMessagesAsync(new() { Queue = "x' OR 1=1 --" }, Token)).Messages.ShouldBeEmpty();

        var all = await api.GetDeadLetterMessagesAsync(new() { PageSize = 500 }, Token);
        all.Messages.ShouldContain(entry => entry.Id == first.Messages[0].Id);
        all.Messages.ShouldContain(entry => entry.Id == next.Messages[0].Id);
    }

    [Fact]
    public async Task Inspection_IncludesInvisibleEntriesAndLeavesDeliveryStateUnchanged()
    {
        var source = NewQueue();
        await PersistFailureAsync(source, "inspection.topic");
        var claimed = (await fixture.Client.ReadAsync(fixture.DeadLetterQueue, 3600, 500, Token))
            .Single(entry => entry.Body.Contains(source, StringComparison.Ordinal));
        await using var connection = await fixture.DataSource.OpenConnectionAsync(Token);
        var before = await GetStateAsync(connection, claimed.Id);
        before.ShouldNotBeNull();

        var first = await fixture.CreateQueue().GetDeadLetterMessagesAsync(new() { Queue = source }, Token);
        var second = await fixture.CreateQueue().GetDeadLetterMessagesAsync(new() { Queue = source }, Token);

        first.Messages.Single().Id.ShouldBe(claimed.Id);
        second.Messages.Single().Id.ShouldBe(claimed.Id);
        (await GetStateAsync(connection, claimed.Id)).ShouldBe(before);
    }

    [Fact]
    public async Task CanceledInspection_ReleasesPoolWaitAndDoesNotModifyDlq()
    {
        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { MaxPoolSize = 1 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var api = fixture.CreateQueue(new PgmqClient(dataSource));
        await using (var blocker = await dataSource.OpenConnectionAsync(Token))
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
            cancellation.CancelAfter(TimeSpan.FromMilliseconds(150));
            await Should.ThrowAsync<OperationCanceledException>(() => api.GetDeadLetterMessagesAsync(
                cancellationToken: cancellation.Token));
        }
        using var verification = CancellationTokenSource.CreateLinkedTokenSource(Token);
        verification.CancelAfter(TimeSpan.FromSeconds(2));
        await api.GetDeadLetterMessagesAsync(new() { PageSize = 1 }, verification.Token);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Startup_MigratesLegacyDlqsInBatchesAndRepeatedStartupDoesNotDuplicate(bool reuseLegacyQueue)
    {
        var firstQueue = NewQueue();
        var secondQueue = NewQueue();
        var freshQueue = NewQueue();
        var sharedQueue = reuseLegacyQueue ? $"{firstQueue}-dlq" : NewQueue();
        var failedAt = DateTimeOffset.Parse("2026-01-01T00:00:00Z");
        await SeedLegacyAsync(firstQueue, 201, failedAt);
        await SeedLegacyAsync(secondQueue, 1, failedAt);
        var builder = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { MaxPoolSize = 1 };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var options = Options.Create(new PgmqOptions
        {
            DeadLetterQueue = sharedQueue,
            Consumers = new()
            {
                ["First"] = new() { Queue = firstQueue }, ["Second"] = new() { Queue = secondQueue },
                ["Fresh"] = new() { Queue = freshQueue }
            }
        });
        var initializer = new PgmqInitializer(dataSource, options, NullLogger<PgmqInitializer>.Instance);
        var api = new PgmqQueue(new PgmqClient(dataSource), new QueueJsonSerializer(), NullLogger<PgmqQueue>.Instance, options);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));

        await Task.WhenAll(initializer.StartAsync(timeout.Token),
            new PgmqInitializer(fixture.DataSource, options, NullLogger<PgmqInitializer>.Instance).StartAsync(timeout.Token));
        var migrated = await api.GetDeadLetterMessagesAsync(new() { PageSize = 500 }, timeout.Token);
        migrated.Messages.Count.ShouldBe(202);
        migrated.Messages.Count(entry => entry.Queue == firstQueue).ShouldBe(201);
        migrated.Messages.Count(entry => entry.Queue == secondQueue).ShouldBe(1);
        migrated.Messages.ShouldAllBe(entry => entry.Topic == "legacy.topic" && entry.FailedAt == failedAt);
        await initializer.StartAsync(timeout.Token);
        var repeated = await api.GetDeadLetterMessagesAsync(new() { PageSize = 500 }, timeout.Token);
        repeated.Messages.Select(entry => entry.Id).ShouldBe(migrated.Messages.Select(entry => entry.Id));

        await using var connection = await dataSource.OpenConnectionAsync(timeout.Token);
        (await connection.ExecuteScalarAsync<int>(new CommandDefinition($"SELECT count(*) FROM pgmq.\"q_{secondQueue}-dlq\"",
            cancellationToken: timeout.Token))).ShouldBe(0);
        (await connection.ExecuteScalarAsync<bool>(new CommandDefinition("SELECT to_regclass(@table) IS NULL",
            new { table = $"pgmq.\"q_{freshQueue}-dlq\"" }, cancellationToken: timeout.Token))).ShouldBeTrue();
        (await connection.ExecuteScalarAsync<int>(new CommandDefinition("""
            SELECT count(*) FROM pg_indexes WHERE schemaname = 'pgmq'
              AND tablename = pgmq.format_table_name(@sharedQueue, 'q') AND indexname LIKE 'ix_dlq_%';
            """, new { sharedQueue }, cancellationToken: timeout.Token))).ShouldBe(2);
    }

    [Fact]
    public async Task FailedMigration_RollsBackBatchAndNextStartupResumesWithoutLoss()
    {
        var source = NewQueue();
        var shared = NewQueue();
        var options = Options.Create(new PgmqOptions
        {
            DeadLetterQueue = shared, Consumers = new() { ["Source"] = new() { Queue = source } }
        });
        var initializer = new PgmqInitializer(fixture.DataSource, options, NullLogger<PgmqInitializer>.Instance);
        await initializer.StartAsync(Token);
        await SeedLegacyAsync(source, 201, DateTimeOffset.UtcNow);
        await using var connection = await fixture.DataSource.OpenConnectionAsync(Token);
        var function = $"reject_{Guid.NewGuid():N}";
        await connection.ExecuteAsync(new CommandDefinition($"""
            CREATE FUNCTION monixone_queue.{function}() RETURNS trigger LANGUAGE plpgsql AS
            $$ BEGIN
                IF ((NEW.message->>'originalMessage')::jsonb->'payload'->>'value')::int > 100 THEN
                    RAISE EXCEPTION 'simulated DLQ insert failure';
                END IF;
                RETURN NEW;
            END; $$;
            CREATE TRIGGER reject_insert BEFORE INSERT ON pgmq.q_{shared}
                FOR EACH ROW EXECUTE FUNCTION monixone_queue.{function}();
            """, cancellationToken: Token));

        await Should.ThrowAsync<PostgresException>(() => initializer.StartAsync(Token));

        (await connection.ExecuteScalarAsync<int>(new CommandDefinition($"SELECT count(*) FROM pgmq.\"q_{source}-dlq\"",
            cancellationToken: Token))).ShouldBe(101);
        (await connection.ExecuteScalarAsync<int>(new CommandDefinition($"SELECT count(*) FROM pgmq.q_{shared}",
            cancellationToken: Token))).ShouldBe(100);
        await connection.ExecuteAsync(new CommandDefinition($"DROP TRIGGER reject_insert ON pgmq.q_{shared}",
            cancellationToken: Token));
        await initializer.StartAsync(Token);
        var api = new PgmqQueue(fixture.Client, new QueueJsonSerializer(), NullLogger<PgmqQueue>.Instance, options);
        var resumed = await api.GetDeadLetterMessagesAsync(new() { PageSize = 500 }, Token);
        resumed.Messages.Count.ShouldBe(201);
        resumed.Messages.Select(entry => entry.MessageId).Distinct().Count().ShouldBe(201);
    }

    private async Task PersistFailureAsync(string source, string topic)
    {
        await using var connection = await fixture.DataSource.OpenConnectionAsync(Token);
        await connection.ExecuteAsync(new CommandDefinition("SELECT pgmq.create(@source)", new { source }, cancellationToken: Token));
        var serializer = new QueueJsonSerializer();
        var original = serializer.Serialize(new { type = topic, payload = new { value = 1 } });
        await fixture.Client.SendAsync(source, original, Token);
        var message = (await fixture.Client.ReadAsync(source, 30, 1, Token)).Single();
        var failure = serializer.Serialize(new
        {
            originalMessage = original, queue = source, topic, consumerName = "dlq-tests", messageId = message.Id,
            deliveryCount = message.ReadCount, failedAt = DateTimeOffset.UtcNow,
            exceptionType = typeof(InvalidOperationException).FullName, errorMessage = "failure"
        });
        (await fixture.Client.TryMoveToDeadLetterAsync(source, fixture.DeadLetterQueue, message,
            "dlq-tests", null, null, failure, Token)).ShouldBeTrue();
    }

    private async Task SeedLegacyAsync(string source, int count, DateTimeOffset failedAt)
    {
        var legacy = $"{source}-dlq";
        await using var connection = await fixture.DataSource.OpenConnectionAsync(Token);
        await connection.ExecuteAsync(new CommandDefinition("SELECT pgmq.create(@legacy)", new { legacy }, cancellationToken: Token));
        var serializer = new QueueJsonSerializer();
        var bodies = Enumerable.Range(1, count).Select(id => serializer.Serialize(new
        {
            originalMessage = serializer.Serialize(new { type = "legacy.topic", payload = new { value = id } }),
            queue = source, messageId = id, deliveryCount = 5, failedAt,
            exceptionType = "LegacyException", errorMessage = "legacy failure"
        })).ToArray();
        await fixture.Client.SendBatchAsync(legacy, bodies, Token);
    }

    private Task<string?> GetStateAsync(NpgsqlConnection connection, long id) => connection.ExecuteScalarAsync<string>(
        new CommandDefinition($"""
            SELECT jsonb_build_object('readCount', read_ct, 'visibility', vt)::text
            FROM pgmq.q_{fixture.DeadLetterQueue} WHERE msg_id = @id;
            """, new { id }, cancellationToken: Token));

    private static string NewQueue() => $"dlq_{Guid.NewGuid():N}";
}
