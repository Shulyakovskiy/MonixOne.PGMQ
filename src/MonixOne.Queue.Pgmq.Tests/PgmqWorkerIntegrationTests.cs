using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

[Collection(PgmqIntegrationCollection.Name)]
public sealed class PgmqWorkerIntegrationTests(PgmqContainerFixture fixture)
{
    private CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task LongHandler_RenewsVisibilityAndAcknowledgesOnlyAfterScopeDisposal()
    {
        await using var test = await StartWorkerAsync(TimeSpan.FromSeconds(4));
        await test.State.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await Task.Delay(TimeSpan.FromMilliseconds(1400), Token);

        (await fixture.Client.ReadAsync(test.Queue, 1, 10, Token)).ShouldBeEmpty();
        test.State.Disposed.Task.IsCompleted.ShouldBeFalse();
        test.State.Finish.TrySetResult();
        await test.State.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await WaitForStatusAsync(test, "completed");
        await test.Worker.StopAsync(Token);

        test.Lifetime.ApplicationStopping.IsCancellationRequested.ShouldBeFalse();
    }

    [Fact]
    public async Task Shutdown_DisposesScopeAndImmediatelyReleasesDeliveryAndIdempotencyLease()
    {
        await using var test = await StartWorkerAsync(TimeSpan.FromSeconds(4));
        await test.State.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);

        await test.Worker.StopAsync(Token).WaitAsync(TimeSpan.FromSeconds(2), Token);

        test.State.Disposed.Task.IsCompleted.ShouldBeTrue();
        (await fixture.Client.ReadAsync(test.Queue, 30, 1, Token)).Count.ShouldBe(1);
        (await new PgmqIdempotencyStore(fixture.DataSource)
            .TryAcquireAsync(test.Queue, test.Key, TimeSpan.FromSeconds(5), Token)).IsAcquired.ShouldBeTrue();
    }

    [Fact]
    public async Task Deadline_CancelsHandlerDisposesScopeAndSchedulesBackoff()
    {
        await using var test = await StartWorkerAsync(TimeSpan.FromMilliseconds(1100));
        await test.State.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await test.State.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(3), Token);
        await WaitForStatusAsync(test, null);
        await test.Worker.StopAsync(Token);

        (await fixture.Client.ReadAsync(test.Queue, 1, 1, Token)).ShouldBeEmpty();
        test.Lifetime.ApplicationStopping.IsCancellationRequested.ShouldBeFalse();
    }

    [Fact]
    public async Task UncooperativeHandler_StopsApplicationKeepsLeaseAndPreservesLiveScope()
    {
        await using var test = await StartWorkerAsync(TimeSpan.FromMilliseconds(1100), ignoreCancellation: true);
        await test.State.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await test.Lifetime.StopRequested.Task.WaitAsync(TimeSpan.FromSeconds(3), Token);

        test.State.Disposed.Task.IsCompleted.ShouldBeFalse();
        await WaitForStatusAsync(test, "processing");
        (await new PgmqIdempotencyStore(fixture.DataSource)
            .TryAcquireAsync(test.Queue, test.Key, TimeSpan.FromSeconds(5), Token)).IsInProgress.ShouldBeTrue();

        test.State.Finish.TrySetResult();
        await test.State.Disposed.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await Should.ThrowAsync<PgmqHandlerUnresponsiveException>(() => test.Worker.ExecuteTask!);
    }

    [Fact]
    public async Task InvalidIdempotencyKey_ReachesDeadLetterWithoutStartingHandler()
    {
        var body = System.Text.Json.JsonSerializer.Serialize(new
        {
            idempotencyKey = new string('x', 513), payload = new { value = 1 }
        });
        await using var test = await StartWorkerAsync(TimeSpan.FromSeconds(4), body: body, maxAttempts: 1);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(3));
        IReadOnlyList<QueueDeadLetterMessage> deadLetters;
        do
        {
            deadLetters = (await fixture.CreateQueue().GetDeadLetterMessagesAsync(
                new() { Queue = test.Queue }, timeout.Token)).Messages;
            if (deadLetters.Count == 0)
                await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        } while (deadLetters.Count == 0);
        await test.Worker.StopAsync(Token);

        deadLetters.Count.ShouldBe(1);
        deadLetters[0].ErrorMessage.ShouldContain("512 characters");
        deadLetters[0].Topic.ShouldBe("worker.test");
        deadLetters[0].ConsumerName.ShouldBe(test.Queue);
        test.State.Started.Task.IsCompleted.ShouldBeFalse();
        (await fixture.Client.ReadAsync(test.Queue, 1, 10, Token)).ShouldBeEmpty();
    }

    [Fact]
    public async Task NamedConsumersOfSameMessageType_DispatchToTheirOwnHandlers()
    {
        var firstQueue = $"first_{Guid.NewGuid():N}";
        var secondQueue = $"second_{Guid.NewGuid():N}";
        await using (var connection = await fixture.DataSource.OpenConnectionAsync(Token))
        {
            await connection.ExecuteAsync(new CommandDefinition("SELECT pgmq.create(@firstQueue); SELECT pgmq.create(@secondQueue)",
                new { firstQueue, secondQueue }, cancellationToken: Token));
        }
        var state = new RoutingState();
        using var lifetime = new TestLifetime();
        var services = new ServiceCollection();
        services.AddSingleton(state);
        services.AddSingleton(fixture.Client);
        services.AddSingleton(new PgmqIdempotencyStore(fixture.DataSource));
        services.AddSingleton<QueueJsonSerializer>();
        services.AddSingleton<IHostApplicationLifetime>(lifetime);
        services.AddSingleton<ILogger<PgmqWorker<WorkerMessage>>>(NullLogger<PgmqWorker<WorkerMessage>>.Instance);
        services.AddSingleton<IOptions<PgmqOptions>>(Options.Create(new PgmqOptions
        {
            Consumers = new()
            {
                ["First"] = CreateConsumer(firstQueue, TimeSpan.FromSeconds(4)),
                ["Second"] = CreateConsumer(secondQueue, TimeSpan.FromSeconds(4))
            }
        }));
        services.AddQueueConsumer<WorkerMessage, FirstHandler>("First");
        services.AddQueueConsumer<WorkerMessage, SecondHandler>("Second");
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var workers = provider.GetServices<IHostedService>().ToArray();
        try
        {
            foreach (var worker in workers)
                await worker.StartAsync(Token);
            var queue = fixture.CreateQueue();
            await queue.SendAsync(firstQueue, new WorkerMessage(1), new() { IdempotencyKey = "first" }, Token);
            await queue.SendAsync(secondQueue, new WorkerMessage(2), new() { IdempotencyKey = "second" }, Token);

            (await state.First.Task.WaitAsync(TimeSpan.FromSeconds(3), Token)).ShouldBeTrue();
            (await state.Second.Task.WaitAsync(TimeSpan.FromSeconds(3), Token)).ShouldBeTrue();
        }
        finally
        {
            foreach (var worker in workers)
                await worker.StopAsync(Token);
        }
    }

    [Fact]
    public async Task BatchLargerThanConcurrency_DoesNotClaimWaitingMessages()
    {
        await using var test = await StartWorkerAsync(TimeSpan.FromSeconds(4));
        await test.State.Started.Task.WaitAsync(TimeSpan.FromSeconds(5), Token);
        await fixture.Client.SendAsync(test.Queue, "{}", Token);
        await fixture.Client.SendAsync(test.Queue, "{}", Token);

        var waiting = await fixture.Client.ReadAsync(test.Queue, 1, 10, Token);

        waiting.Count.ShouldBe(2);
        waiting.ShouldAllBe(message => message.ReadCount == 1);
    }

    private async Task<TestWorker> StartWorkerAsync(TimeSpan maxDuration, bool ignoreCancellation = false,
        string? body = null, int maxAttempts = 5)
    {
        var queue = $"worker_{Guid.NewGuid():N}";
        var key = $"worker-key:{Guid.NewGuid():N}";
        await using (var connection = await fixture.DataSource.OpenConnectionAsync(Token))
        {
            await connection.ExecuteAsync(new CommandDefinition("SELECT pgmq.create(@queue);",
                new { queue }, cancellationToken: Token));
        }
        var serializer = new QueueJsonSerializer();
        await fixture.Client.SendAsync(queue, body ?? serializer.Serialize(new WorkerMessage(1),
            new QueueSendOptions { IdempotencyKey = key }), Token);
        var state = new HandlerState(ignoreCancellation);
        var lifetime = new TestLifetime();
        var services = new ServiceCollection();
        services.AddSingleton(state);
        services.AddScoped<ScopeProbe>();
        services.AddKeyedScoped<IQueueHandler<WorkerMessage>, TestHandler>(queue);
        var provider = services.BuildServiceProvider();
        var consumer = CreateConsumer(queue, maxDuration);
        consumer.MaxAttempts = maxAttempts;
        var worker = new PgmqWorker<WorkerMessage>(queue, fixture.Client, new PgmqIdempotencyStore(fixture.DataSource),
            serializer, provider.GetRequiredService<IServiceScopeFactory>(), lifetime,
            Options.Create(new PgmqOptions { Consumers = new() { [queue] = consumer } }),
            NullLogger<PgmqWorker<WorkerMessage>>.Instance);
        await worker.StartAsync(Token);
        return new TestWorker(queue, key, worker, state, lifetime, provider);
    }

    private async Task WaitForStatusAsync(TestWorker test, string? expected)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(3));
        await using var connection = await fixture.DataSource.OpenConnectionAsync(timeout.Token);
        while (true)
        {
            var status = await connection.ExecuteScalarAsync<string?>(new CommandDefinition("""
                SELECT status FROM monixone_queue.idempotency_keys
                WHERE consumer_name = @consumer AND idempotency_key = @key;
                """, new { consumer = test.Queue, key = test.Key }, cancellationToken: timeout.Token));
            if (status == expected)
                return;
            await Task.Delay(TimeSpan.FromMilliseconds(20), timeout.Token);
        }
    }

    private static PgmqConsumerOptions CreateConsumer(string queue, TimeSpan maxDuration) => new()
    {
        Queue = queue, BatchSize = 10, Concurrency = 1,
        VisibilityTimeout = TimeSpan.FromSeconds(1), VisibilityRenewalInterval = TimeSpan.FromMilliseconds(100),
        MaxProcessingDuration = maxDuration, HandlerCancellationGracePeriod = TimeSpan.FromMilliseconds(150),
        QueueOperationTimeout = TimeSpan.FromMilliseconds(300), IdempotencyLease = TimeSpan.FromSeconds(6),
        PollingInterval = TimeSpan.FromMilliseconds(20), MaxAttempts = 5, RetryDelays = [TimeSpan.FromSeconds(30)]
    };

    private sealed class RoutingState
    {
        public TaskCompletionSource<bool> First { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<bool> Second { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class FirstHandler(RoutingState state) : IQueueHandler<WorkerMessage>
    {
        public Task HandleAsync(WorkerMessage message, CancellationToken cancellationToken)
        {
            state.First.TrySetResult(message.Value == 1 && cancellationToken.CanBeCanceled);
            return Task.CompletedTask;
        }
    }

    private sealed class SecondHandler(RoutingState state) : IQueueHandler<WorkerMessage>
    {
        public Task HandleAsync(WorkerMessage message, CancellationToken cancellationToken)
        {
            state.Second.TrySetResult(message.Value == 2 && cancellationToken.CanBeCanceled);
            return Task.CompletedTask;
        }
    }

    [QueueMessage("worker.test", Version = 1)]
    private sealed record WorkerMessage(int Value);

    private sealed class HandlerState(bool ignoreCancellation)
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Finish { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Disposed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public async Task HandleAsync(CancellationToken ct)
        {
            Started.TrySetResult();
            if (ignoreCancellation)
                await Finish.Task;
            else
                await Finish.Task.WaitAsync(ct);
        }
    }

    private sealed class TestHandler(HandlerState state, ScopeProbe probe) : IQueueHandler<WorkerMessage>
    {
        public Task HandleAsync(WorkerMessage message, CancellationToken cancellationToken)
        {
            _ = probe;
            return state.HandleAsync(cancellationToken);
        }
    }

    private sealed class ScopeProbe(HandlerState state) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() { state.Disposed.TrySetResult(); return ValueTask.CompletedTask; }
    }

    private sealed class TestLifetime : IHostApplicationLifetime, IDisposable
    {
        private readonly CancellationTokenSource _stopping = new();
        public TaskCompletionSource StopRequested { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => _stopping.Token;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() { _stopping.Cancel(); StopRequested.TrySetResult(); }
        public void Dispose() => _stopping.Dispose();
    }

    private sealed record TestWorker(string Queue, string Key, PgmqWorker<WorkerMessage> Worker, HandlerState State,
        TestLifetime Lifetime, ServiceProvider Provider) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            State.Finish.TrySetResult();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await Worker.StopAsync(timeout.Token);
            await Provider.DisposeAsync();
            Worker.Dispose();
            Lifetime.Dispose();
        }
    }
}
