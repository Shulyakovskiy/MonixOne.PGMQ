using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

public sealed class PgmqHandlerExecutionTests
{
    [Fact]
    public async Task Success_InterruptsInFlightRenewalAndDisposesHandlerResources()
    {
        var finishHandler = NewSignal();
        var renewalStarted = NewSignal();
        var renewalStopped = NewSignal();
        var disposed = false;
        var task = PgmqHandlerExecution.RunAsync(
            async _ => { try { await finishHandler.Task; } finally { disposed = true; } },
            async ct =>
            {
                renewalStarted.TrySetResult();
                try
                { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
                finally { renewalStopped.TrySetResult(); }
            }, TimeSpan.FromMilliseconds(10), TimeSpan.FromSeconds(1), NullLogger.Instance,
            TestContext.Current.CancellationToken);

        await renewalStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        finishHandler.TrySetResult();
        await task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        renewalStopped.Task.IsCompleted.ShouldBeTrue();
        disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task Cancellation_WaitsForCooperativeHandlerCleanup()
    {
        using var cancellation = new CancellationTokenSource();
        var started = NewSignal();
        var disposed = false;
        var task = PgmqHandlerExecution.RunAsync(async ct =>
        {
            try
            { started.TrySetResult(); await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
            finally { disposed = true; }
        }, _ => Task.CompletedTask, TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(1), NullLogger.Instance, cancellation.Token);

        await started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));

        disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task RenewalFailure_CancelsHandlerBeforePropagatingLostLease()
    {
        var disposed = false;
        var task = PgmqHandlerExecution.RunAsync(async ct =>
        {
            try
            { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
            finally { disposed = true; }
        }, _ => throw new PgmqLeaseLostException(), TimeSpan.FromMilliseconds(10), TimeSpan.FromSeconds(1),
            NullLogger.Instance, TestContext.Current.CancellationToken);

        await Should.ThrowAsync<PgmqLeaseLostException>(() => task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task ThrowingCancellationCallback_StillDrainsHandlerResourcesBeforeReturning()
    {
        var started = NewSignal();
        var disposed = false;
        var task = PgmqHandlerExecution.RunAsync(async ct =>
        {
            using var registration = ct.Register(() => throw new InvalidOperationException("callback failed"));
            try
            {
                started.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            finally
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken);
                disposed = true;
            }
        }, async ct =>
        {
            await started.Task.WaitAsync(ct);
            throw new PgmqLeaseLostException();
        }, TimeSpan.FromMilliseconds(10), TimeSpan.FromSeconds(1), NullLogger.Instance,
            TestContext.Current.CancellationToken);

        await Should.ThrowAsync<PgmqLeaseLostException>(() => task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task BlockingCancellationCallback_DoesNotBlockHostCancellationOrTheGraceLimit()
    {
        using var cancellation = new CancellationTokenSource();
        var started = NewSignal();
        var releaseCallback = NewSignal();
        var stopped = NewSignal();
        var task = PgmqHandlerExecution.RunAsync(async ct =>
        {
            var registration = ct.Register(() => releaseCallback.Task.GetAwaiter().GetResult());
            try
            {
                started.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            finally
            {
                registration.Dispose();
                stopped.TrySetResult();
            }
        }, _ => Task.CompletedTask, TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(50),
            NullLogger.Instance, cancellation.Token);

        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await cancellation.CancelAsync().WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
            await Should.ThrowAsync<PgmqHandlerUnresponsiveException>(() => task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
            stopped.Task.IsCompleted.ShouldBeFalse();
        }
        finally
        {
            releaseCallback.TrySetResult();
            await stopped.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task UncooperativeHeartbeat_BoundsShutdownAndStillDrainsHandler()
    {
        using var cancellation = new CancellationTokenSource();
        var renewalStarted = NewSignal();
        var finishRenewal = NewSignal();
        var handlerStopped = NewSignal();
        var task = PgmqHandlerExecution.RunAsync(async ct =>
        {
            try
            { await Task.Delay(Timeout.InfiniteTimeSpan, ct); }
            finally { handlerStopped.TrySetResult(); }
        }, async _ =>
        {
            renewalStarted.TrySetResult();
            await finishRenewal.Task;
        }, TimeSpan.FromMilliseconds(10), TimeSpan.FromSeconds(1), NullLogger.Instance,
            cancellation.Token, TimeSpan.FromMilliseconds(50));

        try
        {
            await renewalStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await cancellation.CancelAsync();
            await Should.ThrowAsync<PgmqHandlerUnresponsiveException>(() => task.WaitAsync(
                TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
            handlerStopped.Task.IsCompleted.ShouldBeTrue();
        }
        finally { finishRenewal.TrySetResult(); }
    }

    [Fact]
    public async Task DetachedHandler_KeepsCancellationSourceAliveUntilItsCleanupCompletes()
    {
        using var cancellation = new CancellationTokenSource();
        var started = NewSignal();
        var finish = NewSignal();
        var cleanup = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var task = PgmqHandlerExecution.RunAsync(async ct =>
        {
            started.TrySetResult();
            await finish.Task;
            try
            { cleanup.TrySetResult(ct.WaitHandle.WaitOne(0)); }
            catch (Exception exception) { cleanup.TrySetException(exception); }
        }, _ => Task.CompletedTask, TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(50),
            NullLogger.Instance, cancellation.Token);

        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await cancellation.CancelAsync();
            await Should.ThrowAsync<PgmqHandlerUnresponsiveException>(() => task.WaitAsync(
                TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
        }
        finally { finish.TrySetResult(); }

        (await cleanup.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task UncooperativeHandler_BoundsWaitWithoutDisposingResourcesStillInUse()
    {
        using var cancellation = new CancellationTokenSource();
        var started = NewSignal();
        var finish = NewSignal();
        var disposed = NewSignal();
        var task = PgmqHandlerExecution.RunAsync(async _ =>
        {
            try
            { started.TrySetResult(); await finish.Task; }
            finally { disposed.TrySetResult(); }
        }, _ => Task.CompletedTask, TimeSpan.FromSeconds(20), TimeSpan.FromMilliseconds(50),
            NullLogger.Instance, cancellation.Token);

        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            await cancellation.CancelAsync();
            await Should.ThrowAsync<PgmqHandlerUnresponsiveException>(() => task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
            disposed.Task.IsCompleted.ShouldBeFalse();
        }
        finally
        {
            finish.TrySetResult();
            await disposed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
