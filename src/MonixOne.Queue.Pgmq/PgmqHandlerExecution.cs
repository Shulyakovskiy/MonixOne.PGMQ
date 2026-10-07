using Microsoft.Extensions.Logging;

namespace MonixOne.Queue.Pgmq;

/// <summary>
/// Coordinates handler lifetime and heartbeat without holding database resources or disposing a
/// live handler's scope.
/// </summary>
internal static class PgmqHandlerExecution
{
    public static async Task RunAsync(
        Func<CancellationToken, Task> handle,
        Func<CancellationToken, Task> renew,
        TimeSpan renewalInterval,
        TimeSpan cancellationGracePeriod,
        ILogger logger,
        CancellationToken cancellationToken,
        TimeSpan? heartbeatShutdownTimeout = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // The host token only signals the watchdog. Arbitrary cancellation callbacks run through
        // CancelAsync and cannot block Host.StopAsync synchronously.
        var handlerCancellation = new CancellationTokenSource();
        var heartbeatCancellation = new CancellationTokenSource();
        var stopSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stopRegistration = cancellationToken.UnsafeRegister(
            static state => ((TaskCompletionSource)state!).TrySetResult(), stopSignal);
        var handlerToken = handlerCancellation.Token;
        // Isolate synchronous handler code and cancel queued work before it starts.
        var handlerTask = Task.Run(() => handle(handlerToken), handlerToken);
        var heartbeatTask = HeartbeatAsync(renew, renewalInterval, heartbeatCancellation.Token);
        var processing = Task.CompletedTask;
        try
        {
            await Task.WhenAny(handlerTask, heartbeatTask, stopSignal.Task);
            var heartbeatCancellationTask = heartbeatCancellation.CancelAsync();
            var handlerCancellationTask = handlerTask.IsCompleted ? Task.CompletedTask : handlerCancellation.CancelAsync();
            var handlerDrain = Task.WhenAll(handlerTask, handlerCancellationTask);
            var heartbeatDrain = Task.WhenAll(heartbeatTask, heartbeatCancellationTask);
            processing = Task.WhenAll(handlerDrain, heartbeatDrain);

            // Waits run concurrently, each with its own bound. Expired processing/host tokens must
            // not interrupt the drain before scopes and in-flight SQL are released.
            await Task.WhenAll(
                DrainAsync(handlerDrain, handlerCancellationTask, cancellationGracePeriod, "handler", logger),
                DrainAsync(heartbeatDrain, heartbeatCancellationTask,
                    heartbeatShutdownTimeout ?? cancellationGracePeriod, "heartbeat", logger));

            // Prefer an ownership failure to the handler cancellation it caused.
            await heartbeatTask;
            cancellationToken.ThrowIfCancellationRequested();
            await handlerTask;
        }
        finally
        {
            if (processing.IsCompleted)
            {
                _ = processing.Exception;
                handlerCancellation.Dispose();
                heartbeatCancellation.Dispose();
            }
            else
            {
                // A detached task still owns its scope, token sources and cancellation callbacks.
                // Release them only after actual completion; the caller retains the lease.
                _ = ObserveLateProcessingAsync(processing, handlerCancellation, heartbeatCancellation, logger);
            }
        }
    }

    private static async Task DrainAsync(Task drain, Task cancellation, TimeSpan timeout, string component, ILogger logger)
    {
        try
        {
            // WaitAsync bounds the wait, not the task itself. A live handler cannot safely release
            // its scope or lease.
            await drain.WaitAsync(timeout);
        }
        catch (TimeoutException) when (!drain.IsCompleted)
        {
            throw new PgmqHandlerUnresponsiveException(timeout, component);
        }
        catch (Exception)
        {
            // Work failures are propagated after both drains. Callback failures must not bypass
            // resource cleanup or hide the original ownership/handler error.
            if (cancellation.IsFaulted)
                logger.LogError(cancellation.Exception, "PGMQ {Component} cancellation callback failed.", component);
        }
    }

    private static async Task HeartbeatAsync(Func<CancellationToken, Task> renew, TimeSpan interval, CancellationToken token)
    {
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(token))
                await renew(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { }
    }

    private static async Task ObserveLateProcessingAsync(Task task, CancellationTokenSource handlerCancellation,
        CancellationTokenSource heartbeatCancellation, ILogger logger)
    {
        try
        { await task; }
        catch (OperationCanceledException) { }
        catch (Exception exception) { logger.LogError(exception, "PGMQ abandoned processing eventually failed during application shutdown."); }
        finally
        {
            handlerCancellation.Dispose();
            heartbeatCancellation.Dispose();
        }
    }
}

internal sealed class PgmqLeaseLostException(Exception? innerException = null)
    : Exception("The PGMQ delivery lease was lost.", innerException);

internal sealed class PgmqHandlerUnresponsiveException(TimeSpan gracePeriod, string component = "handler")
    : Exception($"PGMQ {component} did not stop within the cancellation grace period of {gracePeriod}.");
