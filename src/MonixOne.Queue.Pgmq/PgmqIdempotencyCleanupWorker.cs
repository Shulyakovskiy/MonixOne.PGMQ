using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqIdempotencyCleanupWorker(
    PgmqIdempotencyStore idempotencyStore,
    IOptions<PgmqOptions> options,
    ILogger<PgmqIdempotencyCleanupWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var deleted = await DeleteCompletedAsync(stoppingToken);
                if (deleted > 0)
                {
                    logger.LogInformation(
                        "PGMQ completed idempotency keys deleted. Count {Count}, RetentionHours {RetentionHours}.",
                        deleted,
                        options.Value.CompletedIdempotencyRetention.TotalHours);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "PGMQ completed idempotency key cleanup failed.");
            }

            await Task.Delay(options.Value.IdempotencyCleanupInterval, stoppingToken);
        }
    }

    private async Task<int> DeleteCompletedAsync(CancellationToken cancellationToken)
    {
        var cleanupOptions = options.Value;
        var completedBefore = DateTimeOffset.UtcNow - cleanupOptions.CompletedIdempotencyRetention;
        var deleted = 0;
        int batchDeleted;

        do
        {
            batchDeleted = await idempotencyStore.DeleteCompletedAsync(
                completedBefore,
                cleanupOptions.IdempotencyCleanupBatchSize,
                cancellationToken);
            deleted += batchDeleted;
        }
        while (batchDeleted == cleanupOptions.IdempotencyCleanupBatchSize);

        return deleted;
    }
}
