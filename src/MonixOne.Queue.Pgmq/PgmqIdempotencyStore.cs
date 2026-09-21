using Dapper;
using Npgsql;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqIdempotencyStore(NpgsqlDataSource dataSource)
{
    public async Task<IdempotencyClaim> TryAcquireAsync(
        string consumerName,
        string idempotencyKey,
        TimeSpan lease,
        CancellationToken cancellationToken)
    {
        var leaseToken = Guid.NewGuid();
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var acquired = await connection.ExecuteScalarAsync<Guid?>(new CommandDefinition("""
            INSERT INTO monixone_queue.idempotency_keys AS keys
                (consumer_name, idempotency_key, status, lease_token, lease_expires_at, created_at, updated_at)
            VALUES
                (@consumerName, @idempotencyKey, 'processing', @leaseToken, now() + @lease, now(), now())
            ON CONFLICT (consumer_name, idempotency_key) DO UPDATE SET
                lease_token = EXCLUDED.lease_token,
                lease_expires_at = EXCLUDED.lease_expires_at,
                updated_at = EXCLUDED.updated_at
            WHERE keys.status = 'processing' AND keys.lease_expires_at <= now()
            RETURNING lease_token;
            """,
            new { consumerName, idempotencyKey, leaseToken, lease },
            cancellationToken: cancellationToken));
        if (acquired is not null)
        {
            return IdempotencyClaim.Acquired(acquired.Value);
        }

        var existing = await connection.QuerySingleAsync<IdempotencyState>(new CommandDefinition("""
            SELECT status AS Status, lease_expires_at AS LeaseExpiresAt
            FROM monixone_queue.idempotency_keys
            WHERE consumer_name = @consumerName AND idempotency_key = @idempotencyKey;
            """,
            new { consumerName, idempotencyKey },
            cancellationToken: cancellationToken));

        return existing.Status == "completed"
            ? IdempotencyClaim.Completed
            : IdempotencyClaim.InProgress(existing.LeaseExpiresAt);
    }

    public async Task<bool> CompleteAsync(
        string consumerName,
        string idempotencyKey,
        Guid leaseToken,
        CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var updated = await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE monixone_queue.idempotency_keys
            SET status = 'completed', lease_token = NULL, lease_expires_at = NULL, completed_at = now(), updated_at = now()
            WHERE consumer_name = @consumerName
              AND idempotency_key = @idempotencyKey
              AND status = 'processing'
              AND lease_token = @leaseToken;
            """,
            new { consumerName, idempotencyKey, leaseToken },
            cancellationToken: cancellationToken));
        return updated == 1;
    }

    public async Task<int> DeleteCompletedAsync(
        DateTimeOffset completedBefore,
        int batchSize,
        CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition("""
            WITH completed AS (
                SELECT ctid
                FROM monixone_queue.idempotency_keys
                WHERE status = 'completed'
                  AND completed_at < @completedBefore
                ORDER BY completed_at
                LIMIT @batchSize
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM monixone_queue.idempotency_keys AS keys
            USING completed
            WHERE keys.ctid = completed.ctid;
            """,
            new { completedBefore, batchSize },
            cancellationToken: cancellationToken));
    }

    public async Task ReleaseAsync(string consumerName, string idempotencyKey, Guid leaseToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(CancellationToken.None);
        await connection.ExecuteAsync(new CommandDefinition("""
            DELETE FROM monixone_queue.idempotency_keys
            WHERE consumer_name = @consumerName
              AND idempotency_key = @idempotencyKey
              AND status = 'processing'
              AND lease_token = @leaseToken;
            """,
            new { consumerName, idempotencyKey, leaseToken }));
    }

    private sealed record IdempotencyState(string Status, DateTime? LeaseExpiresAt);
}

internal readonly record struct IdempotencyClaim(Guid? LeaseToken, bool IsCompleted, DateTime? LeaseExpiresAt)
{
    public bool IsAcquired => LeaseToken is not null;
    public bool IsInProgress => !IsAcquired && !IsCompleted;

    public static IdempotencyClaim Acquired(Guid leaseToken) => new(leaseToken, false, null);
    public static IdempotencyClaim Completed => new(null, true, null);
    public static IdempotencyClaim InProgress(DateTime? leaseExpiresAt) => new(null, false, leaseExpiresAt);
}
