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
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // A replacement lease starts after acquiring the conflicting row's lock. EXCLUDED's
            // timestamp would consume the new lease while waiting for the previous transaction.
            var acquired = await connection.ExecuteScalarAsync<Guid?>(new CommandDefinition("""
                INSERT INTO monixone_queue.idempotency_keys AS keys
                    (consumer_name, idempotency_key, status, lease_token, lease_expires_at, created_at, updated_at)
                VALUES
                    (@consumerName, @idempotencyKey, 'processing', @leaseToken, clock_timestamp() + @lease, now(), now())
                ON CONFLICT (consumer_name, idempotency_key) DO UPDATE SET
                    lease_token = EXCLUDED.lease_token,
                    lease_expires_at = clock_timestamp() + @lease,
                    updated_at = clock_timestamp()
                WHERE keys.status = 'processing' AND keys.lease_expires_at <= clock_timestamp()
                RETURNING lease_token;
                """,
                new { consumerName, idempotencyKey, leaseToken, lease },
                cancellationToken: cancellationToken));
            if (acquired is not null)
                return IdempotencyClaim.Acquired(acquired.Value);

            var status = await connection.QuerySingleOrDefaultAsync<string>(new CommandDefinition("""
                SELECT status FROM monixone_queue.idempotency_keys
                WHERE consumer_name = @consumerName AND idempotency_key = @idempotencyKey;
                """,
                new { consumerName, idempotencyKey },
                cancellationToken: cancellationToken));
            if (status is not null)
                return status == "completed" ? IdempotencyClaim.Completed : IdempotencyClaim.InProgress;

            // Retry a concurrent release/retention deletion between the two statements. The caller's
            // operation token bounds the loop; no transaction or row lock spans these reads.
        }
    }

    public async Task<bool> CompleteAsync(
        string consumerName,
        string idempotencyKey,
        Guid leaseToken,
        CancellationToken cancellationToken,
        DateTime? deadline = null)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var updated = await connection.ExecuteAsync(new CommandDefinition("""
            UPDATE monixone_queue.idempotency_keys
            SET status = 'completed', lease_token = NULL, lease_expires_at = NULL, completed_at = now(), updated_at = now()
            WHERE consumer_name = @consumerName
              AND idempotency_key = @idempotencyKey
              AND status = 'processing'
              AND lease_token = @leaseToken
              AND lease_expires_at > clock_timestamp()
              AND (@deadline::timestamptz IS NULL OR clock_timestamp() < @deadline);
            """,
            new { consumerName, idempotencyKey, leaseToken, deadline },
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

    public async Task ReleaseAsync(string consumerName, string idempotencyKey, Guid leaseToken, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition("""
            DELETE FROM monixone_queue.idempotency_keys
            WHERE consumer_name = @consumerName
              AND idempotency_key = @idempotencyKey
              AND status = 'processing'
              AND lease_token = @leaseToken;
            """,
            new { consumerName, idempotencyKey, leaseToken },
            cancellationToken: cancellationToken));
    }
}

internal readonly record struct IdempotencyClaim(Guid? LeaseToken, bool IsCompleted)
{
    public bool IsAcquired => LeaseToken is not null;
    public bool IsInProgress => !IsAcquired && !IsCompleted;

    public static IdempotencyClaim Acquired(Guid leaseToken) => new(leaseToken, false);
    public static IdempotencyClaim Completed => new(null, true);
    public static IdempotencyClaim InProgress => new(null, false);
}
