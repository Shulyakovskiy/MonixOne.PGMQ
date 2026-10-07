using Dapper;
using Npgsql;
using System.Data.Common;
using System.Diagnostics;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqClient(NpgsqlDataSource dataSource)
{
    public async Task<long> SendAsync(string queue, string body, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.ExecuteScalarAsync<long>(new CommandDefinition(
            "SELECT pgmq.send(@queue, @body::jsonb)",
            new { queue, body },
            cancellationToken: cancellationToken));
    }

    public Task<long> SendAsync(
        string queue,
        string body,
        DbTransaction transaction,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The caller transaction is not associated with an open connection.");

        return connection.ExecuteScalarAsync<long>(new CommandDefinition(
            "SELECT pgmq.send(@queue, @body::jsonb)",
            new { queue, body },
            transaction,
            cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<PgmqMessage>> ReadAsync(string queue, int visibilityTimeoutSeconds, int batchSize,
        CancellationToken cancellationToken)
    {
        var readStartedTimestamp = Stopwatch.GetTimestamp();
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return (await connection.QueryAsync<PgmqMessage>(new CommandDefinition(
            """
            SELECT msg_id AS Id, read_ct AS ReadCount, message::text AS Body,
                   last_read_at AS ReadAt, vt AS VisibleUntil
            FROM pgmq.read(@queue, @visibilityTimeoutSeconds, @batchSize)
            """,
            new { queue, visibilityTimeoutSeconds, batchSize },
            cancellationToken: cancellationToken)))
            .Select(message => message with { ReadStartedTimestamp = readStartedTimestamp }).ToArray();
    }

    public async Task SendBatchAsync(string queue, string[] bodies, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition("SELECT pgmq.send_batch(@queue, @bodies::jsonb[])",
            new { queue, bodies }, cancellationToken: cancellationToken));
    }

    public Task SendBatchAsync(string queue, string[] bodies, DbTransaction transaction, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The caller transaction is not associated with an open connection.");
        return connection.ExecuteAsync(new CommandDefinition("SELECT pgmq.send_batch(@queue, @bodies::jsonb[])",
            new { queue, bodies }, transaction, cancellationToken: cancellationToken));
    }

    public async Task DeleteAsync(string queue, long messageId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pgmq.delete(@queue, @messageId)",
            new { queue, messageId },
            cancellationToken: cancellationToken));
    }

    public async Task<IReadOnlyList<PgmqDeadLetterRow>> GetDeadLetterMessagesAsync(
        string deadLetterQueue, QueueDeadLetterQuery query, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var table = await connection.ExecuteScalarAsync<string>(new CommandDefinition(
            "SELECT format('pgmq.%I', pgmq.format_table_name(@deadLetterQueue, 'q'))",
            new { deadLetterQueue }, cancellationToken: cancellationToken));
        // Inspection includes invisible entries and never increments read_ct or sets vt. The extra
        // row determines whether a next page exists without an expensive total count.
        return (await connection.QueryAsync<PgmqDeadLetterRow>(new CommandDefinition($"""
            SELECT msg_id AS Id, message::text AS Body FROM {table}
            WHERE msg_id > @afterId
              AND (@queue::text IS NULL OR message->>'queue' = @queue)
              AND (@topic::text IS NULL OR message->>'topic' = @topic)
            ORDER BY msg_id
            LIMIT @limit;
            """, new { afterId = query.AfterId, queue = query.Queue, topic = query.Topic, limit = query.PageSize + 1 },
            cancellationToken: cancellationToken))).AsList();
    }

    public Task<bool> TryRenewVisibilityAsync(
        string queue, PgmqMessage message, string consumerName, string idempotencyKey, Guid leaseToken,
        TimeSpan visibilityTimeout, DateTime deadline, CancellationToken cancellationToken)
    {
        // Only a live delivery owner can renew, and the absolute deadline caps every successive
        // heartbeat.
        return TryOperateDeliveryAsync(queue, message, consumerName, idempotencyKey, leaseToken, "processing",
            requireInvisible: true, deadline,
            table => $"""
                UPDATE {table}
                SET vt = LEAST(clock_timestamp() + @visibilityTimeout, @deadline)
                WHERE msg_id = @messageId;
                """, new { visibilityTimeout }, cancellationToken);
    }

    public Task<bool> TryDeleteCompletedAsync(
        string queue, PgmqMessage message, string consumerName, string idempotencyKey, CancellationToken cancellationToken)
    {
        return TryOperateDeliveryAsync(queue, message, consumerName, idempotencyKey, null, "completed",
            requireInvisible: false, deadline: null,
            table => $"DELETE FROM {table} WHERE msg_id = @messageId", null, cancellationToken);
    }

    public Task<bool> TryDeferConcurrentAsync(
        string queue, PgmqMessage message, string consumerName, string idempotencyKey,
        TimeSpan pollingInterval, CancellationToken cancellationToken)
    {
        // Use database time; the active owner's fixed lease bounds this deferral.
        return TryOperateDeliveryAsync(queue, message, consumerName, idempotencyKey, null, "processing",
            requireInvisible: false, deadline: null,
            table => $"""
                UPDATE {table}
                SET vt = GREATEST(clock_timestamp() + @pollingInterval,
                    (SELECT lease_expires_at FROM monixone_queue.idempotency_keys
                     WHERE consumer_name = @consumerName AND idempotency_key = @idempotencyKey))
                WHERE msg_id = @messageId;
                """, new { pollingInterval }, cancellationToken);
    }

    public Task<bool> TryRetryAsync(
        string queue, PgmqMessage message, string consumerName, string? idempotencyKey, Guid? leaseToken,
        TimeSpan delay, CancellationToken cancellationToken)
    {
        // Never release the idempotency key before scheduling retry: another replica could acquire
        // it in that gap.
        return TryOperateDeliveryAsync(queue, message, consumerName, idempotencyKey, leaseToken,
            leaseToken is null ? null : "processing", requireInvisible: false, deadline: null,
            table => $"""
                UPDATE {table} SET vt = clock_timestamp() + @delay WHERE msg_id = @messageId;
                DELETE FROM monixone_queue.idempotency_keys
                WHERE consumer_name = @consumerName AND idempotency_key = @idempotencyKey
                  AND status = 'processing' AND lease_token = @leaseToken;
                """, new { delay }, cancellationToken);
    }

    public Task<bool> TryMoveToDeadLetterAsync(
        string queue, string deadLetterQueue, PgmqMessage message, string consumerName, string? idempotencyKey, Guid? leaseToken,
        string body, CancellationToken cancellationToken)
    {
        // DLQ persistence, acknowledgement and lease release commit or roll back together.
        return TryOperateDeliveryAsync(queue, message, consumerName, idempotencyKey, leaseToken,
            leaseToken is null ? null : "processing", requireInvisible: false, deadline: null,
            table => $"""
                SELECT pgmq.send(@deadLetterQueue, @body::jsonb || jsonb_build_object(
                    'queue', @queue, 'consumerName', @consumerName,
                    'topic', COALESCE(@body::jsonb->>'topic', 'unknown')));
                DELETE FROM {table} WHERE msg_id = @messageId;
                DELETE FROM monixone_queue.idempotency_keys
                WHERE consumer_name = @consumerName AND idempotency_key = @idempotencyKey
                  AND status = 'processing' AND lease_token = @leaseToken;
                """, new { deadLetterQueue, body }, cancellationToken);
    }

    private async Task<bool> TryOperateDeliveryAsync(
        string queue, PgmqMessage message, string consumerName, string? idempotencyKey, Guid? leaseToken,
        string? expectedStatus, bool requireInvisible, DateTime? deadline,
        Func<string, string> commandText, object? operationParameters, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var parameters = new
        {
            queue, messageId = message.Id, readCount = message.ReadCount, consumerName, idempotencyKey,
            leaseToken, expectedStatus, requireInvisible, deadline
        };

        // Lock order is always idempotency key, then physical delivery. No connection or lock spans
        // handler execution.
        // PostgreSQL supplies a quoted identifier; no caller input is interpolated into SQL.
        var table = expectedStatus is null
            ? await connection.ExecuteScalarAsync<string>(new CommandDefinition(
                "SELECT format('pgmq.%I', pgmq.format_table_name(@queue, 'q'))", parameters, transaction,
                cancellationToken: cancellationToken))
            : await connection.ExecuteScalarAsync<string?>(new CommandDefinition("""
                SELECT format('pgmq.%I', pgmq.format_table_name(@queue, 'q'))
                FROM monixone_queue.idempotency_keys
                WHERE consumer_name = @consumerName AND idempotency_key = @idempotencyKey
                FOR UPDATE;
                """, parameters, transaction, cancellationToken: cancellationToken));
        if (table is null)
            return false;

        var readCount = await connection.ExecuteScalarAsync<int?>(new CommandDefinition($"""
            SELECT read_ct FROM {table} WHERE msg_id = @messageId FOR UPDATE;
            """, parameters, transaction, cancellationToken: cancellationToken));
        // Visibility may have expired during cancellation cleanup. The generation must still match
        // even for retry and DLQ.
        if (readCount != message.ReadCount)
            return false;

        // Recheck ownership and wall time AFTER acquiring both locks, including waits for
        // concurrent SQL operations.
        var allowed = await connection.ExecuteScalarAsync<bool>(new CommandDefinition($"""
            SELECT EXISTS (
                SELECT 1 FROM {table} WHERE msg_id = @messageId
                  AND (NOT @requireInvisible OR vt > clock_timestamp())
                  AND (@deadline::timestamptz IS NULL OR clock_timestamp() < @deadline)
            ) AND (
                @expectedStatus::text IS NULL OR EXISTS (
                    SELECT 1 FROM monixone_queue.idempotency_keys
                    WHERE consumer_name = @consumerName AND idempotency_key = @idempotencyKey
                      AND status = @expectedStatus
                      AND (@expectedStatus = 'completed' OR (
                          lease_expires_at > clock_timestamp()
                          AND (@leaseToken::uuid IS NULL OR lease_token = @leaseToken)))
                )
            );
            """, parameters, transaction, cancellationToken: cancellationToken));
        if (!allowed)
            return false;

        var commandParameters = new DynamicParameters(parameters);
        commandParameters.AddDynamicParams(operationParameters);
        await connection.ExecuteAsync(new CommandDefinition(commandText(table), commandParameters, transaction,
            cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return true;
    }
}

internal sealed record PgmqDeadLetterRow(long Id, string Body);
