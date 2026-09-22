using Dapper;
using Npgsql;
using System.Data.Common;

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
        // PGMQ atomically claims messages and advances their visibility timeout; no application-side lock is needed.
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return (await connection.QueryAsync<PgmqMessage>(new CommandDefinition(
            """
            SELECT msg_id AS Id, read_ct AS ReadCount, message::text AS Body
            FROM pgmq.read(@queue, @visibilityTimeoutSeconds, @batchSize)
            """,
            new { queue, visibilityTimeoutSeconds, batchSize },
            cancellationToken: cancellationToken))).AsList();
    }

    public async Task DeleteAsync(string queue, long messageId, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pgmq.delete(@queue, @messageId)",
            new { queue, messageId },
            cancellationToken: cancellationToken));
    }

    public async Task SetVisibilityAsync(string queue, long messageId, TimeSpan delay,
        CancellationToken cancellationToken)
    {
        // A failed delivery remains in the source queue and becomes eligible only after the selected retry delay.
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT * FROM pgmq.set_vt(@queue, @messageId, @visibilityTimeoutSeconds)",
            new
            {
                queue,
                messageId,
                visibilityTimeoutSeconds = Math.Max(1, (int)Math.Ceiling(delay.TotalSeconds))
            },
            cancellationToken: cancellationToken));
    }

    public async Task MoveToDeadLetterAsync(string queue, long messageId, string deadLetterQueue, string body,
        CancellationToken cancellationToken)
    {
        // Send and source acknowledgement share one transaction: never acknowledge before the DLQ copy exists.
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pgmq.send(@deadLetterQueue, @body::jsonb); SELECT pgmq.delete(@queue, @messageId);",
            new { deadLetterQueue, body, queue, messageId },
            transaction,
            cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
    }
}
