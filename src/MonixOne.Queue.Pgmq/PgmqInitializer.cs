using Dapper;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqInitializer(
    NpgsqlDataSource dataSource,
    IOptions<PgmqOptions> options,
    ILogger<PgmqInitializer> logger) : IHostedService
{
    private const int MigrationBatchSize = 100;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        PgmqDeploymentScripts.ValidateSourceArchive();
        var queueNames = options.Value.Consumers.Values
            .Select(consumer => consumer.Queue)
            .Append(options.Value.DeadLetterQueue)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        logger.LogInformation(
            "PGMQ provisioning started. TargetVersion {TargetVersion}, ConsumerCount {ConsumerCount}.",
            PgmqDeploymentScripts.PgmqVersion,
            options.Value.Consumers.Count);
        await ProvisionAsync(queueNames, cancellationToken);

        // Provisioning releases its connection before migration opens bounded batch transactions.
        // Concurrent replicas skip each other's locked rows; committed batches never move twice.
        foreach (var sourceQueue in options.Value.Consumers.Values.Select(consumer => consumer.Queue)
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            long totalMoved = 0;
            int moved;
            do
            {
                using var batchCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                batchCancellation.CancelAfter(options.Value.Defaults.QueueOperationTimeout);
                moved = await MigrateLegacyDeadLettersAsync(sourceQueue, options.Value.DeadLetterQueue, batchCancellation.Token);
                totalMoved += moved;
            } while (moved == MigrationBatchSize);
            if (totalMoved > 0)
                logger.LogInformation("PGMQ legacy DLQ entries migrated. Queue {Queue}, DeadLetterQueue {DeadLetterQueue}, Count {Count}.",
                    sourceQueue, options.Value.DeadLetterQueue, totalMoved);
        }
        logger.LogInformation("PGMQ {PgmqVersion} is ready. QueueCount {QueueCount}.",
            PgmqDeploymentScripts.PgmqVersion, queueNames.Length);
    }

    private async Task ProvisionAsync(IReadOnlyList<string> queueNames, CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await connection.ExecuteAsync(new CommandDefinition(
            "SELECT pg_advisory_xact_lock(hashtext('monixone.queue.pgmq.deployment'))",
            transaction: transaction,
            cancellationToken: cancellationToken));

        foreach (var script in PgmqDeploymentScripts.OrderedFiles)
        {
            await connection.ExecuteAsync(new CommandDefinition(
                PgmqDeploymentScripts.ReadSql(script),
                transaction: transaction,
                cancellationToken: cancellationToken));
        }

        var installedVersion = await connection.ExecuteScalarAsync<string?>(new CommandDefinition("""
            SELECT installed_version
            FROM monixone_queue.infrastructure_metadata
            WHERE component = 'pgmq'
            FOR UPDATE
            """,
            transaction: transaction,
            cancellationToken: cancellationToken));

        if (installedVersion is null)
        {
            var pgmqSchemaExists = await connection.ExecuteScalarAsync<bool>(new CommandDefinition(
                "SELECT to_regnamespace('pgmq') IS NOT NULL",
                transaction: transaction,
                cancellationToken: cancellationToken));
            if (pgmqSchemaExists)
            {
                throw new InvalidOperationException("The pgmq schema exists but has no package migration record. Add a migration record before enabling package-managed PGMQ SQL upgrades.");
            }

            logger.LogInformation("PGMQ initial SQL provisioning started. TargetVersion {TargetVersion}.", PgmqDeploymentScripts.PgmqVersion);
            await connection.ExecuteAsync(new CommandDefinition(
                PgmqDeploymentScripts.ReadInitialPgmqSql(),
                transaction: transaction,
                cancellationToken: cancellationToken));
        }
        else
        {
            logger.LogInformation(
                "PGMQ SQL upgrade started. InstalledVersion {InstalledVersion}, TargetVersion {TargetVersion}.",
                installedVersion,
                PgmqDeploymentScripts.PgmqVersion);
            foreach (var migration in PgmqDeploymentScripts.ReadUpgradeSql(installedVersion))
            {
                await connection.ExecuteAsync(new CommandDefinition(
                    migration,
                    transaction: transaction,
                    cancellationToken: cancellationToken));
            }
        }

        const string status = "installed";
        var targetVersion = PgmqDeploymentScripts.PgmqVersion;

        await connection.ExecuteAsync(new CommandDefinition("""
            INSERT INTO monixone_queue.infrastructure_metadata
                (component, status, expected_version, installed_version, source_url, source_revision, checked_at)
            VALUES ('pgmq', @status, @expectedVersion, @installedVersion, @sourceUrl, @sourceRevision, now())
            ON CONFLICT (component) DO UPDATE SET
                status = EXCLUDED.status,
                expected_version = EXCLUDED.expected_version,
                installed_version = EXCLUDED.installed_version,
                source_url = EXCLUDED.source_url,
                source_revision = EXCLUDED.source_revision,
                checked_at = EXCLUDED.checked_at;
            """,
            new
            {
                status,
                expectedVersion = targetVersion,
                installedVersion = targetVersion,
                sourceUrl = "package://MonixOne.Queue.Pgmq",
                sourceRevision = PgmqDeploymentScripts.Version
            },
            transaction: transaction,
            cancellationToken: cancellationToken));

        foreach (var queueName in queueNames)
        {
            await connection.ExecuteAsync(new CommandDefinition(
                "SELECT pgmq.create(@queueName)",
                new { queueName },
                transaction: transaction,
                cancellationToken: cancellationToken));
        }

        var dlq = await connection.QuerySingleAsync<DeadLetterTable>(new CommandDefinition("""
            SELECT format('pgmq.%I', pgmq.format_table_name(@deadLetterQueue, 'q')) AS TableName,
                   format('%I', 'ix_dlq_queue_' || md5(lower(@deadLetterQueue))) AS QueueIndexName,
                   format('%I', 'ix_dlq_topic_' || md5(lower(@deadLetterQueue))) AS TopicIndexName;
            """, new { deadLetterQueue = options.Value.DeadLetterQueue }, transaction, cancellationToken: cancellationToken));
        // Separate indexes support queue-only and topic-only inspections; msg_id also serves as the
        // cursor. Every interpolated identifier is quoted by PostgreSQL.
        await connection.ExecuteAsync(new CommandDefinition($"""
            CREATE INDEX IF NOT EXISTS {dlq.QueueIndexName} ON {dlq.TableName} ((message->>'queue'), msg_id);
            CREATE INDEX IF NOT EXISTS {dlq.TopicIndexName} ON {dlq.TableName} ((message->>'topic'), msg_id);
            """, transaction: transaction, cancellationToken: cancellationToken));

        await transaction.CommitAsync(cancellationToken);
    }

    private async Task<int> MigrateLegacyDeadLettersAsync(string sourceQueue, string deadLetterQueue,
        CancellationToken cancellationToken)
    {
        var legacyQueue = $"{sourceQueue}-dlq";
        var sameQueue = string.Equals(legacyQueue, deadLetterQueue, StringComparison.OrdinalIgnoreCase);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var table = await connection.ExecuteScalarAsync<string?>(new CommandDefinition("""
            SELECT format('pgmq.%I', pgmq.format_table_name(@legacyQueue, 'q'))
            WHERE to_regclass(format('pgmq.%I', pgmq.format_table_name(@legacyQueue, 'q'))) IS NOT NULL;
            """, new { legacyQueue }, transaction, cancellationToken: cancellationToken));
        if (table is null)
            return 0;

        var entries = (await connection.QueryAsync<PgmqDeadLetterRow>(new CommandDefinition($"""
            SELECT msg_id AS Id, message::text AS Body FROM {table}
            {(sameQueue ? "WHERE message->>'topic' IS NULL" : string.Empty)}
            ORDER BY msg_id LIMIT @limit FOR UPDATE SKIP LOCKED;
            """, new { limit = MigrationBatchSize }, transaction, cancellationToken: cancellationToken))).AsList();
        if (entries.Count == 0)
            return 0;
        var serializer = new QueueJsonSerializer();
        var bodies = new List<string>(entries.Count);
        foreach (var entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bodies.Add(serializer.NormalizeLegacyDeadLetter(entry.Body, sourceQueue));
        }
        var ids = entries.Select(entry => entry.Id).ToArray();
        // Moving and deleting are atomic per batch. Existing legacy tables remain intact and empty;
        // an interrupted startup resumes with only the entries still present in those tables.
        var sql = sameQueue
            ? $"""
                UPDATE {table} SET message = replacements.body
                FROM unnest(@ids::bigint[], @bodies::jsonb[]) AS replacements(id, body)
                WHERE msg_id = replacements.id;
                """
            : $"""
                SELECT pgmq.send_batch(@deadLetterQueue, @bodies::jsonb[]);
                DELETE FROM {table} WHERE msg_id = ANY(@ids);
                """;
        await connection.ExecuteAsync(new CommandDefinition(sql,
            new { deadLetterQueue, bodies = bodies.ToArray(), ids }, transaction, cancellationToken: cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        return entries.Count;
    }

    private sealed record DeadLetterTable(string TableName, string QueueIndexName, string TopicIndexName);

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
