using Dapper;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqInitializer(
    NpgsqlDataSource dataSource,
    ILogger<PgmqInitializer> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        PgmqDeploymentScripts.ValidateSourceArchive();
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

            await connection.ExecuteAsync(new CommandDefinition(
                PgmqDeploymentScripts.ReadInitialPgmqSql(),
                transaction: transaction,
                cancellationToken: cancellationToken));
        }
        else
        {
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

        await transaction.CommitAsync(cancellationToken);

        logger.LogInformation("PGMQ {PgmqVersion} is ready.", targetVersion);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
