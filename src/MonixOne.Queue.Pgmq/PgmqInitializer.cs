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
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Validate before database access, but leave extension installation and schema ownership to deployment scripts.
        _ = options.Value;
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        var installedVersion = await connection.ExecuteScalarAsync<string?>(new CommandDefinition(
            "SELECT extversion FROM pg_extension WHERE extname = 'pgmq'",
            cancellationToken: cancellationToken));

        var status = installedVersion switch
        {
            null => "missing",
            var version when version == PgmqOptions.SupportedExtensionVersion => "installed",
            _ => "version_mismatch"
        };

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
                expectedVersion = PgmqOptions.SupportedExtensionVersion,
                installedVersion,
                sourceUrl = "https://github.com/pgmq/pgmq.git",
                sourceRevision = "v1.13.0"
            },
            cancellationToken: cancellationToken));

        if (status != "installed")
        {
            throw new InvalidOperationException($"PGMQ extension must be installed at version {PgmqOptions.SupportedExtensionVersion}; detected '{installedVersion ?? "not installed"}'. Apply infrastructure/pgmq/v1.13.0/001-create-metadata.sql and 002-install-pgmq.sql before starting the application.");
        }

        logger.LogInformation("PGMQ {PgmqVersion} is installed and connected.", installedVersion);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
