using Dapper;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace MonixOne.Queue.Pgmq;

internal sealed class PgmqHealthCheck(NpgsqlDataSource dataSource) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
            var version = await connection.ExecuteScalarAsync<string?>(new CommandDefinition(
                "SELECT extversion FROM pg_extension WHERE extname = 'pgmq'",
                cancellationToken: cancellationToken));
            return version == PgmqOptions.SupportedExtensionVersion
                ? HealthCheckResult.Healthy($"PGMQ {version} is available.")
                : HealthCheckResult.Unhealthy($"PGMQ {PgmqOptions.SupportedExtensionVersion} is required; detected '{version ?? "not installed"}'.");
        }
        catch (Exception exception)
        {
            return HealthCheckResult.Unhealthy("PostgreSQL or PGMQ is unavailable.", exception);
        }
    }
}
