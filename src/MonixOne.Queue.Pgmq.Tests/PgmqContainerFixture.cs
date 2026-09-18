using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

public sealed class PgmqContainerFixture : IAsyncLifetime
{
    private const string QueueName = "integration_messages";
    // A fresh PGMQ-enabled PostgreSQL instance keeps tests independent from developer databases and configuration.
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("ghcr.io/pgmq/pg17-pgmq:v1.13.0")
        .WithDatabase("queue_tests")
        .WithUsername("queue_tests")
        .WithPassword("queue_tests")
        .Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;
    internal PgmqClient Client { get; private set; } = null!;
    public string Queue => QueueName;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());

        foreach (var script in PgmqDeploymentScripts.OrderedFiles)
        {
            // Exercise the exact package artifacts in the same order used by the database deployment job.
            var path = Path.Combine(AppContext.BaseDirectory, PgmqDeploymentScripts.RelativeDirectory, script);
            await using var command = DataSource.CreateCommand(await File.ReadAllTextAsync(path));
            await command.ExecuteNonQueryAsync();
        }

        await using (var command = DataSource.CreateCommand("SELECT pgmq.create(@queue)"))
        {
            command.Parameters.AddWithValue("queue", QueueName);
            await command.ExecuteNonQueryAsync();
        }

        Client = new PgmqClient(DataSource);
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null) await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class PgmqIntegrationCollection : ICollectionFixture<PgmqContainerFixture>
{
    public const string Name = "pgmq-integration";
}
