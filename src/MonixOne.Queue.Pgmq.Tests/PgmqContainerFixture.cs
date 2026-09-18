using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

public sealed class PgmqContainerFixture : IAsyncLifetime
{
    private const string QueueName = "integration_messages";
    // A fresh PGMQ-enabled PostgreSQL instance keeps tests independent from developer databases and configuration.
    private PostgreSqlContainer? _container;

    public NpgsqlDataSource DataSource { get; private set; } = null!;
    internal PgmqClient Client { get; private set; } = null!;
    public string Queue => QueueName;

    public async ValueTask InitializeAsync()
    {
        _container = new PostgreSqlBuilder("ghcr.io/pgmq/pg17-pgmq:v1.13.0")
            .WithDatabase("queue_tests")
            .WithUsername("queue_tests")
            .WithPassword("queue_tests")
            .Build();
        await _container.StartAsync();

        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());

        var initializer = new PgmqInitializer(
            DataSource,
            Options.Create(new PgmqOptions
            {
                Consumers = new Dictionary<string, PgmqConsumerOptions>
                {
                    ["Integration"] = new() { Queue = QueueName }
                }
            }),
            NullLogger<PgmqInitializer>.Instance);
        await initializer.StartAsync(TestContext.Current.CancellationToken);

        Client = new PgmqClient(DataSource);
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null) await DataSource.DisposeAsync();
        if (_container is not null) await _container.DisposeAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class PgmqIntegrationCollection : ICollectionFixture<PgmqContainerFixture>
{
    public const string Name = "pgmq-integration";
}
