using MonixOne.Queue;
using Shouldly;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

public sealed class QueueContractTests
{
    [Fact]
    public void DeploymentScripts_SelectUpstreamUpgradeChainFromAppliedVersion()
    {
        var migrations = PgmqDeploymentScripts.ReadUpgradeSql("1.12.0");

        migrations.Count.ShouldBe(1);
        migrations.Single().ShouldContain("last_read_at");
        PgmqDeploymentScripts.ReadUpgradeSql("1.13.0").ShouldBeEmpty();
    }

    [Fact]
    public void QueueSerialization_RejectsMissingIdempotencyKey()
    {
        var serializer = new QueueJsonSerializer();

        Should.Throw<ArgumentException>(() =>
            serializer.Serialize(
                new VersionedMessage(Guid.NewGuid()),
                new QueueSendOptions { IdempotencyKey = " " }
            )
        );
    }

    [Fact]
    public void QueueDeserialization_RejectsKeyThatCannotBePersisted()
    {
        var body = System.Text.Json.JsonSerializer.Serialize(
            new { idempotencyKey = new string('x', 513) }
        );

        Should.Throw<QueueException>(() =>
            new QueueJsonSerializer().Deserialize<VersionedMessage>(body)
        );
    }

    [QueueMessage("notification.requested", Version = 2)]
    private sealed record VersionedMessage(Guid UserId);
}
