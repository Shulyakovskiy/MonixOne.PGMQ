using MonixOne.Queue;
using Shouldly;
using Xunit;

namespace MonixOne.Queue.Pgmq.Tests;

public sealed class QueueContractTests
{
    [Fact]
    public void QueueMessageAttribute_DeclaresStableTypeAndVersion()
    {
        var attribute = (QueueMessageAttribute?)Attribute.GetCustomAttribute(typeof(VersionedMessage), typeof(QueueMessageAttribute));

        attribute.ShouldNotBeNull();
        attribute.Type.ShouldBe("notification.requested");
        attribute.Version.ShouldBe(2);
    }

    [Fact]
    public void DeploymentScripts_DeclarePinnedVersionAndExecutionOrder()
    {
        PgmqDeploymentScripts.Version.ShouldBe("v1.13.0");
        PgmqDeploymentScripts.RelativeDirectory.ShouldBe("pgmq/v1.13.0");
        PgmqDeploymentScripts.SourceArchive.ShouldBe("pgmq-v1.13.0.tar.gz");
        PgmqDeploymentScripts.SourceArchiveSha256.ShouldBe(
            "c980705ffa2a731b69f3d26be5650d6fbcc76b2b9add67b138da4ee74a4579a5");
        PgmqDeploymentScripts.PgmqVersion.ShouldBe("1.13.0");
        PgmqDeploymentScripts.OrderedFiles.ShouldBe(["001-create-metadata.sql"]);
    }

    [Fact]
    public void DeploymentScripts_SelectUpstreamUpgradeChainFromAppliedVersion()
    {
        var migrations = PgmqDeploymentScripts.ReadUpgradeSql("1.12.0");

        migrations.Count.ShouldBe(1);
        migrations.Single().ShouldContain("last_read_at");
        PgmqDeploymentScripts.ReadUpgradeSql("1.13.0").ShouldBeEmpty();
    }

    [QueueMessage("notification.requested", Version = 2)]
    private sealed record VersionedMessage(Guid UserId);
}
