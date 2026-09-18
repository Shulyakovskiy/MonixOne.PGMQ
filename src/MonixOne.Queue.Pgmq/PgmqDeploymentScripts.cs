namespace MonixOne.Queue.Pgmq;

/// <summary>Versioned SQL deployment contract shipped with the NuGet package.</summary>
public static class PgmqDeploymentScripts
{
    public const string Version = "v1.13.0";
    public const string RelativeDirectory = "pgmq/v1.13.0";
    public const string SourceArchive = "pgmq-v1.13.0.tar.gz";
    public const string SourceArchiveSha256 = "c980705ffa2a731b69f3d26be5650d6fbcc76b2b9add67b138da4ee74a4579a5";
    public static IReadOnlyList<string> OrderedFiles { get; } =
    [
        "001-create-metadata.sql",
        "002-install-pgmq.sql"
    ];
}
