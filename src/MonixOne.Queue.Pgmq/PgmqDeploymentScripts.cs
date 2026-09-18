using System.Formats.Tar;
using System.IO.Compression;
using System.Reflection;

namespace MonixOne.Queue.Pgmq;

/// <summary>Versioned SQL deployment contract shipped with the NuGet package.</summary>
public static class PgmqDeploymentScripts
{
    public const string Version = "v1.13.0";
    public const string PgmqVersion = "1.13.0";
    public const string RelativeDirectory = "pgmq/v1.13.0";
    public const string SourceArchive = "pgmq-v1.13.0.tar.gz";
    public const string SourceArchiveSha256 = "c980705ffa2a731b69f3d26be5650d6fbcc76b2b9add67b138da4ee74a4579a5";
    public static IReadOnlyList<string> OrderedFiles { get; } =
    [
        "001-create-metadata.sql"
    ];

    private static readonly Lazy<IReadOnlyDictionary<string, string>> _pgmqSqlFiles = new(ReadPgmqSqlFiles);

    internal static string ReadSql(string fileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fileName);

        var resourceName = $"MonixOne.Queue.Pgmq.PgmqScripts.{Version}.{fileName}";
        using var stream = typeof(PgmqDeploymentScripts).Assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded PGMQ deployment script '{fileName}' was not found.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }

    internal static string ReadInitialPgmqSql() => GetPgmqSqlFile("pgmq.sql");

    internal static IReadOnlyList<string> ReadUpgradeSql(string installedVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(installedVersion);

        var currentVersion = installedVersion;
        var applied = new List<string>();
        var visited = new HashSet<string>(StringComparer.Ordinal);

        while (!string.Equals(currentVersion, PgmqVersion, StringComparison.Ordinal))
        {
            if (!visited.Add(currentVersion))
            {
                throw new InvalidOperationException($"PGMQ SQL migration cycle starts at version '{currentVersion}'.");
            }

            var migration = _pgmqSqlFiles.Value
                .Where(pair => TryParseUpgrade(pair.Key, out var fromVersion, out _) && string.Equals(fromVersion, currentVersion, StringComparison.Ordinal))
                .Select(pair => new { Sql = pair.Value, ToVersion = ParseUpgrade(pair.Key).ToVersion })
                .SingleOrDefault();

            if (migration is null)
            {
                throw new InvalidOperationException($"The package has no PGMQ SQL migration from '{currentVersion}' to '{PgmqVersion}'.");
            }

            applied.Add(migration.Sql);
            currentVersion = migration.ToVersion;
        }

        return applied;
    }

    private static string GetPgmqSqlFile(string fileName) =>
        _pgmqSqlFiles.Value.TryGetValue(fileName, out var sql)
            ? sql
            : throw new InvalidOperationException($"PGMQ SQL file '{fileName}' was not found in the embedded source archive.");

    private static IReadOnlyDictionary<string, string> ReadPgmqSqlFiles()
    {
        using var archive = typeof(PgmqDeploymentScripts).Assembly.GetManifestResourceStream(
            "MonixOne.Queue.Pgmq.PgmqArchive.v1.13.0.tar.gz")
            ?? throw new InvalidOperationException("Embedded PGMQ source archive was not found.");
        using var gzip = new GZipStream(archive, CompressionMode.Decompress);
        using var tar = new TarReader(gzip);

        var files = new Dictionary<string, string>(StringComparer.Ordinal);
        const string sqlPrefix = "pgmq-1.13.0/pgmq-extension/sql/";

        TarEntry? entry;
        while ((entry = tar.GetNextEntry()) is not null)
        {
            if (entry.DataStream is null || !entry.Name.StartsWith(sqlPrefix, StringComparison.Ordinal) || !entry.Name.EndsWith(".sql", StringComparison.Ordinal))
            {
                continue;
            }

            using var reader = new StreamReader(entry.DataStream);
            files.Add(entry.Name[sqlPrefix.Length..], reader.ReadToEnd());
        }

        return files;
    }

    private static bool TryParseUpgrade(string fileName, out string fromVersion, out string toVersion)
    {
        fromVersion = string.Empty;
        toVersion = string.Empty;
        const string prefix = "pgmq--";
        const string suffix = ".sql";

        if (!fileName.StartsWith(prefix, StringComparison.Ordinal) || !fileName.EndsWith(suffix, StringComparison.Ordinal))
        {
            return false;
        }

        var versions = fileName[prefix.Length..^suffix.Length].Split("--", StringSplitOptions.None);
        if (versions.Length != 2 || versions.Any(string.IsNullOrWhiteSpace))
        {
            return false;
        }

        fromVersion = versions[0];
        toVersion = versions[1];
        return true;
    }

    private static (string FromVersion, string ToVersion) ParseUpgrade(string fileName)
    {
        _ = TryParseUpgrade(fileName, out var fromVersion, out var toVersion);
        return (fromVersion, toVersion);
    }
}
