namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Caches each DAO36-authored Longs shape and returns private mutation copies.</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "xUnit IClassFixture<T> requires public accessibility")]
public sealed class NativeJet3SchemaSeedCache
{
    private readonly ConcurrentDictionary<bool, Lazy<Task<(byte[] Image, string Owner)>>> longs = new();

    internal async Task<(byte[] Image, string Owner)> CopyLongsAsync(bool wide)
    {
        (byte[] image, string owner) = await this.longs.GetOrAdd(wide, static shape => new Lazy<Task<(byte[] Image, string Owner)>>(() => CreateLongsAsync(shape))).Value;
        return ((byte[])image.Clone(), owner);
    }

    private static async Task<(byte[] Image, string Owner)> CreateLongsAsync(bool wide)
    {
        await using var seed = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeJet3SchemaSeed", databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(seed.SourcePath);
        int rows = wide ? 300 : 1;
        string extraColumns = wide ? string.Concat(Enumerable.Range(0, 100).Select(index => $", [C{index}] LONG")) : string.Empty;
        PowerShellRunResult created = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                $db.Execute('CREATE TABLE [Longs] ([Id] LONG, [Spare] LONG{{extraColumns}})', 128)
                for ($id = 1; $id -le {{rows}}; $id++) { $db.Execute(('INSERT INTO [Longs] ([Id],[Spare]) VALUES (' + $id + ',7)'), 128) }
                if ({{(wide ? "$true" : "$false")}}) {
                    for ($i = 0; $i -lt 16; $i++) { $db.Execute(('CREATE INDEX [I' + $i + '] ON [Longs] ([C' + $i + '])'), 128) }
                }
                Write-Output ('OWNER=' + $db.Containers('Tables').Documents('Longs').Owner)
            } finally { $db.Close() }
            """,
            seed.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(created.ExitCode == 0 && !created.TimedOut && created.OutputComplete, $"DAO36 create failed: {created.StandardOutput}\n{created.StandardError}");
        string owner = created.StandardOutput.Trim();
        Assert.StartsWith("OWNER=", owner, StringComparison.Ordinal);
        return (await File.ReadAllBytesAsync(seed.SourcePath, TestContext.Current.CancellationToken), owner);
    }
}
