namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native acceptance of staged tree bytes, installed into a controlled existing allocation.</summary>
public sealed class CompactRawIndexNativeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task StagedTree_Dao120ReadsKeysEnforcesUniquenessAndCompacts(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CompactRawIndex", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await InstallStagedTreeAsync(session, format);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(NativeScript(session), TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO tree validation failed:\n{result.StdOut}\n{result.StdErr}");
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task StagedTree_Dao36ReadsKeysEnforcesUniquenessAndCompacts()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CompactRawIndex", databaseExtension: ".mdb");
        await InstallStagedTreeAsync(session, DatabaseFormat.Jet3Mdb);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(NativeScript(session), session.WorkDir, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 tree validation failed:\n{result.StandardOutput}\n{result.StandardError}");
    }

    private static async Task InstallStagedTreeAsync(AccessRoundTripSession session, DatabaseFormat format)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, format, new AccessWriterOptions { UseLockFile = false }, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Payload", typeof(string), 32)], Ct);
        await writer.InsertRowsAsync("T", Enumerable.Range(0, 1_200).Select(static id => new object?[] { id, id.ToString(CultureInfo.InvariantCulture) }), Ct);
        CatalogEntry table = (await writer.Services.Catalog.GetCatalogEntryAsync("T", Ct))!;
        TableDef definition = (await writer.Database.TableDefs.ReadTableDefAsync(table.TDefPage, Ct))!;
        await writer.Services.Indexes.MaintainIndexesAsync(table.TDefPage, definition, "T", Ct);
        long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
        long[] pages = (await IndexLeafChain.ReadTreePagesAsync(writer.Database, table.TDefPage, root, Ct)).Order().ToArray();
        Assert.True(pages.Length > 1);
        Assert.Equal(Enumerable.Range(0, pages.Length).Select(index => pages[0] + index), pages);
        List<IndexEntry> entries = await IndexLeafChain.ReadEntriesAsync(writer.Database, table.TDefPage, root, Ct);
        Assert.Equal(1_200, entries.Count);
        await using var staging = new MemoryStream();
        await using var fencesA = new MemoryStream();
        await using var fencesB = new MemoryStream();
        staging.SetLength(pages[0] * writer.Database.Format.PageSize);
        CompactRawIndexBuilderResult result = await CompactRawIndexBuilder.BuildAsync(writer.Database.Format, table.TDefPage, EnumerateAsync(entries, Ct), staging, fencesA, fencesB, new(), Ct);
        Assert.False(result.IsValidated);
        Assert.Equal(root, result.RootPageNumber);
        Assert.Equal(pages.Length, result.PageCount);
        Assert.Equal((pages[^1] + 1) * writer.Database.Format.PageSize, staging.Length);
        byte[] image = staging.ToArray();
        foreach (long page in pages)
        {
            int offset = checked((int)(page * writer.Database.Format.PageSize));
            await writer.Pager.WritePageAsync(page, image.AsMemory(offset, writer.Database.Format.PageSize).ToArray(), Ct);
        }

        // The controlled subject retains the exact existing allocation, root,
        // maps and counts. Production compact publication is a separate protocol.
        List<IndexEntry> rebuilt = await IndexLeafChain.ReadEntriesAsync(writer.Database, table.TDefPage, root, Ct);
        Assert.Equal(entries.Count, rebuilt.Count);
        for (int index = 0; index < entries.Count; index++)
        {
            Assert.Equal(entries[index].Key, rebuilt[index].Key);
            Assert.Equal(entries[index].DataPage, rebuilt[index].DataPage);
            Assert.Equal(entries[index].DataRow, rebuilt[index].DataRow);
        }
    }

    private static async IAsyncEnumerable<IndexEntry> EnumerateAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (IndexEntry entry in entries)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entry;
        }

        await Task.CompletedTask;
    }

    private static string NativeScript(AccessRoundTripSession session)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        return $$"""
            function Assert-Contents([string]$path) {
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('T',1)
                    try {
                        $rs.Index=$db.TableDefs.Item('T').Indexes.Item(0).Name
                        $rs.MoveFirst()
                        $count=0
                        while(!$rs.EOF) {
                            if($rs.Fields.Item('Id').Value -ne $count -or $rs.Fields.Item('Payload').Value -cne $count.ToString([Globalization.CultureInfo]::InvariantCulture)) { throw 'Native indexed content mismatch' }
                            $count++
                            $rs.MoveNext()
                        }
                        if($count -ne 1200) { throw 'Native indexed row count mismatch' }
                    } finally { $rs.Close() }
                    $rejected=$false
                    try { $db.Execute("INSERT INTO T VALUES (0,'duplicate')",128) } catch {
                        if(@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3022) { throw }
                        $rejected=$true
                    }
                    if(!$rejected) { throw 'Native duplicate key was accepted' }
                } finally { $db.Close() }
            }
            Assert-Contents {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Contents {{compacted}}
            """;
    }
}
