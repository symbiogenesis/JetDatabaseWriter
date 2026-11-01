namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Duplicate native keys retain ascending row bookmarks across leaf boundaries.</summary>
public sealed class DuplicateBookmarkOrderingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Bulk and incremental duplicate runs use native bookmarks, including changed leaf maxima.</summary>
    /// <param name="format">The storage format.</param>
    /// <param name="incremental">Whether the index exists while rows are inserted.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task DuplicateKeys_SortByPageThenRowAcrossBulkAndIncrementalTrees(DatabaseFormat format, bool incremental)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Stamp", typeof(DateTime))], Ct);
        if (incremental)
        {
            await writer.CreateIndexAsync("T", new IndexDefinition("IX_Stamp", "Stamp"), Ct);
        }

        for (int first = 0; first < 2560; first += 128)
        {
            await writer.InsertRowsAsync("T", Enumerable.Range(first, 128).Select(static id => new object[] { id, new DateTime(2020, 1, 1) }), Ct);
        }

        if (!incremental)
        {
            await writer.CreateIndexAsync("T", new IndexDefinition("IX_Stamp", "Stamp"), Ct);
        }

        CatalogEntry table = await writer.Services.Catalog.GetRequiredCatalogEntryAsync("T", Ct);
        long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
        List<IndexEntry> entries = await IndexLeafChain.ReadEntriesAsync(writer.Database, table.TDefPage, root, Ct);
        Assert.Equal(2560, entries.Count);
        Assert.True((await IndexLeafChain.ReadTreePagesAsync(writer.Database, table.TDefPage, root, Ct)).Count > 1);
        Assert.Equal(entries.OrderBy(static entry => entry.DataPage).ThenBy(static entry => entry.DataRow), entries);
        _ = await AssertMaximaAsync(writer, root, []);
        byte[] rootPage = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        IndexEntry boundary = IndexPageCodec.DecodeIntermediateEntries(writer.Database.Format.IndexPage, rootPage, writer.Database.Format.PageSize)[0].Entry;
        int boundaryId = entries.FindIndex(entry => entry.DataPage == boundary.DataPage && entry.DataRow == boundary.DataRow);
        Assert.InRange(boundaryId, 1, 2558);
        Assert.Equal(1, await writer.DeleteRowsAsync("T", RowCriteria.Where("Id", boundaryId), Ct));
        _ = await AssertMaximaAsync(writer, root, []);
    }

    /// <summary>DAO3.6 independently modifies and compacts managed duplicate trees.</summary>
    /// <param name="format">The native-supported format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Dao36_DuplicateTreesPermitNativeInsertUpdateDeleteAndCompact(DatabaseFormat format) => NativeAsync(format, useDao36: true);

    /// <summary>DAO120 independently modifies and compacts Jet4/ACE duplicate trees.</summary>
    /// <param name="format">The native-supported format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Dao120_DuplicateTreesPermitNativeInsertUpdateDeleteAndCompact(DatabaseFormat format) => NativeAsync(format, useDao36: false);

    private static async Task NativeAsync(DatabaseFormat format, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        RunNative(session, useDao36, $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                $db.Execute('CREATE TABLE T (Id LONG CONSTRAINT PK_T PRIMARY KEY,Stamp DATETIME,Payload LONG)',128)
                $db.Execute('CREATE INDEX IX_Stamp ON T (Stamp)',128)
                for($i=1;$i -le 1536;$i++) {$db.Execute(('INSERT INTO T VALUES ('+$i+',#2020-01-01#,1)'),128)}
                $db.Execute('INSERT INTO T (Id,Payload) VALUES (1000001,2)',128)
                $db.Execute('UPDATE T SET Payload=3 WHERE Id=1000001',128)
                $db.Execute('DELETE FROM T WHERE Id=1000001',128)
            } finally {$db.Close()}
            """);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            for (int first = 1537; first <= 2560; first += 128)
            {
                // Descending primary keys also exercise unordered index maintenance.
                await writer.InsertRowsAsync("T", Enumerable.Range(first, 128).Reverse().Select(static id => new object[] { id, new DateTime(2020, 1, 1), 1 }), Ct);
            }

            Assert.Equal(1, await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 99_000 }, Ct));
            Assert.Equal(1, await writer.DeleteRowsAsync("T", RowCriteria.Where("Id", 3), Ct));
        }

        RunNative(session, useDao36, $$"""
            foreach($path in @({{source}},{{compacted}})) {
                if($path -eq {{compacted}}) {$engine.CompactDatabase({{source}},{{compacted}})}
                $db=$engine.OpenDatabase($path)
                try {
                    $db.Execute('INSERT INTO T (Id,Payload) VALUES (1000001,2)',128)
                    $db.Execute('UPDATE T SET Payload=3 WHERE Id=1000001',128)
                    $db.Execute('DELETE FROM T WHERE Id=1000001',128)
                    $db.Execute('DELETE FROM T WHERE Id=99000',128)
                    $db.Execute('DELETE FROM T WHERE Id=2560',128)
                    $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM T',4)
                    try {
                        if([int]$rs.Fields('N').Value -ne 2557) {throw 'Wrong duplicate-tree membership'}
                    } finally {$rs.Close()}
                } finally {$db.Close()}
            }
            """);
    }

    private static async Task<IndexEntry> AssertMaximaAsync(AccessWriter writer, long pageNumber, HashSet<long> visited)
    {
        Assert.True(visited.Add(pageNumber) && visited.Count < 10_000);
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);
        JetFormat format = writer.Database.Format;
        if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
        {
            return IndexPageCodec.DecodeLeafEntries(format.IndexPage, page, format.PageSize)[^1];
        }

        Assert.Equal(Constants.IndexLeafPage.PageTypeIntermediate, page[0]);
        List<DecodedIntermediateEntry> children = IndexPageCodec.DecodeIntermediateEntries(format.IndexPage, page, format.PageSize);
        Assert.NotEmpty(children);
        foreach (DecodedIntermediateEntry child in children)
        {
            IndexEntry maximum = await AssertMaximaAsync(writer, child.ChildPage, visited);
            Assert.Equal(maximum.Key, child.Entry.Key);
            Assert.Equal(maximum.DataPage, child.Entry.DataPage);
            Assert.Equal(maximum.DataRow, child.Entry.DataRow);
        }

        return children[^1].Entry;
    }

    private static void RunNative(AccessRoundTripSession session, bool useDao36, string script)
    {
        if (!useDao36)
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
            return;
        }

        PowerShellRunResult native = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
        Assert.True(native.ExitCode == 0 && !native.TimedOut && native.OutputComplete, native.StandardOutput + native.StandardError);
    }
}
