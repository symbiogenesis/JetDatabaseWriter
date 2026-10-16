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
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Jet3 index maps follow live trees while retaining aliased native storage.</summary>
public sealed class Jet3IndexUsageMapTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task IndexMap_AliasedByAnotherTablesFreeMap_DetachesWithoutChangingSharedRow(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("Alias", [new ColumnDefinition("Id", typeof(int))], Ct);
        CatalogEntry table = (await writer.Services.Catalog.GetCatalogEntryAsync("T", Ct))!;
        CatalogEntry alias = (await writer.Services.Catalog.GetCatalogEntryAsync("Alias", Ct))!;
        (UsageMapPointer original, byte[] originalRow, List<long> originalPages) = await ReadIndexMapAsync(writer, table.TDefPage);
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(alias.TDefPage, Ct);
        UsageMap.WritePointer(chain.Bytes, writer.Database.Format.TDef.FreePages, original.RowIndex, original.PageNumber);
        await writer.Services.TDefWriter.WriteChainInPlaceAsync(chain, Ct);

        await writer.InsertRowsAsync("T", Enumerable.Range(0, 4000).Select(static id => new object?[] { id }), Ct);
        Assert.Equal(3990, await writer.DeleteRowsAsync("T", RowCriteria.Where(ColumnPredicate.GreaterThanOrEqual("Id", 10)), Ct));

        (UsageMapPointer current, _, List<long> pages) = await ReadIndexMapAsync(writer, table.TDefPage);
        Assert.NotEqual(original, current);
        long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
        Assert.Equal((await IndexLeafChain.ReadTreePagesAsync(writer.Database, table.TDefPage, root, Ct)).Order(), pages.Order());
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(original.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, original.RowIndex, out RowBound row));
        Assert.Equal(originalRow, map.AsSpan(row.RowStart, row.RowSize).ToArray());
        foreach (long page in originalPages)
        {
            Assert.False(await writer.Services.PageAllocator.IsPageFreeAsync(page, Ct));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DropRelationship_DedicatedIndexMapPage_IsFreedUnlessAnotherTableReferencesIt(bool aliasStorage)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))], Ct);
        await writer.CreateTableAsync("Alias", [new ColumnDefinition("Id", typeof(int))], Ct);
        CatalogEntry child = (await writer.Services.Catalog.GetCatalogEntryAsync("C", Ct))!;
        byte[] definition = (await writer.Database.TableDefs.ReadTDefBytesAsync(child.TDefPage, Ct))!;
        Assert.True(UsageMap.TryReadPointer(definition, writer.Database.Format.TDef.UsedPages, out UsageMapPointer tableMap));
        byte[] map = await writer.Database.Pages.ReadPageCopyAsync(tableMap.PageNumber, Ct);
        int rows = Ru16(map, writer.Database.Format.DataPage.NumRows);
        int firstRow = writer.Database.Format.PageSize;
        for (int row = 0; row < rows; row++)
        {
            Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, row, out RowBound bound));
            firstRow = Math.Min(firstRow, bound.RowStart);
        }

        int fillerStart = writer.Database.Format.DataPage.RowsStart + ((rows + 1) * 2) + 8;
        Array.Clear(map, fillerStart, firstRow - fillerStart);
        Wu16(map, writer.Database.Format.DataPage.RowsStart + (rows * 2), fillerStart);
        Wu16(map, writer.Database.Format.DataPage.NumRows, rows + 1);
        Wu16(map, 2, 8);
        await writer.Pager.WritePageAsync(tableMap.PageNumber, map, Ct);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("Rel", "P", "Id", "C", "ParentId"), Ct);
        definition = (await writer.Database.TableDefs.ReadTDefBytesAsync(child.TDefPage, Ct))!;
        TDefCounts counts = TDefCodec.ReadCounts(writer.Database.Format, definition);
        int descriptor = IndexCatalogReader.LocateRealIdxDescStart(writer.Database.Format, definition, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(writer.Database.Format.Index.TryReadRealIdxSlot(definition, descriptor, 1, out RealIdxSlot slot));
        Assert.True(UsageMap.TryReadPointer(definition, slot.FirstDpOffset - 4, out UsageMapPointer indexMap));
        Assert.NotEqual(tableMap.PageNumber, indexMap.PageNumber);
        byte[] originalMap = await writer.Database.Pages.ReadPageCopyAsync(indexMap.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(originalMap, writer.Database.Format.DataPage, writer.Database.Format.PageSize, indexMap.RowIndex, out RowBound indexMapRow));
        var members = new List<long>();
        Assert.True(UsageMap.TryEnumerateInlinePages(originalMap, indexMapRow, writer.Database.Format.PageSize, writer.Database.Pages.PageCount, 1, true, members));
        byte[] bitmap = new byte[writer.Database.Format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[1] = 1;
        foreach (long member in members)
        {
            Assert.True(UsageMap.TrySetReferencePageState(bitmap, writer.Database.Format.PageSize, member, isMarked: true));
        }

        long bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
        Array.Clear(originalMap, indexMapRow.RowStart, indexMapRow.RowSize);
        originalMap[indexMapRow.RowStart] = Constants.UsageMap.ReferenceMapType;
        Wi32(originalMap, indexMapRow.RowStart + Constants.UsageMap.ReferenceMapPointerOffset, checked((int)bitmapPage));
        await writer.Pager.WritePageAsync(indexMap.PageNumber, originalMap, Ct);
        if (aliasStorage)
        {
            CatalogEntry alias = (await writer.Services.Catalog.GetCatalogEntryAsync("Alias", Ct))!;
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(alias.TDefPage, Ct);
            UsageMap.WritePointer(chain.Bytes, writer.Database.Format.TDef.FreePages, indexMap.RowIndex, indexMap.PageNumber);
            await writer.Services.TDefWriter.WriteChainInPlaceAsync(chain, Ct);
        }

        await writer.DropRelationshipAsync("Rel", Ct);
        Assert.Equal(!aliasStorage, await writer.Services.PageAllocator.IsPageFreeAsync(indexMap.PageNumber, Ct));
        Assert.Equal(!aliasStorage, await writer.Services.PageAllocator.IsPageFreeAsync(bitmapPage, Ct));
        if (aliasStorage)
        {
            Assert.Equal(originalMap, await writer.Database.Pages.ReadPageCopyAsync(indexMap.PageNumber, Ct));
            Assert.Equal(bitmap, await writer.Database.Pages.ReadPageCopyAsync(bitmapPage, Ct));
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_ReferenceIndexMap_GrowthMergeAndCompactKeepKeysAndConstraints()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3IndexMaps", databaseExtension: ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.InsertRowsAsync("T", Enumerable.Range(0, 4000).Select(static id => new object?[] { id }), Ct);
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            var childColumns = new List<ColumnDefinition> { new("Id", typeof(int)), new("ParentId", typeof(int)) };
            var childIndexes = new List<IndexDefinition> { new("PK", "Id") { IsPrimaryKey = true } };
            for (int index = 0; index < 12; index++)
            {
                string name = "K" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);
                childColumns.Add(new ColumnDefinition(name, typeof(int)));
                childIndexes.Add(new IndexDefinition("IX_" + name, name));
            }

            await writer.CreateTableAsync("C", childColumns, childIndexes, Ct);
            await writer.InsertRowAsync("P", [1], Ct);
            await writer.InsertRowAsync("C", new RowValues { ["Id"] = 1, ["ParentId"] = 1 }, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("Rel", "P", "Id", "C", "ParentId"), Ct);
        }

        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(session.SourcePath, cancellationToken: Ct))
        {
            CatalogEntry table = (await writer.Services.Catalog.GetCatalogEntryAsync("T", Ct))!;
            long root = Assert.Single(await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, table.TDefPage, Ct));
            int treeSize = (await IndexLeafChain.ReadTreePagesAsync(writer.Database, table.TDefPage, root, Ct)).Count;
            int inlineWindow = (writer.Database.Format.UsageMapRowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
            long target = inlineWindow - (treeSize / 2);
            Assert.True(target > writer.Database.Pages.PageCount);
            _ = await writer.Services.PageAllocator.ReserveContiguousPagesAsync(checked((int)(target - writer.Database.Pages.PageCount)), Ct);
            TableDef definition = (await writer.Database.TableDefs.ReadTableDefAsync(table.TDefPage, Ct))!;
            await writer.Services.Indexes.MaintainIndexesAsync(table.TDefPage, definition, "T", Ct);
            (_, byte[] mapRow, _) = await ReadIndexMapAsync(writer, table.TDefPage);
            Assert.Equal(Constants.UsageMap.ReferenceMapType, mapRow[0]);
        }

        RunNative(session, ExpectedRowsScript(session.SourcePath, 4000));
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            Assert.Equal(3990, await writer.DeleteRowsAsync("T", RowCriteria.Where(ColumnPredicate.GreaterThanOrEqual("Id", 10)), Ct));
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        RunNative(session, ExpectedRowsScript(session.SourcePath, 10) + $"\n$engine.CompactDatabase({source},{compacted})\n" + ExpectedRowsScript(session.CompactedPath, 10));
    }

    private static string ExpectedRowsScript(string path, int expectedRows)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(path);
        return $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('T',1)
                try {
                    $rs.Index=$db.TableDefs.Item('T').Indexes.Item(0).Name
                    $rs.MoveFirst()
                    $count=0
                    while(!$rs.EOF) {
                        if($rs.Fields.Item('Id').Value -ne $count) { throw 'DAO index key order mismatch' }
                        $count++
                        $rs.MoveNext()
                    }
                    if($count -ne {{expectedRows}}) { throw "DAO expected {{expectedRows}} rows, found $count" }
                } finally { $rs.Close() }
                $rejected=$false
                try { $db.Execute('INSERT INTO T VALUES (0)',128) } catch {
                    if(@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3022) { throw }
                    $rejected=$true
                }
                if(!$rejected) { throw 'DAO accepted a duplicate primary key' }
                if($db.Relations.Item('Rel').ForeignTable -ne 'C') { throw 'DAO lost the relationship with new index map storage' }
                $rejected=$false
                try { $db.Execute('INSERT INTO C (Id,ParentId) VALUES (2,99)',128) } catch {
                    if(@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3201) { throw }
                    $rejected=$true
                }
                if(!$rejected) { throw 'DAO accepted an orphan using the new index map page' }
            } finally { $db.Close() }
            """;
    }

    private static void RunNative(AccessRoundTripSession session, string script)
    {
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 index-map verification failed:\n{result.StandardOutput}\n{result.StandardError}");
    }

    private static async Task<(UsageMapPointer Pointer, byte[] RowBytes, List<long> Pages)> ReadIndexMapAsync(AccessWriter writer, long tdefPage)
    {
        DatabaseFile db = writer.Database;
        byte[] definition = (await db.TableDefs.ReadTDefBytesAsync(tdefPage, Ct))!;
        TDefCounts counts = TDefCodec.ReadCounts(db.Format, definition);
        int descriptor = IndexCatalogReader.LocateRealIdxDescStart(db.Format, definition, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(db.Format.Index.TryReadRealIdxSlot(definition, descriptor, 0, out RealIdxSlot slot));
        Assert.True(UsageMap.TryReadPointer(definition, slot.FirstDpOffset - 4, out UsageMapPointer pointer));
        byte[] map = await db.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, db.Format.DataPage, db.Format.PageSize, pointer.RowIndex, out RowBound row));
        var pages = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, row, db.Format.PageSize, db.Pages.PageCount, 1, true, db.Pages.ReadPageAsync, PageBuffers.Return, pages, Ct));
        return (pointer, map.AsSpan(row.RowStart, row.RowSize).ToArray(), pages);
    }
}
