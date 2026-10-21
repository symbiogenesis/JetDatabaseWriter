namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Column-owned LVAL maps remain complete across allocation, deletion and reuse.</summary>
public sealed class LongValueUsageMapTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertDeleteReuse_MaintainsEachColumnsNativeMapPointers(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Memo", typeof(string), maxLength: int.MaxValue), new("Blob", typeof(byte[]))], ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
        DatabaseFile db = writer.Database;
        List<MapPair> initial = await ReadMapsAsync(db, entry.TDefPage, ct);
        Assert.Equal(2, initial.Count);
        Assert.Equal(1, initial[0].Column);
        Assert.Equal(2, initial[1].Column);
        Assert.All(initial, map => Assert.Empty(map.Owned));
        Assert.All(initial, map => Assert.Empty(map.Free));
        await writer.InsertRowAsync("T", [1, new string('m', 500), Enumerable.Repeat((byte)0xA3, 8_200).ToArray()], ct);
        List<MapPair> inserted = await ReadMapsAsync(db, entry.TDefPage, ct);
        Assert.Single(inserted[0].Owned);
        Assert.True(inserted[1].Owned.Count > 1);
        Assert.Empty(inserted[0].Owned.Intersect(inserted[1].Owned));
        Assert.All(inserted, map => Assert.Subset(map.Owned, map.Free));
        foreach (MapPair map in inserted)
        {
            Assert.Equal(initial.Single(before => before.Column == map.Column).Pointer, map.Pointer);
            foreach (long pageNumber in map.Owned)
            {
                byte[] page = await db.Pages.ReadPageCopyAsync(pageNumber, ct);
                Assert.Equal("LVAL"u8.ToArray(), page[4..8]);
            }
        }

        Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, ct));
        List<MapPair> deleted = await ReadMapsAsync(db, entry.TDefPage, ct);
        Assert.All(deleted, map => Assert.Empty(map.Owned));
        Assert.All(deleted, map => Assert.Empty(map.Free));
        await writer.InsertRowAsync("T", [2, new string('z', 500), Enumerable.Repeat((byte)0xB4, 8_200).ToArray()], ct);
        List<MapPair> reused = await ReadMapsAsync(db, entry.TDefPage, ct);
        Assert.All(reused, map => Assert.NotEmpty(map.Owned));
        Assert.Equal(inserted.SelectMany(map => map.Owned).Order(), reused.SelectMany(map => map.Owned).Order());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertAcrossInlineWindow_PromotesColumnMapsWithoutLosingPages(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Blob", typeof(byte[]))], ct);
        await writer.InsertRowAsync("T", [1, new byte[writer.Database.Format.LvalPage.SinglePagePayloadCapacity(writer.Database.Format.PageSize)]], ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
        List<MapPair> before = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        byte[] padding = new byte[writer.Database.Format.PageSize];
        padding[0] = Constants.PageTypes.TableDefinition;
        int boundary = writer.Database.Format.IsJet3 ? 1_024 : 512;
        while (writer.Pager.PageCount <= boundary)
        {
            _ = await writer.Services.PageAllocator.AllocatePageAsync(padding, ct);
        }

        await writer.InsertRowAsync("T", [2, new byte[writer.Database.Format.LvalPage.SinglePagePayloadCapacity(writer.Database.Format.PageSize)]], ct);
        List<MapPair> after = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        MapPair map = Assert.Single(after);
        Assert.Equal(before[0].Pointer, map.Pointer);
        Assert.Equal(2, map.Owned.Count);
        Assert.Subset(map.Owned, map.Free);
        Assert.Subset(map.Owned, before[0].Owned);
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(map.Pointer.PageNumber, ct);
        Assert.True(UsageMap.TryGetRowBound(page, writer.Database.Format.DataPage, writer.Database.Format.PageSize, map.Pointer.RowIndex, out RowBound row));
        Assert.Equal(Constants.UsageMap.ReferenceMapType, page[row.RowStart]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SchemaRewrite_MaintainsSurvivingLongColumnMaps(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Memo", typeof(string), maxLength: int.MaxValue), new("Blob", typeof(byte[]))], ct);
        byte[] value = Enumerable.Repeat((byte)0xA5, 8_200).ToArray();
        await writer.InsertRowAsync("T", [1, new string('r', 500), value], ct);
        await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), ct);
        await writer.RenameColumnAsync("T", "Blob", "RenamedBlob", ct);
        await writer.DropColumnAsync("T", "Memo", ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
        List<MapPair> maps = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        MapPair surviving = Assert.Single(maps);
        Assert.NotEmpty(surviving.Owned);
        Assert.Subset(surviving.Owned, surviving.Free);
        Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, ct));
        maps = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        Assert.Empty(Assert.Single(maps).Owned);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public async Task DaoJet3_NativeColumnMapsSurviveWriterDeleteReuseAndCompact()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        PowerShellRunResult created = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                $db.Execute('CREATE TABLE T ([Id] LONG, [Memo] MEMO, [Blob] OLEOBJECT)')
                $rs = $db.OpenRecordset('T')
                try {
                    $rs.AddNew()
                    $rs.Fields('Id').Value = 0
                    $rs.Fields('Memo').Value = ('n' * 12000)
                    $rs.Fields('Blob').Value = (New-Object byte[] 9000)
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0 && !created.TimedOut, created.StandardError);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            CatalogEntry nativeEntry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
            List<MapPair> nativeMaps = await ReadMapsAsync(writer.Database, nativeEntry.TDefPage, ct);
            Assert.Equal(2, nativeMaps.Count);
            Assert.All(nativeMaps, map => Assert.True(map.Owned.Count > 1));
            Assert.All(nativeMaps, map => Assert.Empty(map.Free));
            Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 0, ct));
            await writer.InsertRowAsync("T", [1, new string('a', 500), new byte[8_200]], ct);
            await writer.InsertRowAsync("T", [2, new string('b', 500), new byte[8_200]], ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, ct));
            await writer.InsertRowAsync("T", [3, new string('c', 500), new byte[8_200]], ct);
            await writer.CreateTableAsync("Generated", [new("Id", typeof(int)), new("Blob", typeof(byte[]))], ct);
            await writer.InsertRowAsync("Generated", [1, new byte[500]], ct);
        }

        PowerShellRunResult native = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                $rs = $db.OpenRecordset('SELECT * FROM T ORDER BY Id', 4)
                try {
                    while (!$rs.EOF) {
                        if ($rs.Fields('Memo').Value.Length -ne 500) { throw 'Memo value was truncated.' }
                        if ($rs.Fields('Blob').Value.Length -ne 8200) { throw 'OLE value was truncated.' }
                        Write-Output "ID=$($rs.Fields('Id').Value)"
                        $rs.MoveNext()
                    }
                } finally { $rs.Close() }
                $check = $db.OpenRecordset('SELECT * FROM Generated', 4)
                try {
                    if ($check.EOF -or $check.Fields('Blob').Value.Length -ne 500) { throw 'The generated column maps lost their value.' }
                } finally { $check.Close() }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(native.ExitCode == 0 && !native.TimedOut, native.StandardError);
        Assert.Contains("ID=2", native.StandardOutput, StringComparison.Ordinal);
        Assert.Contains("ID=3", native.StandardOutput, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, ct);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: ct))
        {
            Assert.Equal(500, Assert.IsType<string>(row[1]).Length);
            Assert.Equal(8_200, Assert.IsType<byte[]>(row[2]).Length);
            count++;
        }

        Assert.Equal(2, count);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Dao120_NativeColumnMapsSurviveWriterDeleteReuseAndCompact(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int version = format == DatabaseFormat.Jet4Mdb ? 64 : 128;
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        AccessRoundTripEnvironment.CompactResult created = AccessRoundTripEnvironment.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE T ([Id] LONG, [Memo] MEMO, [Blob] OLEOBJECT)')
                $rs = $db.OpenRecordset('T')
                try {
                    $rs.AddNew()
                    $rs.Fields('Id').Value = 0
                    $rs.Fields('Memo').Value = ('n' * 12000)
                    $rs.Fields('Blob').Value = (New-Object byte[] 9000)
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdErr);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            CatalogEntry nativeEntry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
            List<MapPair> nativeMaps = await ReadMapsAsync(writer.Database, nativeEntry.TDefPage, ct);
            Assert.Equal(2, nativeMaps.Count);
            Assert.All(nativeMaps, map => Assert.True(map.Owned.Count > 1));
            Assert.All(nativeMaps, map => Assert.Empty(map.Free));
            Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 0, ct));
            await writer.InsertRowAsync("T", [1, new string('a', 500), new byte[8_200]], ct);
            await writer.InsertRowAsync("T", [2, new string('b', 500), new byte[8_200]], ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, ct));
            await writer.InsertRowAsync("T", [3, new string('c', 500), new byte[8_200]], ct);
            await writer.CreateTableAsync("Generated", [new("Id", typeof(int)), new("Blob", typeof(byte[]))], ct);
            await writer.InsertRowAsync("Generated", [1, new byte[500]], ct);
        }

        AccessRoundTripEnvironment.CompactResult native = AccessRoundTripEnvironment.RunDaoEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                $rs = $db.OpenRecordset('SELECT * FROM T ORDER BY Id', 4)
                try {
                    while (!$rs.EOF) {
                        if ($rs.Fields('Memo').Value.Length -ne 500) { throw 'Memo value was truncated.' }
                        if ($rs.Fields('Blob').Value.Length -ne 8200) { throw 'OLE value was truncated.' }
                        Write-Output "ID=$($rs.Fields('Id').Value)"
                        $rs.MoveNext()
                    }
                } finally { $rs.Close() }
                $check = $db.OpenRecordset('SELECT * FROM Generated', 4)
                try {
                    if ($check.EOF -or $check.Fields('Blob').Value.Length -ne 500) { throw 'The generated column maps lost their value.' }
                } finally { $check.Close() }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(native.ExitCode == 0, native.StdErr);
        Assert.Contains("ID=2", native.StdOut, StringComparison.Ordinal);
        Assert.Contains("ID=3", native.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, ct);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: ct))
        {
            Assert.Equal(500, Assert.IsType<string>(row[1]).Length);
            Assert.Equal(8_200, Assert.IsType<byte[]>(row[2]).Length);
            count++;
        }

        Assert.Equal(2, count);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SmallExternalValues_ReuseColumnFreePagesAndPreserveOtherRows(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Blob", typeof(byte[]))], ct);
        await writer.InsertRowAsync("T", [1, Enumerable.Repeat((byte)0xA1, 500).ToArray()], ct);
        await writer.InsertRowAsync("T", [2, Enumerable.Repeat((byte)0xB2, 500).ToArray()], ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
        List<MapPair> maps = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        long pageNumber = Assert.Single(Assert.Single(maps).Owned);
        Assert.Equal(1, await writer.DeleteRowsAsync("T", "Id", 1, ct));
        maps = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        Assert.Equal(pageNumber, Assert.Single(Assert.Single(maps).Owned));
        await writer.InsertRowAsync("T", [3, Enumerable.Repeat((byte)0xC3, 500).ToArray()], ct);
        maps = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        Assert.Equal(pageNumber, Assert.Single(Assert.Single(maps).Owned));
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pageNumber, ct);
        Assert.Equal(3, Ru16(page, writer.Database.Format.DataPage.NumRows));
        foreach (LocatedRow located in await writer.Services.Snapshots.ReadRowsAsync(entry.TDefPage, ct))
        {
            object[] row = located.Values;
            byte expected = Assert.IsType<int>(row[0]) == 2 ? (byte)0xB2 : (byte)0xC3;
            Assert.Equal(Enumerable.Repeat(expected, 500), Assert.IsType<byte[]>(row[1]));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task AliasedColumnFreeMap_IsDetachedBeforeAddingPages(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Memo", typeof(string), maxLength: int.MaxValue), new("Blob", typeof(byte[]))], ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(entry.TDefPage, ct);
        Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, chain.Bytes, out List<LongValueUsageMap> maps, out _));
        LongValueUsageMap first = maps.Single(map => map.ColumnNumber == 1);
        LongValueUsageMap second = maps.Single(map => map.ColumnNumber == 2);
        UsageMap.WritePointer(chain.Bytes, first.FreeOffset, second.Free.RowIndex, second.Free.PageNumber);
        await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, ct);
        byte[] original = await writer.Database.Pages.ReadPageCopyAsync(second.Free.PageNumber, ct);
        Assert.True(UsageMap.TryGetRowBound(original, writer.Database.Format.DataPage, writer.Database.Format.PageSize, second.Free.RowIndex, out RowBound bound));
        byte[] originalRow = original.AsSpan(bound.RowStart, bound.RowSize).ToArray();
        await writer.InsertRowAsync("T", [1, new string('a', 500), DBNull.Value], ct);
        byte[] updated = Assert.IsType<byte[]>(await writer.Database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, ct));
        Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, updated, out List<LongValueUsageMap> after, out _));
        Assert.NotEqual(second.Free, after.Single(map => map.ColumnNumber == 1).Free);
        Assert.Equal(second.Free, after.Single(map => map.ColumnNumber == 2).Free);
        byte[] preserved = await writer.Database.Pages.ReadPageCopyAsync(second.Free.PageNumber, ct);
        Assert.Equal(originalRow, preserved.AsSpan(bound.RowStart, bound.RowSize).ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CancelAndRollback_RestoreColumnMapsAndNewBitmapReservations(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Blob", typeof(byte[]))], ct);
        await writer.InsertRowAsync("T", [1, new byte[500]], ct);
        byte[] before = stream.ToArray();
        await using JetTransaction transaction = await writer.BeginTransactionAsync(ct);
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writer.InsertRowAsync("T", [2, new byte[2_100_000]], canceled.Token).AsTask());
        await writer.InsertRowAsync("T", [3, new byte[2_100_000]], ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("T", ct));
        List<MapPair> maps = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        Assert.True(Assert.Single(maps).Owned.Count > 500);
        await transaction.RollbackAsync(ct);
        Assert.Equal(before, stream.ToArray());
        maps = await ReadMapsAsync(writer.Database, entry.TDefPage, ct);
        Assert.Single(Assert.Single(maps).Owned);
    }

    private static async Task<List<MapPair>> ReadMapsAsync(DatabaseFile db, long tdefPage, CancellationToken ct)
    {
        byte[] tdef = Assert.IsType<byte[]>(await db.TableDefs.ReadTDefBytesAsync(tdefPage, ct));
        int columns = Ru16(tdef, db.Format.TDef.NumCols);
        int indexes = Ri32(tdef, db.Format.TDef.NumIdx);
        int reals = Ri32(tdef, db.Format.TDef.NumRealIdx);
        int start = TDefCodec.LocateRealIndexDescriptors(db.Format, tdef, columns, reals);
        start = db.Format.Index.GetIndexSection(start, reals, indexes).LogIdxNamesStart;
        for (int i = 0; i < indexes; i++)
        {
            Assert.True(db.Format.ReadColumnName(tdef, ref start, out _) >= 0);
        }

        var maps = new List<MapPair>();
        int end = Math.Min(tdef.Length, checked(Ri32(tdef, 8) + 8));
        for (int offset = start; offset + 10 <= end && Ru16(tdef, offset) != ushort.MaxValue; offset += 10)
        {
            var owned = new UsageMapPointer(tdef[offset + 2], UsageMap.ReadUInt24(tdef, offset + 3));
            var free = new UsageMapPointer(tdef[offset + 6], UsageMap.ReadUInt24(tdef, offset + 7));
            maps.Add(new MapPair(Ru16(tdef, offset), owned, await ReadPagesAsync(db, owned, ct), await ReadPagesAsync(db, free, ct)));
        }

        return maps.OrderBy(map => map.Column).ToList();
    }

    private static async Task<HashSet<long>> ReadPagesAsync(DatabaseFile db, UsageMapPointer pointer, CancellationToken ct)
    {
        Assert.True(pointer.PageNumber > 0);
        byte[] page = await db.Pages.ReadPageCopyAsync(pointer.PageNumber, ct);
        Assert.True(UsageMap.TryGetRowBound(page, db.Format.DataPage, db.Format.PageSize, pointer.RowIndex, out RowBound row));
        var pages = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(page, row, db.Format.PageSize, db.Pages.PageCount, 1, true, db.Pages.ReadPageAsync, PageBuffers.Return, pages, ct));
        return pages.ToHashSet();
    }

    private sealed record MapPair(int Column, UsageMapPointer Pointer, HashSet<long> Owned, HashSet<long> Free);
}
