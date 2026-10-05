namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Pins the owned-page and row-directory seams split out of
/// <see cref="DatabaseFile"/>: <see cref="OwnedDataPages"/> finds the same
/// pages with and without its caches, reads a rejected usage map once when it
/// caches, and refuses to cache over the writer's
/// file, the two row-directory parsers of <see cref="DataPageRows"/> agree on
/// every data page of the Access-authored fixtures, and the writer's owned
/// pages include the pages a transaction appended.
/// </summary>
public sealed class OwnedDataPagesTests
{
    private const string TableName = "Items";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<string> Fixtures => ["Jet3Test", "AdventureLT2008", "NorthwindTraders"];

    [Fact]
    public async Task CachingOverPager_IsRejected()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using WriterHarness writer = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);

        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => new OwnedDataPages(writer.Database.Pages, writer.Database.Format, cacheResults: true));
        Assert.Equal("cacheResults", ex.ParamName);

        using var uncached = new OwnedDataPages(writer.Database.Pages, writer.Database.Format, cacheResults: false);
        Assert.Empty(await uncached.GetOwnedDataPagesAsync(0, Ct));

        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        using var cached = new OwnedDataPages(reader.Database.Pages, reader.Database.Format, cacheResults: true);
        Assert.Empty(await cached.GetOwnedDataPagesAsync(0, Ct));
    }

    /// <summary>Writer results are reused until a logical mutation invalidates them.</summary>
    [Fact]
    public async Task WriterOwnedPages_UnchangedLookup_DoesNotReadPages()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using var counting = new CountingStream(stream);
        await using WriterHarness harness = await WriterHarness.OpenAsync(counting, new AccessWriterOptions { PageCacheSize = 0 }, cancellationToken: Ct);
        await harness.InsertRowsAsync(TableName, Enumerable.Range(1, 60).Select(id => new object?[] { id, new string('p', 120) }), Ct);
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(entry);
        IReadOnlyList<long> first = await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct);
        counting.Reset();
        Assert.Equal(first, await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
        Assert.Equal(0, counting.BytesRead);
        await harness.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], [], Ct);
        await harness.InsertRowAsync("Other", [1], Ct);
        Assert.Equal(1, await harness.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Pad"] = "updated" }, Ct));
        counting.Reset();
        Assert.Equal(first, await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
        Assert.Equal(0, counting.BytesRead);
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task GetOwnedDataPages_MatchesForEachOwnedDataPage(string fixture)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(PathOf(fixture), cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        using var uncached = new OwnedDataPages(db.Pages, db.Format, cacheResults: false);

        foreach (string table in await harness.Services.Schema.ListTablesAsync(Ct))
        {
            CatalogEntry? entry = await harness.GetCatalogEntryAsync(table, Ct);
            Assert.NotNull(entry);

            IReadOnlyList<long> owned = await db.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct);
            Assert.Equal(owned, await db.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
            Assert.Equal(owned, await uncached.GetOwnedDataPagesAsync(entry.TDefPage, Ct));

            var visited = new List<long>();
            await db.OwnedPages.ForEachOwnedDataPageAsync(
                entry.TDefPage,
                (pageNumber, _, _) =>
                {
                    visited.Add(pageNumber);
                    return ValueTask.FromResult(true);
                },
                Ct);
            Assert.Equal(owned, visited);

            Assert.Equal(await CountRowsAsync(db.OwnedPages, entry.TDefPage), await CountRowsAsync(uncached, entry.TDefPage));
        }
    }

    /// <summary>
    /// A caching instance remembers a table's owned pages when its usage map
    /// is rejected too, so it reads the TDEF, the map and the pages the map
    /// lists once; a read-only instance that does not cache
    /// reads and validates the map, and then takes the whole-file pass, on
    /// every call.
    /// </summary>
    [Fact]
    public async Task GetOwnedDataPages_RejectedUsageMap_IsValidatedOnceWhenCaching()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        long tdefPage;
        int numRowsOffset;
        await using (WriterHarness writer = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            Assert.Equal(60, await writer.InsertRowsAsync(TableName, Enumerable.Range(1, 60).Select(id => new object?[] { id, new string('p', 120) }), Ct));
            CatalogEntry? entry = await writer.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
            Assert.NotNull(entry);
            tdefPage = entry.TDefPage;
            numRowsOffset = checked((int)(tdefPage * writer.Database.Format.PageSize)) + writer.Database.Format.TDef.NumRows;
        }

        // Declare more rows than the mapped pages hold, so the map is rejected.
        byte[] bytes = stream.ToArray();
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(numRowsOffset), 1_000);

        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(counting, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        using var cached = new OwnedDataPages(db.Pages, db.Format, cacheResults: true);
        using var uncached = new OwnedDataPages(db.Pages, db.Format, cacheResults: false);

        // The first call reads the TDEF page, the map and the pages it lists,
        // and the rejected map sends it on to the whole-file pass, which reads
        // every page from 3 on.
        counting.Reset();
        IReadOnlyList<long> owned = await cached.GetOwnedDataPagesAsync(tdefPage, Ct);
        long firstCallBytes = counting.BytesRead;
        Assert.NotEmpty(owned);
        int pageCount = bytes.Length / db.Format.PageSize;
        Assert.True(
            counting.PagesRead(db.Format.PageSize).IsSupersetOf(Enumerable.Range(3, pageCount - 3).Select(page => (long)page)),
            "The first call did not take the whole-file owned-page pass, so the usage map was not rejected.");

        counting.Reset();
        Assert.Equal(owned, await cached.GetOwnedDataPagesAsync(tdefPage, Ct));
        Assert.True(counting.BytesRead == 0, $"The second call read pages {string.Join(", ", counting.PagesRead(db.Format.PageSize).Order())}.");

        // An instance that does not cache reads the TDEF page, the map and the
        // pages it lists again before the whole-file pass: as many bytes as the
        // caching instance's first call.
        Assert.Equal(owned, await uncached.GetOwnedDataPagesAsync(tdefPage, Ct));
        Assert.Equal(firstCallBytes, counting.BytesRead);
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task DataPageRows_ArrayAndIterator_Agree(string fixture)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(PathOf(fixture), cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        JetFormat format = db.Format;
        int dataPages = 0;

        for (long pageNumber = 1; pageNumber < db.Pages.PageCount; pageNumber++)
        {
            byte[] page = await harness.ReadPageCopyAsync(pageNumber, Ct);
            if (page[0] != Constants.PageTypes.Data)
            {
                continue;
            }

            dataPages++;
            RowBound[] directory = DataPageRows.ComputeRowDirectory(format, page);
            RowBound[] live = [.. DataPageRows.EnumerateLiveRowBounds(format, page)];
            Assert.Equal(directory.Where(bound => !bound.IsOverflowPointer), live);
            Assert.Equal(
                live.Select(bound => new RowLocation(pageNumber, bound.RowIndex, bound.RowStart, bound.RowSize)),
                DataPageRows.EnumerateLiveRowLocations(format, pageNumber, page));

            foreach (RowBound bound in directory)
            {
                Assert.True(DataPageRows.TryGetSlotBound(format, page, bound.RowIndex, out RowBound slot));
                Assert.Equal(bound with { IsOverflowPointer = false }, slot);
            }
        }

        Assert.True(dataPages > 0);
    }

    /// <summary>
    /// The writer's owned pages, read through its <see cref="JetDatabaseWriter.Pages.Paging.Pager"/>
    /// with observer-maintained caching, include the pages a transaction appended past
    /// the physical end of file, and the row walk visits every row the
    /// transaction inserted, in an explicit transaction and in a
    /// <see cref="AccessWriterOptions.UseTransactionalWrites"/> call.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.AutoCommit)]
    public async Task WriterOwnedPages_InTransaction_SeeAppendedPages(DatabaseFormat format, WriteMode mode)
    {
        const int rowCount = 300;
        await using MemoryStream stream = await CreateDatabaseAsync(format);
        AccessWriterOptions options = WriteModes.WriterOptions(mode);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, options, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        long physicalPages = stream.Length / db.Format.PageSize;
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(entry);
        long tdefPage = entry.TDefPage;

        object?[][] rows = [.. Enumerable.Range(1, rowCount).Select(id => new object?[] { id, new string('p', 120) })];
        if (mode == WriteMode.AutoCommit)
        {
            await harness.Services.Transactions.RunAutoCommitAsync(
                async token =>
                {
                    _ = await harness.Services.Data.InsertRowsAsync(TableName, rows, token);
                    Assert.True(harness.Pager.IsJournalActive);
                    await AssertOwnedPagesAsync(db, tdefPage, physicalPages, rowCount, token);
                },
                Ct);
        }
        else
        {
            JetTransaction tx = await harness.Services.Transactions.BeginTransactionAsync(Ct);
            _ = await harness.Services.Data.InsertRowsAsync(TableName, rows, Ct);
            await AssertOwnedPagesAsync(db, tdefPage, physicalPages, rowCount, Ct);
            await tx.CommitAsync(Ct);
        }

        Assert.False(harness.Pager.IsJournalActive);
        await AssertOwnedPagesAsync(db, tdefPage, physicalPages, rowCount, Ct);
    }

    /// <summary>Observed results match a scan through writes, schema replacement and rollback.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task WriterOwnedPages_Mutations_MatchPhysicalScan(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        await AssertScanParityAsync(harness);
        await harness.InsertRowsAsync(TableName, Enumerable.Range(1, 100).Select(id => new object?[] { id, new string('p', 120) }), Ct);
        await AssertScanParityAsync(harness);
        Assert.Equal(1, await harness.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Pad"] = new string('u', 200) }, Ct));
        await AssertScanParityAsync(harness);
        Assert.Equal(1, await harness.DeleteRowsAsync(TableName, "Id", 2, Ct));
        await AssertScanParityAsync(harness);
        await harness.Services.Schema.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), Ct);
        await AssertScanParityAsync(harness);
        await using JetTransaction transaction = await harness.BeginTransactionAsync(Ct);
        await harness.InsertRowsAsync(TableName, Enumerable.Range(101, 100).Select(id => new object?[] { id, new string('p', 120), id }), Ct);
        await AssertScanParityAsync(harness);
        await transaction.RollbackAsync(Ct);
        await AssertScanParityAsync(harness);
        harness.Pager.InvalidateAll();
        await AssertScanParityAsync(harness);
    }

    /// <summary>A corrupted owned pointer and changed page owner never reuse a stale result.</summary>
    [Fact]
    public async Task WriterOwnedPages_CorruptPointerAndOwner_MatchScan()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        await harness.InsertRowsAsync(TableName, Enumerable.Range(1, 60).Select(id => new object?[] { id, new string('p', 120) }), Ct);
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(entry);
        IReadOnlyList<long> owned = await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct);
        Assert.NotEmpty(owned);
        byte[] tdef = await harness.Pager.ReadPageAsync(entry.TDefPage, Ct);
        try
        {
            tdef.AsSpan(harness.Database.Format.TDef.UsedPages, 4).Clear();
            await harness.Pager.WritePageAsync(entry.TDefPage, tdef, Ct);
        }
        finally
        {
            PageBuffers.Return(tdef);
        }

        await AssertScanParityAsync(harness);
        await harness.Pager.WritePageAsync(owned[0], new byte[harness.Database.Format.PageSize], Ct);
        await AssertScanParityAsync(harness);
    }

    /// <summary>Shared map-page siblings retain cached results while owned-map edits invalidate them.</summary>
    /// <param name="referenceMap">Whether the owned row points to a bitmap page.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WriterOwnedPages_SharedMapPage_InvalidatesOnlyChangedOwnedRow(bool referenceMap)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using var counting = new CountingStream(stream);
        await using WriterHarness harness = await WriterHarness.OpenAsync(counting, new AccessWriterOptions { PageCacheSize = 0 }, cancellationToken: Ct);
        await harness.InsertRowsAsync(TableName, Enumerable.Range(1, 60).Select(id => new object?[] { id, new string('p', 120) }), Ct);
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(entry);
        byte[] tdef = await harness.Pager.ReadPageAsync(entry.TDefPage, Ct);
        UsageMapPointer pointer;
        try
        {
            Assert.True(UsageMap.TryReadPointer(tdef, harness.Database.Format.TDef.UsedPages, out pointer));
        }
        finally
        {
            PageBuffers.Return(tdef);
        }

        IReadOnlyList<long> owned = await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct);
        long bitmapNumber = 0;
        if (referenceMap)
        {
            byte[] bitmap = new byte[harness.Database.Format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            foreach (long number in owned)
            {
                bitmap[Constants.UsageMap.ReferenceMapBitmapOffset + checked((int)(number / 8))] |= (byte)(1 << checked((int)(number % 8)));
            }

            bitmapNumber = await harness.Pager.AppendPageAsync(bitmap, Ct);
        }

        byte[] map = await harness.Pager.ReadPageAsync(pointer.PageNumber, Ct);
        try
        {
            if (referenceMap)
            {
                Assert.True(UsageMap.TryGetRowBound(map, harness.Database.Format.DataPage, harness.Database.Format.PageSize, pointer.RowIndex, out RowBound referenceRow));
                map.AsSpan(referenceRow.RowStart, referenceRow.RowSize).Clear();
                map[referenceRow.RowStart] = Constants.UsageMap.ReferenceMapType;
                BinaryPrimitives.WriteInt32LittleEndian(map.AsSpan(referenceRow.RowStart + Constants.UsageMap.ReferenceMapPointerOffset, 4), checked((int)bitmapNumber));
                await harness.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
                Assert.Equal(owned, await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
            }

            Assert.True(UsageMap.TryGetRowBound(map, harness.Database.Format.DataPage, harness.Database.Format.PageSize, pointer.RowIndex + 1, out RowBound sibling));
            map[sibling.RowStart + sibling.RowSize - 1] ^= 1;
            await harness.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            counting.Reset();
            Assert.Equal(owned, await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
            Assert.Equal(0, counting.BytesRead);
            Assert.True(UsageMap.TryGetRowBound(map, harness.Database.Format.DataPage, harness.Database.Format.PageSize, pointer.RowIndex, out RowBound ownedMap));
            if (referenceMap)
            {
                await harness.Pager.WritePageAsync(bitmapNumber, new byte[harness.Database.Format.PageSize], Ct);
                counting.Reset();
                Assert.Equal(owned, await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
                Assert.True(counting.BytesRead > 0);
            }

            map[ownedMap.RowStart] = byte.MaxValue;
            await harness.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
            counting.Reset();
            Assert.Equal(owned, await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
            Assert.True(counting.BytesRead > 0);
        }
        finally
        {
            PageBuffers.Return(map);
        }

        await AssertScanParityAsync(harness);
    }

    private static async Task AssertScanParityAsync(WriterHarness harness)
    {
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(entry);
        var scanned = new List<long>();
        for (long number = 3; number < harness.Pager.PageCount; number++)
        {
            byte[] page = await harness.Pager.ReadUncachedPageAsync(number, Ct);
            try
            {
                if (page[0] == Constants.PageTypes.Data
                    && BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(harness.Database.Format.DataPage.TDefOff, 4)) == entry.TDefPage)
                {
                    scanned.Add(number);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        Assert.Equal(scanned, await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct));
    }

    private static async Task AssertOwnedPagesAsync(DatabaseFile db, long tdefPage, long physicalPagesBefore, int expectedRows, CancellationToken cancellationToken)
    {
        IReadOnlyList<long> owned = await db.OwnedPages.GetOwnedDataPagesAsync(tdefPage, cancellationToken);
        Assert.Contains(owned, pageNumber => pageNumber >= physicalPagesBefore);
        Assert.All(owned, pageNumber => Assert.InRange(pageNumber, 1, db.Pages.PageCount - 1));

        TableDef tableDef = await db.TableDefs.ReadRequiredTableDefAsync(tdefPage, TableName, cancellationToken);
        var ids = new List<int>();
        await db.OwnedPages.ForEachLiveTableRowAsync(
            tdefPage,
            async (row, token) =>
            {
                object?[]? values = await PartialColumnReader.TryReadColumnValuesTypedAsync(db.Format, db.Pages, row.Location, tableDef, [0], token);
                ids.Add(Assert.IsType<int>(values?[0]));
                return true;
            },
            cancellationToken);
        Assert.Equal(Enumerable.Range(1, expectedRows), ids.Order());
    }

    private static async Task<int> CountRowsAsync(OwnedDataPages ownedPages, long tdefPage)
    {
        int rows = 0;
        await ownedPages.ForEachLiveTableRowAsync(
            tdefPage,
            (_, _) =>
            {
                rows++;
                return ValueTask.FromResult(true);
            },
            Ct);
        return rows;
    }

    private static string PathOf(string fixture) => fixture switch
    {
        "Jet3Test" => TestDatabases.Jet3Test,
        "AdventureLT2008" => TestDatabases.AdventureWorks,
        "NorthwindTraders" => TestDatabases.NorthwindTraders,
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, "Unknown fixture."),
    };

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Pad", typeof(string), maxLength: 200)],
                [new IndexDefinition("PK_Items", "Id") { IsPrimaryKey = true }],
                Ct);
        }

        stream.Position = 0;
        return stream;
    }
}
