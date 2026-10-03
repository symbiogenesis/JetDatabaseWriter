namespace JetDatabaseWriter.Tests.Pages;

using System;
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
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the owned-page and row-directory seams split out of
/// <see cref="DatabaseFile"/>: <see cref="OwnedDataPages"/> finds the same
/// pages with and without its caches and refuses to cache over the writer's
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
            () => new OwnedDataPages(writer.Database.Pages, writer.Database.Profile, cacheResults: true));
        Assert.Equal("cacheResults", ex.ParamName);

        using var uncached = new OwnedDataPages(writer.Database.Pages, writer.Database.Profile, cacheResults: false);
        Assert.Empty(await uncached.GetOwnedDataPagesAsync(0, Ct));

        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        using var cached = new OwnedDataPages(reader.Database.Pages, reader.Database.Profile, cacheResults: true);
        Assert.Empty(await cached.GetOwnedDataPagesAsync(0, Ct));
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task GetOwnedDataPages_MatchesForEachOwnedDataPage(string fixture)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(PathOf(fixture), cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        using var uncached = new OwnedDataPages(db.Pages, db.Profile, cacheResults: false);

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

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task DataPageRows_ArrayAndIterator_Agree(string fixture)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(PathOf(fixture), cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        JetFormat format = db.Profile;
        int dataPages = 0;

        for (long pageNumber = 1; pageNumber < db.PageCount; pageNumber++)
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
    /// without caching, include the data pages a transaction appended past
    /// the physical end of file, and the row walk visits every row the
    /// transaction inserted, in an explicit transaction and in a
    /// <see cref="AccessWriterOptions.UseTransactionalWrites"/> call.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="transactionalWrites">Whether the rows go in through a <c>UseTransactionalWrites</c> call instead of an explicit transaction.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task WriterOwnedPages_InTransaction_SeeAppendedPages(DatabaseFormat format, bool transactionalWrites)
    {
        const int rowCount = 300;
        await using MemoryStream stream = await CreateDatabaseAsync(format);
        var options = new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = transactionalWrites };
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, options, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        long physicalPages = stream.Length / db.PageSizeBytes;
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(TableName, Ct);
        Assert.NotNull(entry);
        long tdefPage = entry.TDefPage;

        object?[][] rows = [.. Enumerable.Range(1, rowCount).Select(id => new object?[] { id, new string('p', 120) })];
        if (transactionalWrites)
        {
            await harness.Services.Transactions.RunAutoCommitAsync(
                async token =>
                {
                    _ = await harness.Services.Data.InsertRowsAsync(TableName, rows, token);
                    Assert.True(db.IsJournalActive);
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

        Assert.False(db.IsJournalActive);
        await AssertOwnedPagesAsync(db, tdefPage, physicalPages, rowCount, Ct);
    }

    private static async Task AssertOwnedPagesAsync(DatabaseFile db, long tdefPage, long physicalPagesBefore, int expectedRows, CancellationToken cancellationToken)
    {
        IReadOnlyList<long> owned = await db.OwnedPages.GetOwnedDataPagesAsync(tdefPage, cancellationToken);
        Assert.Contains(owned, pageNumber => pageNumber >= physicalPagesBefore);
        Assert.All(owned, pageNumber => Assert.InRange(pageNumber, 1, db.PageCount - 1));

        TableDef tableDef = await db.ReadRequiredTableDefAsync(tdefPage, TableName, cancellationToken);
        var ids = new List<int>();
        await db.OwnedPages.ForEachLiveTableRowAsync(
            tdefPage,
            async (row, token) =>
            {
                object?[]? values = await db.TryReadColumnValuesTypedAsync(row.Location, tableDef, [0], token);
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
                Ct);
        }

        stream.Position = 0;
        return stream;
    }
}
