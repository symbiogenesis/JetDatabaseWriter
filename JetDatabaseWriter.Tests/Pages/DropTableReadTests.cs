namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Catalog index maintenance preserves the other rows of an Access-authored usage-map page.</summary>
/// <param name="output">The test output.</param>
public sealed class DropTableReadTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Northwind's MSysObjects owned-pages row is 381 bytes, and its usage-map
    /// page holds more rows than its two indexes use. Dropping a table must
    /// preserve those other maps, so the catalog can still use its owned map
    /// instead of falling back to a whole-file scan.
    /// </summary>
    [Fact]
    public async Task EmptyTable_DropPreservesCatalogMapsAndAvoidsAWholeFileScan()
    {
        await using var file = new MemoryStream();
        await file.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.NorthwindTraders, Ct), Ct);
        await using var counting = new CountingStream(file);
        await using WriterHarness harness = await WriterHarness.OpenAsync(counting, cancellationToken: Ct);
        JetFormat format = harness.Database.Format;
        byte[] tdef = await harness.Database.Pages.ReadPageCopyAsync(2, Ct);
        Assert.True(UsageMap.TryReadPointer(tdef, format.TDef.UsedPages, out UsageMapPointer ownedMap));
        int indexCount = Ri32(tdef, format.TDef.NumRealIdx);
        int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(format, tdef, Ru16(tdef, format.TDef.NumCols), indexCount);
        var indexPointers = new (int Offset, UsageMapPointer Pointer)[indexCount];
        for (int index = 0; index < indexCount; index++)
        {
            Assert.True(format.Index.TryReadRealIdxSlot(tdef, descriptorStart, index, out var slot));
            int offset = slot.FirstDpOffset - 4;
            Assert.True(UsageMap.TryReadPointer(tdef, offset, out UsageMapPointer pointer));
            Assert.Equal(ownedMap.PageNumber, pointer.PageNumber);
            indexPointers[index] = (offset, pointer);
        }

        Assert.Equal([16, 17], indexPointers.Select(index => index.Pointer.RowIndex));
        byte[] before = await harness.Database.Pages.ReadPageCopyAsync(ownedMap.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(before, format.DataPage, format.PageSize, ownedMap.RowIndex, out RowBound ownedBound));
        Assert.Equal(381, ownedBound.RowSize);

        await harness.CreateTableAsync("BenchDrop", [new("Id", typeof(int)), new("Name", typeof(string), 255)], [], Ct);

        counting.Reset();
        await harness.Services.Transactions.RunAutoCommitAsync(token => harness.Services.Schema.DropTableAsync("BenchDrop", token), Ct);
        long bytesRead = counting.BytesRead;
        output.WriteLine($"Drop read {bytesRead / format.PageSize} pages of a {harness.Database.Pages.PageCount}-page database.");
        bool scannedWholeFile = counting.PagesRead(format.PageSize).IsSupersetOf(
            Enumerable.Range(3, checked((int)harness.Database.Pages.PageCount) - 3).Select(page => (long)page));

        byte[] afterTdef = await harness.Database.Pages.ReadPageCopyAsync(2, Ct);
        foreach ((int offset, UsageMapPointer pointer) in indexPointers)
        {
            Assert.True(UsageMap.TryReadPointer(afterTdef, offset, out UsageMapPointer afterPointer));
            Assert.Equal(pointer, afterPointer);
        }

        byte[] after = await harness.Database.Pages.ReadPageCopyAsync(ownedMap.PageNumber, Ct);
        Assert.Equal(Ru16(before, format.DataPage.NumRows), Ru16(after, format.DataPage.NumRows));
        foreach (RowBound bound in DataPageRows.EnumerateLiveRowBounds(format, before))
        {
            Assert.True(UsageMap.TryGetRowBound(after, format.DataPage, format.PageSize, bound.RowIndex, out RowBound afterBound));
            Assert.Equal(bound, afterBound);
            if (!indexPointers.Any(index => index.Pointer.RowIndex == bound.RowIndex))
            {
                Assert.Equal(before.AsSpan(bound.RowStart, bound.RowSize).ToArray(), after.AsSpan(afterBound.RowStart, afterBound.RowSize).ToArray());
            }
        }

        Assert.False(
            scannedWholeFile,
            $"Dropping an empty table read {bytesRead / format.PageSize} pages of a {harness.Database.Pages.PageCount}-page database, including every unrelated page.");
    }
}
