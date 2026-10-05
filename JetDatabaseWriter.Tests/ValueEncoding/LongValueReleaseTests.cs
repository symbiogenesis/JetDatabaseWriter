namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Releasing a long value (a SecureErase delete, DropTableAsync or a schema
/// rewrite) frees its LVAL rows, not whole pages. Access packs several values
/// onto one LVAL page, so a page is freed only once no other live row is left
/// on it, and a chain hop that does not name an LVAL page is never freed.
/// </summary>
public sealed class LongValueReleaseTests
{
    private const int FirstRowLength = 40;
    private const int SecondRowLength = 30;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task DeallocateLongValueAsync_SharedPage_DeletesOnlyThatRow(DatabaseFormat format, bool secureErase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        AccessWriterOptions options = Options(secureErase);
        await using MemoryStream ms = await CreateDatabaseAsync(format, ct);
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, options, cancellationToken: ct);
        DatabaseFile db = harness.Database;
        var encoder = new LongValueEncoder(db.Profile, harness.Pager, harness.Services.PageAllocator, options);

        // Two single-page values packed at the end of one LVAL page, as Access writes them.
        int pageSize = db.PageSizeBytes;
        int firstStart = pageSize - FirstRowLength;
        int secondStart = firstStart - SecondRowLength;
        byte[] page = NewLvalPage(db, [firstStart, secondStart]);
        page.AsSpan(firstStart, FirstRowLength).Fill((byte)'A');
        page.AsSpan(secondStart, SecondRowLength).Fill((byte)'B');
        long pageNumber = await harness.Services.PageAllocator.AllocatePageAsync(page, ct);

        await encoder.DeallocateLongValueAsync(LongValueDescriptor.SinglePage(FirstRowLength, LongValueStore.MakeRowPointer(pageNumber, 0), 0), ct);

        byte[] after = await ReadPageCopyAsync(db, pageNumber, ct);
        Assert.Equal(Constants.PageTypes.Data, after[0]);
        int firstSlot = Ru16(after, db.DataPage.RowsStart);
        int secondSlot = Ru16(after, db.DataPage.RowsStart + 2);
        Assert.Equal(firstStart | Constants.DataPage.DeletedRowFlag, firstSlot);
        Assert.Equal(secondStart, secondSlot);
        Assert.Equal(2, Ru16(after, db.DataPage.NumRows));
        Assert.True(after.AsSpan(secondStart, SecondRowLength).IndexOfAnyExcept((byte)'B') < 0, "The other value's row changed.");
        byte expectedFirst = secureErase ? (byte)0 : (byte)'A';
        Assert.True(after.AsSpan(firstStart, FirstRowLength).IndexOfAnyExcept(expectedFirst) < 0, secureErase ? "The released row was not scrubbed." : "The released row was changed.");
        Assert.False(await harness.Services.PageAllocator.IsPageFreeAsync(pageNumber, ct));

        // Releasing the last live row frees the page.
        await encoder.DeallocateLongValueAsync(LongValueDescriptor.SinglePage(SecondRowLength, LongValueStore.MakeRowPointer(pageNumber, 1), 0), ct);

        after = await ReadPageCopyAsync(db, pageNumber, ct);
        Assert.Equal(Constants.PageTypes.Freed, after[0]);
        Assert.True(await harness.Services.PageAllocator.IsPageFreeAsync(pageNumber, ct));
        if (secureErase)
        {
            Assert.True(after.AsSpan(secondStart, SecondRowLength).IndexOfAnyExcept((byte)0) < 0, "The freed page was not scrubbed.");
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task DeallocateLongValueAsync_ChainPointsAtNonLvalPage_StopsWithoutFreeingIt(DatabaseFormat format, bool secureErase)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        AccessWriterOptions options = Options(secureErase);
        await using MemoryStream ms = await CreateDatabaseAsync(format, ct);
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, options, cancellationToken: ct);
        DatabaseFile db = harness.Database;
        var encoder = new LongValueEncoder(db.Profile, harness.Pager, harness.Services.PageAllocator, options);
        CatalogEntry table = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("T", ct));
        byte[] tdefBefore = await ReadPageCopyAsync(db, table.TDefPage, ct);

        // A chained row whose next-row pointer names the table's TDEF page.
        int pageSize = db.PageSizeBytes;
        int rowStart = pageSize - 104;
        byte[] page = NewLvalPage(db, [rowStart]);
        Wi32(page, rowStart, unchecked((int)LongValueStore.MakeRowPointer(table.TDefPage, 0)));
        page.AsSpan(rowStart + 4, 100).Fill((byte)'C');
        long pageNumber = await harness.Services.PageAllocator.AllocatePageAsync(page, ct);

        await encoder.DeallocateLongValueAsync(LongValueDescriptor.Chained(5000, LongValueStore.MakeRowPointer(pageNumber, 0), 0), ct);

        Assert.Equal(tdefBefore, await ReadPageCopyAsync(db, table.TDefPage, ct));
        Assert.False(await harness.Services.PageAllocator.IsPageFreeAsync(table.TDefPage, ct));
        Assert.Equal(Constants.PageTypes.Freed, (await ReadPageCopyAsync(db, pageNumber, ct))[0]);
        Assert.True(await harness.Services.PageAllocator.IsPageFreeAsync(pageNumber, ct));
    }

    private static AccessWriterOptions Options(bool secureErase) => new()
    {
        UseLockFile = false,
        SecureEraseMode = secureErase ? SecureEraseMode.DeletedRowsAndFreedPages : SecureEraseMode.None,
    };

    /// <summary>
    /// Builds an LVAL page whose rows start at <paramref name="rowStarts"/>,
    /// in the database's data-page layout.
    /// </summary>
    /// <param name="db">The database the page is for.</param>
    /// <param name="rowStarts">The row start offsets, in row-index order and descending.</param>
    /// <returns>The page bytes.</returns>
    private static byte[] NewLvalPage(DatabaseFile db, int[] rowStarts)
    {
        byte[] page = new byte[db.PageSizeBytes];
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        "LVAL"u8.CopyTo(page.AsSpan(4));
        Wu16(page, db.DataPage.NumRows, rowStarts.Length);
        for (int row = 0; row < rowStarts.Length; row++)
        {
            Wu16(page, db.DataPage.RowsStart + (row * 2), rowStarts[row]);
        }

        Wu16(page, 2, rowStarts[^1] - (db.DataPage.RowsStart + (rowStarts.Length * 2)));
        return page;
    }

    private static async Task<byte[]> ReadPageCopyAsync(DatabaseFile db, long pageNumber, CancellationToken cancellationToken)
    {
        byte[] page = await db.ReadPageAsync(pageNumber, cancellationToken);
        try
        {
            return page.AsSpan(0, db.PageSizeBytes).ToArray();
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, CancellationToken cancellationToken)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Blob", typeof(byte[]))], cancellationToken);
        }

        ms.Position = 0;
        return ms;
    }
}
