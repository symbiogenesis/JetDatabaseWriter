namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Unpublished column-map row pages are released when a later allocation fails.</summary>
public sealed class LongValueUsageMapReservationTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 0, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, 256, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, 0, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, 256, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, 0, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, 256, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, 0, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, 256, true)]
    [InlineData(DatabaseFormat.AceAccdb, 0, false)]
    [InlineData(DatabaseFormat.AceAccdb, 256, false)]
    [InlineData(DatabaseFormat.AceAccdb, 0, true)]
    [InlineData(DatabaseFormat.AceAccdb, 256, true)]
    public async Task LaterRowPageFailure_ReleasesEarlierPagesAndAllowsReuse(DatabaseFormat kind, int cacheSize, bool cancel)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var format = JetFormat.ForNewDatabase(kind);
        await using var stream = new WriteFaultStream();
        stream.SetLength(format.PageSize * 3L);
        byte[] global = new byte[format.PageSize];
        global[0] = Constants.PageTypes.Data;
        global[1] = 1;
        Wu16(global, format.DataPage.NumRows, 1);
        Wu16(global, format.DataPage.RowsStart, format.PageSize - format.UsageMapRowSize);
        stream.Position = format.PageSize;
        stream.Write(global);
#pragma warning disable CA2000 // The awaited pager owns and disposes its codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), cacheSize);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        Assert.Equal(0, await allocator.ScrubFreePagesAsync(ct));
        int rowsPerPage = Math.Min(byte.MaxValue, (format.PageSize - format.DataPage.RowsStart) / (format.UsageMapRowSize + 2));
        int columns = rowsPerPage + 1;
        int rows = columns * 2;
        using var cancellation = new CancellationTokenSource();

        // Each allocated page writes its global-map reservation and its image.
        // The second page fails, or completes and cancels the following allocation.
        if (cancel)
        {
            stream.CancelAfterWrites(4, cancellation);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => LongValueUsageMaps.CreateRowsAsync(format, allocator, rows, cancellation.Token).AsTask());
        }
        else
        {
            stream.FailOnWrite(4);
            await Assert.ThrowsAsync<IOException>(() => LongValueUsageMaps.CreateRowsAsync(format, allocator, rows, ct).AsTask());
        }

        Assert.True(stream.Faulted);
        Assert.Equal(5, pager.PageCount);
        for (long number = 3; number < pager.PageCount; number++)
        {
            Assert.True(await allocator.IsPageFreeAsync(number, ct), $"Unpublished column-map page {number} remained allocated.");
            byte[] released = await pager.ReadPageCopyAsync(number, ct);
            Assert.Equal(Constants.PageTypes.Freed, released[0]);
        }

        UsageMapPointer[] retry = await LongValueUsageMaps.CreateRowsAsync(format, allocator, rows, ct);
        Assert.Equal(rows, retry.Length);
        Assert.Equal(3, retry[0].PageNumber);
        Assert.Equal(3, retry.Select(pointer => pointer.PageNumber).Distinct().Count());
        foreach (int page in retry.Select(pointer => pointer.PageNumber).Distinct())
        {
            Assert.False(await allocator.IsPageFreeAsync(page, ct));
        }
    }
}
