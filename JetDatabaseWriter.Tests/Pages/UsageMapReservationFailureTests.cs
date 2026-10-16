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
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Unpublished bitmap reservations are released, and failed public map promotion restores the source.</summary>
public sealed class UsageMapReservationFailureTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task WriteRow_EveryBitmapWriteFailure_ReleasesUnpublishedStorage(DatabaseFormat format, bool partialWrite)
    {
        byte[] baseline = await CreateReservationBaselineAsync(format);
        int writes = await MeasureBitmapWritesAsync(baseline);
        Assert.True(writes >= 4);
        for (int fault = 1; fault <= writes; fault++)
        {
            await using var stream = new WriteFaultStream();
            await stream.WriteAsync(baseline, Ct);
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, Options(), cancellationToken: Ct);
            SortedSet<long> before = await PageAudit.FindAllocatedPagesAsync(writer.Database, writer.Services.PageAllocator, Ct);
            if (partialWrite)
            {
                stream.FailDuringWrite(fault);
            }
            else
            {
                stream.FailOnWrite(fault);
            }

            await Assert.ThrowsAsync<IOException>(() => WriteTwoWindowRowAsync(writer, Ct).AsTask());
            Assert.True(stream.Faulted);
            Assert.Empty((await PageAudit.FindAllocatedPagesAsync(writer.Database, writer.Services.PageAllocator, Ct)).Except(before));
            Assert.Empty(await PageAudit.FindUnlinkedReservedPagesAsync(writer.Database, writer.Services.PageAllocator, Ct));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task WriteRow_CancelledBetweenBitmaps_ReleasesFirstBitmap(DatabaseFormat format)
    {
        byte[] baseline = await CreateReservationBaselineAsync(format);
        int writes = await MeasureBitmapWritesAsync(baseline);
        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(baseline, Ct);
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, Options(), cancellationToken: Ct);
        SortedSet<long> before = await PageAudit.FindAllocatedPagesAsync(writer.Database, writer.Services.PageAllocator, Ct);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        stream.CancelAfterWrites(writes / 2, cancellation);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WriteTwoWindowRowAsync(writer, cancellation.Token).AsTask());
        Assert.True(stream.Faulted);
        Assert.Empty((await PageAudit.FindAllocatedPagesAsync(writer.Database, writer.Services.PageAllocator, Ct)).Except(before));
        Assert.Empty(await PageAudit.FindUnlinkedReservedPagesAsync(writer.Database, writer.Services.PageAllocator, Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task PublicInsert_MapPromotionWriteOrFlushFault_RestoresBytesAndAllocation(DatabaseFormat format, bool flushFault)
    {
        byte[] baseline = await CreatePromotionBaselineAsync(format);
        int writes;
        await using (var successful = new WriteFaultStream())
        {
            await successful.WriteAsync(baseline, Ct);
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(successful, Options(), cancellationToken: Ct);
            int before = successful.WriteCount;
            await writer.InsertRowAsync("T", WideRow(writer.Database.Format, 2), Ct);
            writes = successful.WriteCount - before;
            CatalogEntry? table = await writer.Services.Catalog.GetCatalogEntryAsync("T", Ct);
            Assert.NotNull(table);
            byte[] definition = (await writer.Database.TableDefs.ReadTDefBytesAsync(table.TDefPage, Ct))!;
            Assert.True(UsageMap.TryReadPointer(definition, writer.Database.Format.TDef.UsedPages, out UsageMapPointer pointer));
            byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
            Assert.True(UsageMap.TryGetRowBound(page, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound row));
            Assert.Equal(Constants.UsageMap.ReferenceMapType, page[row.RowStart]);
        }

        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(baseline, Ct);
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, Options(), cancellationToken: Ct);
        byte[] original = stream.ToArray();
        long pageCount = harness.Pager.PageCount;
        SortedSet<long> allocated = await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct);
        if (flushFault)
        {
            stream.FailOnFlush(1);
        }
        else
        {
            stream.FailDuringWrite(writes);
        }

        await Assert.ThrowsAsync<IOException>(() => harness.InsertRowAsync("T", WideRow(harness.Database.Format, 2), Ct).AsTask());
        Assert.True(stream.Faulted);
        Assert.Equal(original, stream.ToArray());
        Assert.Equal(pageCount, harness.Pager.PageCount);
        Assert.Equal(allocated, await PageAudit.FindAllocatedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct));
        Assert.Empty(await PageAudit.FindUnlinkedReservedPagesAsync(harness.Database, harness.Services.PageAllocator, Ct));
        await harness.InsertRowAsync("T", WideRow(harness.Database.Format, 2), Ct);
    }

    private static AccessWriterOptions Options() => new() { UseLockFile = false, UseByteRangeLocks = false, PageCacheSize = 0, UseTransactionalWrites = true };

    private static async Task<byte[]> CreateReservationBaselineAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, Options(), leaveOpen: true, Ct))
        {
            long first = await writer.Services.PageAllocator.ReserveContiguousPagesAsync(2, Ct);
            await writer.Services.PageAllocator.DeallocatePageAsync(first, Ct);
            await writer.Services.PageAllocator.DeallocatePageAsync(first + 1, Ct);
        }

        return stream.ToArray();
    }

    private static async Task<int> MeasureBitmapWritesAsync(byte[] baseline)
    {
        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(baseline, Ct);
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, Options(), cancellationToken: Ct);
        int before = stream.WriteCount;
        await WriteTwoWindowRowAsync(writer, Ct);
        return stream.WriteCount - before;
    }

    private static ValueTask WriteTwoWindowRowAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        JetFormat format = writer.Database.Format;
        byte[] rowPage = new byte[format.PageSize];
        int rowStart = format.PageSize - format.UsageMapRowSize;

        // The editor serializes bitmap addresses independently of member-page
        // contents; future addresses force two REFERENCE windows without a huge file.
        long[] members = [3, UsageMap.PagesPerReferenceMapPage(format.PageSize) + 3L];
        return new UsageMapEditor(format, writer.Pager, writer.Services.PageAllocator).WriteRowAsync(rowPage, rowStart, format.UsageMapRowSize, members, keepReferencePages: false, cancellationToken);
    }

    private static async Task<byte[]> CreatePromotionBaselineAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, Options(), leaveOpen: true, Ct))
        {
            JetFormat profile = writer.Database.Format;
            List<ColumnDefinition> columns = [new("Id", typeof(int))];
            for (int index = 0; index < BinaryColumnCount(profile); index++)
            {
                columns.Add(new ColumnDefinition("B" + index.ToString(System.Globalization.CultureInfo.InvariantCulture), typeof(byte[]), maxLength: 255));
            }

            await writer.CreateTableAsync("T", columns, Ct);
            await writer.InsertRowAsync("T", WideRow(profile, 1), Ct);
            await writer.CreateTableAsync("Padding", columns, Ct);
            int window = (profile.UsageMapRowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
            await writer.InsertRowsAsync("Padding", Enumerable.Range(1, window + 8).Select(id => WideRow(profile, id)), Ct);
            Assert.True(writer.Pager.PageCount > window);
        }

        return stream.ToArray();
    }

    private static int BinaryColumnCount(JetFormat format) => (format.PageSize / 255) - 1;

    private static object?[] WideRow(JetFormat format, int id)
    {
        object?[] row = new object?[BinaryColumnCount(format) + 1];
        row[0] = id;
        byte[] payload = new byte[255];
        for (int index = 1; index < row.Length; index++)
        {
            row[index] = payload;
        }

        return row;
    }
}
