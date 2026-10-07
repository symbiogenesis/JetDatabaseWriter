namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

public sealed class GlobalAllocationAppendTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Append_ClearsExistingFutureFreeBit(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        await using MemoryStream stream = CreateStream(format, 3, futureFree: true);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        Assert.Equal(3, await allocator.ReserveContiguousPagesAsync(1, TestContext.Current.CancellationToken));
        byte[] file = stream.ToArray();
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(format.PageSize + format.DataPage.RowsStart, 2));
        Assert.Equal(0, file[format.PageSize + rowStart + 5] & (1 << 3));
        Assert.NotEqual(0, file[format.PageSize + rowStart + 5] & (1 << 4));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Append_BeyondInlineRange_PreservesContiguousReservationAndMarksBitmapUsed(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        const int first = (Constants.UsageMap.RowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        await using MemoryStream stream = CreateStream(format, first, futureFree: false);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        Assert.Equal(first, await allocator.ReserveContiguousPagesAsync(3, TestContext.Current.CancellationToken));
        byte[] file = stream.ToArray();
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(format.PageSize + format.DataPage.RowsStart, 2));
        Assert.Equal(Constants.UsageMap.ReferenceMapType, file[format.PageSize + rowStart]);
        int bitmapPage = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(format.PageSize + rowStart + 1, 4));
        Assert.Equal(first + 3, bitmapPage);
        byte[] bitmap = file.AsSpan(bitmapPage * format.PageSize, format.PageSize).ToArray();
        for (int page = 0; page <= bitmapPage; page++)
        {
            Assert.True(UsageMap.TryGetReferencePageState(bitmap, format.PageSize, page, out bool free));
            Assert.False(free, $"Live page {page} is globally free.");
        }

        Assert.True(UsageMap.TryGetReferencePageState(bitmap, format.PageSize, bitmapPage + 1, out bool futureFree));
        Assert.True(futureFree);
    }

    [Theory]
    [InlineData("bitmap-free", false)]
    [InlineData("uncovered-bitmap", false)]
    [InlineData("short-inline", false)]
    [InlineData("header", false)]
    [InlineData("negative", false)]
    [InlineData("outside", false)]
    [InlineData("page-one", false)]
    [InlineData("duplicate", false)]
    [InlineData("wrong-type", false)]
    [InlineData("bitmap-free", true)]
    [InlineData("uncovered-bitmap", true)]
    [InlineData("short-inline", true)]
    [InlineData("header", true)]
    [InlineData("negative", true)]
    [InlineData("outside", true)]
    [InlineData("page-one", true)]
    [InlineData("duplicate", true)]
    [InlineData("wrong-type", true)]
    public async Task MalformedGlobalMap_RefusesBeforeReservationOrFreeing(string corruption, bool freeing)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        await using MemoryStream stream = CreateStream(format, 3, futureFree: false);
        byte[] file = stream.ToArray();
        int root = format.PageSize;
        int start = format.PageSize - Constants.UsageMap.RowSize;
        if (corruption == "header")
        {
            file[root] = Constants.PageTypes.TableDefinition;
        }
        else if (corruption == "short-inline")
        {
            BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(root + format.DataPage.RowsStart, 2), checked((ushort)(format.PageSize - 4)));
        }
        else
        {
            file[root + start] = Constants.UsageMap.ReferenceMapType;
            int pointer = corruption switch
            {
                "negative" => -1,
                "outside" => 99,
                "page-one" => 1,
                _ => 2,
            };
            BinaryPrimitives.WriteInt32LittleEndian(file.AsSpan(root + start + 1, 4), pointer);
            file[2 * format.PageSize] = Constants.PageTypes.TableDefinition;
            if (corruption is "duplicate" or "bitmap-free" or "uncovered-bitmap")
            {
                file[2 * format.PageSize] = Constants.PageTypes.UsageMap;
                file[(2 * format.PageSize) + 1] = 1;
                if (corruption == "duplicate")
                {
                    BinaryPrimitives.WriteInt32LittleEndian(file.AsSpan(root + start + 5, 4), pointer);
                }
                else if (corruption == "bitmap-free")
                {
                    file[(2 * format.PageSize) + Constants.UsageMap.ReferenceMapBitmapOffset] = 1 << 2;
                }
                else
                {
                    BinaryPrimitives.WriteInt32LittleEndian(file.AsSpan(root + start + 1, 4), 0);
                    BinaryPrimitives.WriteInt32LittleEndian(file.AsSpan(root + start + 5, 4), pointer);
                }
            }
        }

        stream.Position = 0;
        stream.Write(file);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        if (freeing)
        {
            _ = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await allocator.DeallocatePageAsync(2, TestContext.Current.CancellationToken));
        }
        else
        {
            _ = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await allocator.ReserveContiguousPagesAsync(1, TestContext.Current.CancellationToken));
        }

        Assert.Equal(file, stream.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Append_InsufficientGlobalCoverage_RefusesBeforeWriting(bool bitmapOverflows)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb);
        int pointerCount = bitmapOverflows ? 2 : 1;
        int capacity = pointerCount * UsageMap.PagesPerReferenceMapPage(format.PageSize);
        int pageCount = capacity - (bitmapOverflows ? 1 : 0);
        await using MemoryStream stream = CreateStream(format, pageCount, futureFree: false);
        byte[] map = new byte[format.PageSize];
        map[0] = Constants.PageTypes.Data;
        map[1] = 1;
        int start = format.PageSize - (1 + (4 * pointerCount));
        BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.NumRows, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.RowsStart, 2), checked((ushort)start));
        map[start] = Constants.UsageMap.ReferenceMapType;
        BinaryPrimitives.WriteInt32LittleEndian(map.AsSpan(start + 1, 4), 2);
        stream.Position = format.PageSize;
        stream.Write(map);
        byte[] bitmap = new byte[format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[1] = 1;
        stream.Position = 2 * format.PageSize;
        stream.Write(bitmap);
        byte[] baseline = stream.ToArray();
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        _ = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await allocator.ReserveContiguousPagesAsync(1, TestContext.Current.CancellationToken));
        Assert.Equal(baseline, stream.ToArray());
    }

    private static MemoryStream CreateStream(JetFormat format, int pageCount, bool futureFree)
    {
        var stream = new MemoryStream();
        stream.SetLength((long)format.PageSize * pageCount);
        byte[] map = new byte[format.PageSize];
        map[0] = Constants.PageTypes.Data;
        map[1] = 1;
        int start = format.PageSize - Constants.UsageMap.RowSize;
        BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.NumRows, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.RowsStart, 2), checked((ushort)start));
        if (futureFree)
        {
            map.AsSpan(start + Constants.UsageMap.InlineMapHeaderSize).Fill(byte.MaxValue);
            map[start + Constants.UsageMap.InlineMapHeaderSize] &= 0xF8;
        }

        stream.Position = format.PageSize;
        stream.Write(map);
        stream.Position = 0;
        return stream;
    }
}
