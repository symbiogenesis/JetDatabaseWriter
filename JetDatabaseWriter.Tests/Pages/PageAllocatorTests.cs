namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using Xunit;

public sealed class PageAllocatorTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_InitializesGlobalUsageMapPage(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        byte[] bytes = stream.ToArray();
        int pageSize = PageSizeOf(format);
        DataPageLayout layout = DataPageLayout.For(format);
        ReadOnlySpan<byte> globalMap = bytes.AsSpan(pageSize, pageSize);
        int rowStart = ReadUInt16(globalMap, layout.RowsStart) & 0x1FFF;
        int row1Start = ReadUInt16(globalMap, layout.RowsStart + 2) & 0x1FFF;

        Assert.Equal(0x01, globalMap[0]);
        Assert.Equal(0x01, globalMap[1]);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(globalMap.Slice(layout.TDefOff, 4)));
        Assert.Equal(2, ReadUInt16(globalMap, layout.NumRows));
        Assert.True(row1Start < rowStart);
        Assert.Equal(0x00, globalMap[rowStart]);
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(globalMap.Slice(rowStart + 1, 4)));
        Assert.Equal(0x00, globalMap[row1Start]);
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(globalMap.Slice(row1Start + 1, 4)));
        Assert.False(IsInlineGlobalMapBitSet(bytes, format, 0));
        Assert.False(IsInlineGlobalMapBitSet(bytes, format, 1));
        Assert.False(IsInlineGlobalMapBitSet(bytes, format, 2));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTableAsync_ReusesPageMarkedFreeInGlobalMap(DatabaseFormat format)
    {
        await using var stream = await CreateDatabaseWithTrailingFreePagesAsync(format, 1);
        int pageSize = PageSizeOf(format);
        int reusablePage = (int)(stream.Length / pageSize) - 1;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "ReuseTarget",
                [new ColumnDefinition("Id", typeof(int))],
                TestContext.Current.CancellationToken);
        }

        byte[] bytes = stream.ToArray();
        Assert.Equal(0x02, bytes[reusablePage * pageSize]);
        Assert.False(IsInlineGlobalMapBitSet(bytes, format, reusablePage));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTableAsync_ReusesPageMarkedFreeInReferenceGlobalMap(DatabaseFormat format)
    {
        const int ReusablePage = 520;
        await using var stream = await CreateDatabaseWithReferenceMappedFreePageAsync(format, ReusablePage);
        int pageSize = PageSizeOf(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "ReferenceReuseTarget",
                [new ColumnDefinition("Id", typeof(int))],
                TestContext.Current.CancellationToken);
        }

        byte[] bytes = stream.ToArray();
        Assert.Equal(0x02, bytes[ReusablePage * pageSize]);
        Assert.False(IsReferenceGlobalMapBitSet(bytes, format, ReusablePage));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ShrinkDatabaseAsync_TruncatesTrailingFreePages(DatabaseFormat format)
    {
        await using var stream = await CreateDatabaseWithTrailingFreePagesAsync(format, 3);
        int pageSize = PageSizeOf(format);
        long originalLength = stream.Length - (3L * pageSize);

        long removed;
        await using (var writer = await OpenWriterAsync(stream))
        {
            removed = await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(3, removed);
        Assert.Equal(originalLength, stream.Length);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ShrinkDatabaseAsync_DoesNotRemoveInteriorFreePagesWhenTailIsLive(DatabaseFormat format)
    {
        await using var stream = await CreateDatabaseWithInteriorFreePagesAndLiveTailAsync(format, freePageCount: 2);
        int pageSize = PageSizeOf(format);
        int totalPages = (int)(stream.Length / pageSize);
        int firstInteriorFreePage = totalPages - 3;
        int liveTailPage = totalPages - 1;
        long originalLength = stream.Length;

        long removed;
        await using (var writer = await OpenWriterAsync(stream))
        {
            removed = await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(0, removed);
        Assert.Equal(originalLength, stream.Length);

        byte[] bytes = stream.ToArray();
        for (int pageNumber = firstInteriorFreePage; pageNumber < liveTailPage; pageNumber++)
        {
            Assert.Equal(0x09, bytes[pageNumber * pageSize]);
            Assert.True(IsInlineGlobalMapBitSet(bytes, format, pageNumber));
        }

        Assert.Equal(0x02, bytes[liveTailPage * pageSize]);
        Assert.False(IsInlineGlobalMapBitSet(bytes, format, liveTailPage));
    }

    private static async ValueTask<MemoryStream> CreateDatabaseWithTrailingFreePagesAsync(DatabaseFormat format, int freePageCount)
    {
        await using var seed = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            seed,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        int pageSize = PageSizeOf(format);
        byte[] bytes = seed.ToArray();
        int firstFreePage = bytes.Length / pageSize;
        Array.Resize(ref bytes, bytes.Length + (freePageCount * pageSize));
        for (int pageOffset = 0; pageOffset < freePageCount; pageOffset++)
        {
            int pageNumber = firstFreePage + pageOffset;
            int byteOffset = pageNumber * pageSize;
            bytes[byteOffset] = 0x09;
            bytes[byteOffset + 1] = 0x01;
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(byteOffset + 2, 2), (ushort)(pageSize - 16));
            SetInlineGlobalMapBit(bytes, format, pageNumber, free: true);
        }

        var stream = new MemoryStream();
        await stream.WriteAsync(bytes.AsMemory(), TestContext.Current.CancellationToken);
        stream.Position = 0;
        return stream;
    }

    private static async ValueTask<MemoryStream> CreateDatabaseWithInteriorFreePagesAndLiveTailAsync(DatabaseFormat format, int freePageCount)
    {
        await using var stream = await CreateDatabaseWithTrailingFreePagesAsync(format, freePageCount);
        int pageSize = PageSizeOf(format);
        byte[] bytes = stream.ToArray();
        int liveTailPage = bytes.Length / pageSize;
        Array.Resize(ref bytes, bytes.Length + pageSize);
        int byteOffset = liveTailPage * pageSize;
        bytes[byteOffset] = 0x02;
        bytes[byteOffset + 1] = 0x01;
        SetInlineGlobalMapBit(bytes, format, liveTailPage, free: false);

        var result = new MemoryStream();
        await result.WriteAsync(bytes.AsMemory(), TestContext.Current.CancellationToken);
        result.Position = 0;
        return result;
    }

    private static async ValueTask<MemoryStream> CreateDatabaseWithReferenceMappedFreePageAsync(DatabaseFormat format, int freePageNumber)
    {
        await using var seed = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            seed,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        int pageSize = PageSizeOf(format);
        int referencePageNumber = freePageNumber + 1;
        byte[] bytes = seed.ToArray();
        Array.Resize(ref bytes, (referencePageNumber + 1) * pageSize);

        int freePageOffset = freePageNumber * pageSize;
        bytes[freePageOffset] = Constants.PageTypes.Freed;
        bytes[freePageOffset + 1] = 0x01;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(freePageOffset + 2, 2), (ushort)(pageSize - 16));

        DataPageLayout layout = DataPageLayout.For(format);
        var globalMap = bytes.AsSpan(pageSize, pageSize);
        int rowStart = ReadUInt16(globalMap, layout.RowsStart) & Constants.DataPage.RowOffsetMask;
        globalMap.Slice(rowStart, Constants.UsageMap.RowSize).Clear();
        globalMap[rowStart] = Constants.UsageMap.ReferenceMapType;
        BinaryPrimitives.WriteInt32LittleEndian(
            globalMap.Slice(rowStart + Constants.UsageMap.ReferenceMapPointerOffset, 4),
            referencePageNumber);

        var referenceMap = bytes.AsSpan(referencePageNumber * pageSize, pageSize);
        referenceMap[0] = Constants.PageTypes.UsageMap;
        int pagesPerReferenceMap = (pageSize - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;
        int bitIndex = freePageNumber % pagesPerReferenceMap;
        referenceMap[Constants.UsageMap.ReferenceMapBitmapOffset + (bitIndex / 8)] |= (byte)(1 << (bitIndex % 8));

        var stream = new MemoryStream();
        await stream.WriteAsync(bytes.AsMemory(), TestContext.Current.CancellationToken);
        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static bool IsInlineGlobalMapBitSet(byte[] bytes, DatabaseFormat format, int pageNumber)
    {
        int pageSize = PageSizeOf(format);
        DataPageLayout layout = DataPageLayout.For(format);
        ReadOnlySpan<byte> globalMap = bytes.AsSpan(pageSize, pageSize);
        int rowStart = ReadUInt16(globalMap, layout.RowsStart) & 0x1FFF;
        int basePage = BinaryPrimitives.ReadInt32LittleEndian(globalMap.Slice(rowStart + 1, 4));
        int bitIndex = pageNumber - basePage;
        Assert.InRange(bitIndex, 0, 511);
        return (globalMap[rowStart + 5 + (bitIndex / 8)] & (1 << (bitIndex % 8))) != 0;
    }

    private static void SetInlineGlobalMapBit(byte[] bytes, DatabaseFormat format, int pageNumber, bool free)
    {
        int pageSize = PageSizeOf(format);
        DataPageLayout layout = DataPageLayout.For(format);
        var globalMap = bytes.AsSpan(pageSize, pageSize);
        int rowStart = ReadUInt16(globalMap, layout.RowsStart) & 0x1FFF;
        int basePage = BinaryPrimitives.ReadInt32LittleEndian(globalMap.Slice(rowStart + 1, 4));
        int bitIndex = pageNumber - basePage;
        Assert.InRange(bitIndex, 0, 511);
        int byteOffset = rowStart + 5 + (bitIndex / 8);
        byte bitMask = (byte)(1 << (bitIndex % 8));
        if (free)
        {
            globalMap[byteOffset] |= bitMask;
        }
        else
        {
            globalMap[byteOffset] &= unchecked((byte)~bitMask);
        }
    }

    private static bool IsReferenceGlobalMapBitSet(byte[] bytes, DatabaseFormat format, int pageNumber)
    {
        int pageSize = PageSizeOf(format);
        DataPageLayout layout = DataPageLayout.For(format);
        ReadOnlySpan<byte> globalMap = bytes.AsSpan(pageSize, pageSize);
        int rowStart = ReadUInt16(globalMap, layout.RowsStart) & Constants.DataPage.RowOffsetMask;
        Assert.Equal(Constants.UsageMap.ReferenceMapType, globalMap[rowStart]);

        int pagesPerReferenceMap = (pageSize - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;
        int pointerIndex = pageNumber / pagesPerReferenceMap;
        int pointerOffset = rowStart + Constants.UsageMap.ReferenceMapPointerOffset + (pointerIndex * 4);
        int referencePageNumber = BinaryPrimitives.ReadInt32LittleEndian(globalMap.Slice(pointerOffset, 4));
        Assert.InRange(referencePageNumber, 1, (bytes.Length / pageSize) - 1);

        ReadOnlySpan<byte> referenceMap = bytes.AsSpan(referencePageNumber * pageSize, pageSize);
        Assert.Equal(Constants.PageTypes.UsageMap, referenceMap[0]);
        int bitIndex = pageNumber % pagesPerReferenceMap;
        return (referenceMap[Constants.UsageMap.ReferenceMapBitmapOffset + (bitIndex / 8)] & (1 << (bitIndex % 8))) != 0;
    }

    private static int ReadUInt16(ReadOnlySpan<byte> bytes, int offset)
        => BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset, 2));

    private static int PageSizeOf(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;
}
