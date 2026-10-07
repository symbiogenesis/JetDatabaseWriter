namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class LongValueStoreTests
{
    [Theory]
    [InlineData(17, Constants.LongValue.InlineStorageMode, 0u, 0u)]
    [InlineData(4096, Constants.LongValue.SinglePageStorageMode, 0x00012300u, 0x89ABCDEFu)]
    [InlineData(9000, Constants.LongValue.ChainedStorageMode, 0x00045600u, 0x10203040u)]
    public void LongValueDescriptor_ToHeaderBytes_RoundTrips(int length, byte storageMode, uint firstDp, uint token)
    {
        var descriptor = new LongValueDescriptor(length, storageMode, firstDp, token);

        byte[] header = descriptor.ToHeaderBytes();

        Assert.True(LongValueDescriptor.TryRead(header, out LongValueDescriptor roundTrip));
        Assert.Equal(descriptor, roundTrip);
    }

    [Fact]
    public void WrapInlineLongValue_WritesDescriptorAndPayload()
    {
        byte[] payload = [0x10, 0x20, 0x30, 0x40];

        byte[]? wrapped = LongValueStore.WrapInlineLongValue(payload);

        Assert.NotNull(wrapped);
        Assert.True(LongValueDescriptor.TryRead(wrapped, out LongValueDescriptor descriptor));
        Assert.Equal(payload.Length, descriptor.Length);
        Assert.True(descriptor.IsInline);
        Assert.Equal(payload, wrapped.AsSpan(Constants.LongValue.HeaderSize, payload.Length).ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 2048, 12, false, true, 2036, 2032)]
    [InlineData(DatabaseFormat.Jet4Mdb, 4096, 20, true, false, 4076, 4072)]
    [InlineData(DatabaseFormat.AceAccdb, 4096, 20, true, false, 4076, 4072)]
    public void LvalPageLayout_For_GivesAccessHeaderAreas(
        DatabaseFormat format,
        int pageSize,
        int minRowStart,
        bool writesToken,
        bool packRowsAtEnd,
        int singlePageCapacity,
        int chainedPageCapacity)
    {
        LvalPageLayout layout = JetFormat.ForNewDatabase(format).LvalPage;

        Assert.Equal(JetFormat.ForNewDatabase(format).DataPage, layout.DataPage);
        Assert.Equal(minRowStart, layout.MinRowStart);
        Assert.Equal(writesToken, layout.WritesToken);
        Assert.Equal(packRowsAtEnd, layout.PackRowsAtEnd);
        Assert.Equal(singlePageCapacity, layout.SinglePagePayloadCapacity(pageSize));
        Assert.Equal(chainedPageCapacity, layout.ChainedPagePayloadCapacity(pageSize));
        Assert.Equal(0, layout.FreeSpace(format == DatabaseFormat.Jet3Mdb ? 12 : 16));
    }

    [Fact]
    public void BuildSinglePageBuffer_Jet4_KeepsExistingLayout()
    {
        const int pageSize = 4096;
        byte[] payload = Payload(100);

        byte[] page = LongValueStore.BuildSinglePageBuffer(payload, 0xDEADBEEF, pageSize, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb).LvalPage, packRowsAtEnd: false);
        try
        {
            AssertLvalPageStart(page);
            Assert.Equal(4, Ru16(page, 2));
            Assert.Equal(0xDEADBEEF, unchecked((uint)Ri32(page, 8)));
            Assert.Equal(1, Ru16(page, 12));
            Assert.Equal(20, Ru16(page, 14));
            Assert.Equal(payload, page.AsSpan(20, payload.Length).ToArray());
            Assert.True(IsZero(page.AsSpan(16, 4)));
            Assert.True(IsZero(page.AsSpan(20 + payload.Length, pageSize - 20 - payload.Length)));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(page);
        }
    }

    [Fact]
    public void BuildSinglePageBuffer_Jet3_WritesJet3HeaderPackedAtEnd()
    {
        const int pageSize = 2048;
        byte[] payload = Payload(100);

        byte[] page = LongValueStore.BuildSinglePageBuffer(payload, 0xDEADBEEF, pageSize, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb).LvalPage, packRowsAtEnd: false);
        try
        {
            AssertLvalPageStart(page);
            Assert.Equal(1936, Ru16(page, 2));
            Assert.Equal(1, Ru16(page, 8));
            Assert.Equal(1948, Ru16(page, 10));
            Assert.True(IsZero(page.AsSpan(12, 1948 - 12)));
            Assert.Equal(payload, page.AsSpan(1948, payload.Length).ToArray());
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(page);
        }
    }

    [Theory]
    [InlineData(2032, 12, 0)]
    [InlineData(618, 1426, 1414)]
    public void BuildChainedPageBuffer_Jet3_FullChunkStartsAfterOffsetTable(int chunkLength, int expectedRowStart, int expectedFreeSpace)
    {
        const int pageSize = 2048;
        byte[] data = Payload(chunkLength + 10);
        uint nextDp = LongValueStore.MakeRowPointer(37, rowIndex: 0);

        byte[] page = LongValueStore.BuildChainedPageBuffer(data, 10, chunkLength, nextDp, 0xDEADBEEF, pageSize, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb).LvalPage, packRowsAtEnd: false);
        try
        {
            AssertLvalPageStart(page);
            Assert.Equal(expectedFreeSpace, Ru16(page, 2));
            Assert.Equal(1, Ru16(page, 8));
            Assert.Equal(expectedRowStart, Ru16(page, 10));
            Assert.True(IsZero(page.AsSpan(12, expectedRowStart - 12)));
            Assert.Equal(nextDp, unchecked((uint)Ri32(page, expectedRowStart)));
            Assert.Equal(data.AsSpan(10, chunkLength).ToArray(), page.AsSpan(expectedRowStart + 4, chunkLength).ToArray());
            Assert.Equal(pageSize, expectedRowStart + 4 + chunkLength);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(page);
        }
    }

    [Fact]
    public async Task ReadChainedPayloadAsync_RejectsCycleBeforeDeclaredLength()
    {
        const int pageSize = 64;
        uint firstDp = LongValueStore.MakeRowPointer(4, rowIndex: 0);
        uint secondDp = LongValueStore.MakeRowPointer(5, rowIndex: 0);
        var rows = new Dictionary<uint, LvalRowLocation>
        {
            [firstDp] = CreateChainedRow(secondDp, [0x41, 0x42, 0x43], pageSize),
            [secondDp] = CreateChainedRow(firstDp, [0x44, 0x45, 0x46], pageSize),
        };

        LvalChainResult result = await LongValueStore.ReadChainedPayloadAsync(
            firstDp,
            maxLength: 10,
            pageSize,
            (lvalDp, _) => new ValueTask<LvalRowLocation>(rows[lvalDp]),
            CancellationToken.None);

        Assert.Null(result.Data);
        Assert.NotNull(result.Error);
    }

    [Fact]
    public async Task ReadChainedPayloadAsync_RejectsTruncatedChain()
    {
        LvalChainResult result = await LongValueStore.ReadChainedPayloadAsync(
            256,
            maxLength: 10,
            pageSize: 64,
            (_, _) => new ValueTask<LvalRowLocation>(CreateChainedRow(0, [1, 2, 3], 64)),
            TestContext.Current.CancellationToken);

        Assert.Null(result.Data);
        Assert.NotNull(result.Error);
    }

    [Fact]
    public async Task ReadChainedPayloadAsync_PropagatesIoFailure()
    {
        var failure = new IOException("read failure");
        IOException actual = await Assert.ThrowsAsync<IOException>(async () =>
            await LongValueStore.ReadChainedPayloadAsync(
                256,
                maxLength: 10,
                pageSize: 64,
                (_, _) => ValueTask.FromException<LvalRowLocation>(failure),
                TestContext.Current.CancellationToken));

        Assert.Same(failure, actual);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(13)]
    public void LocateRow_RejectsTruncatedPageBeforeHeaderRead(int length)
    {
        byte[] page = new byte[length];
        if (length > 0)
        {
            page[0] = Constants.PageTypes.Data;
        }

        LvalRowLocation location = LongValueStore.LocateRow(
            7, 0, page, new DataPageLayout(TDefOff: 4, NumRows: 12, RowsStart: 14), 128, []);

        Assert.True(location.Failed);
    }

    [Fact]
    public void LocateRow_RejectsRowExtendingBeyondPage()
    {
        byte[] page = new byte[128];
        page[0] = Constants.PageTypes.Data;
        var layout = new DataPageLayout(TDefOff: 4, NumRows: 12, RowsStart: 14);
        Wu16(page, layout.NumRows, 1);

        LvalRowLocation location = LongValueStore.LocateRow(
            7, 0, page, layout, 128, [new RowBound(RowIndex: 0, RowStart: 120, RowSize: 20)]);

        Assert.True(location.Failed);
    }

    [Fact]
    public void LocateRow_UsesProvidedLiveRowBounds()
    {
        const int pageSize = 128;
        var dataPage = new DataPageLayout(TDefOff: 4, NumRows: 12, RowsStart: 14);
        byte[] page = new byte[pageSize];
        page[0] = Constants.PageTypes.Data;
        Wu16(page, dataPage.NumRows, 2);
        RowBound[] liveRows = [new(RowIndex: 1, RowStart: 48, RowSize: 11)];

        LvalRowLocation location = LongValueStore.LocateRow(
            lvalPage: 7,
            lvalRow: 1,
            page,
            dataPage,
            pageSize,
            liveRows);

        Assert.False(location.Failed);
        Assert.Same(page, location.Page);
        Assert.Equal(48, location.Start);
        Assert.Equal(11, location.Size);
    }

    [Fact]
    public async Task DeallocateExternalPagesAsync_Chained_ReleasesEachCycleRowOnce()
    {
        uint firstDp = LongValueStore.MakeRowPointer(7, rowIndex: 0);
        uint secondDp = LongValueStore.MakeRowPointer(8, rowIndex: 2);
        var nextPointers = new Dictionary<uint, uint>
        {
            [firstDp] = secondDp,
            [secondDp] = firstDp,
        };
        var releasedRows = new List<uint>();

        await LongValueStore.DeallocateExternalPagesAsync(
            LongValueDescriptor.Chained(length: 8192, firstDp, token: 0xAABBCCDD),
            (lvalDp, _) =>
            {
                releasedRows.Add(lvalDp);
                return new ValueTask<uint>(nextPointers[lvalDp]);
            },
            CancellationToken.None);

        Assert.Equal([firstDp, secondDp], releasedRows);
    }

    [Fact]
    public async Task DeallocateExternalPagesAsync_SinglePage_ReleasesOnlyItsRow()
    {
        uint lvalDp = LongValueStore.MakeRowPointer(9, rowIndex: 3);
        var releasedRows = new List<uint>();

        await LongValueStore.DeallocateExternalPagesAsync(
            LongValueDescriptor.SinglePage(length: 500, lvalDp, token: 0),
            (dp, _) =>
            {
                releasedRows.Add(dp);
                return new ValueTask<uint>(LongValueStore.MakeRowPointer(10, rowIndex: 0));
            },
            CancellationToken.None);

        Assert.Equal([lvalDp], releasedRows);
    }

    [Theory]
    [InlineData(new byte[] { 0x01, 0x01, 0x00, 0x00, (byte)'L', (byte)'V', (byte)'A', (byte)'L' }, true)]
    [InlineData(new byte[] { 0x09, 0x01, 0x00, 0x00, (byte)'L', (byte)'V', (byte)'A', (byte)'L' }, false)]
    [InlineData(new byte[] { 0x01, 0x01, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00 }, false)]
    [InlineData(new byte[] { 0x01, 0x01, 0x00, 0x00, (byte)'L' }, false)]
    public void IsLvalPage_RequiresDataPageWithLvalSignature(byte[] page, bool expected)
        => Assert.Equal(expected, LongValueStore.IsLvalPage(page));

    private static byte[] Payload(int length)
    {
        byte[] payload = new byte[length];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = unchecked((byte)((i * 13) + 1));
        }

        return payload;
    }

    private static void AssertLvalPageStart(byte[] page)
    {
        Assert.Equal(Constants.PageTypes.Data, page[0]);
        Assert.Equal(0x01, page[1]);
        Assert.Equal("LVAL"u8.ToArray(), page.AsSpan(4, 4).ToArray());
    }

    private static bool IsZero(ReadOnlySpan<byte> bytes) => bytes.IndexOfAnyExcept((byte)0) < 0;

    private static LvalRowLocation CreateChainedRow(uint nextDp, byte[] payload, int pageSize)
    {
        byte[] page = new byte[pageSize];
        Wi32(page, 0, unchecked((int)nextDp));
        payload.CopyTo(page.AsSpan(4));
        return new LvalRowLocation(page, 0, payload.Length + 4, null);
    }
}
