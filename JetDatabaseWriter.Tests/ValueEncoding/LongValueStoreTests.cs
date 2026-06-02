namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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

    [Fact]
    public async Task ReadChainedPayloadAsync_StopsAtCycleAndReturnsBytesRead()
    {
        const int pageSize = 64;
        uint firstDp = LongValueStore.MakeRowPointer(4, rowIndex: 0);
        uint secondDp = LongValueStore.MakeRowPointer(5, rowIndex: 0);
        var rows = new Dictionary<uint, LongValueStore.LvalRowLocation>
        {
            [firstDp] = CreateChainedRow(secondDp, [0x41, 0x42, 0x43], pageSize),
            [secondDp] = CreateChainedRow(firstDp, [0x44, 0x45, 0x46], pageSize),
        };

        LvalChainResult result = await LongValueStore.ReadChainedPayloadAsync(
            firstDp,
            maxLength: 10,
            pageSize,
            (lvalDp, _) => new ValueTask<LongValueStore.LvalRowLocation>(rows[lvalDp]),
            CancellationToken.None);

        Assert.NotNull(result.Data);
        Assert.Equal([0x41, 0x42, 0x43, 0x44, 0x45, 0x46], result.Data);
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

        LongValueStore.LvalRowLocation location = LongValueStore.LocateRow(
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
    public async Task DeallocateExternalPagesAsync_Chained_DeallocatesEachCyclePageOnce()
    {
        uint firstDp = LongValueStore.MakeRowPointer(7, rowIndex: 0);
        uint secondDp = LongValueStore.MakeRowPointer(8, rowIndex: 0);
        var nextPointers = new Dictionary<uint, uint>
        {
            [firstDp] = secondDp,
            [secondDp] = firstDp,
        };
        var deallocatedPages = new List<long>();

        await LongValueStore.DeallocateExternalPagesAsync(
            LongValueDescriptor.Chained(length: 8192, firstDp, token: 0xAABBCCDD),
            (lvalDp, _) => new ValueTask<uint>(nextPointers[lvalDp]),
            (pageNumber, _) =>
            {
                deallocatedPages.Add(pageNumber);
                return ValueTask.CompletedTask;
            },
            CancellationToken.None);

        Assert.Equal([7L, 8L], deallocatedPages);
    }

    private static LongValueStore.LvalRowLocation CreateChainedRow(uint nextDp, byte[] payload, int pageSize)
    {
        byte[] page = new byte[pageSize];
        Wi32(page, 0, unchecked((int)nextDp));
        payload.CopyTo(page.AsSpan(4));
        return new LongValueStore.LvalRowLocation(page, 0, payload.Length + 4, null);
    }
}
