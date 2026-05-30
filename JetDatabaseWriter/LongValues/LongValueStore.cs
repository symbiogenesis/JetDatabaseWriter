namespace JetDatabaseWriter.LongValues;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Shared LVAL descriptor, page-buffer, chain-read, and deallocation helpers.
/// </summary>
internal static class LongValueStore
{
    internal static byte[]? WrapInlineLongValue(byte[]? data)
    {
        if (data == null)
        {
            return null;
        }

        byte[] buffer = new byte[Constants.LongValue.HeaderSize + data.Length];
        LongValueDescriptor.Inline(data.Length).WriteTo(buffer);
        Buffer.BlockCopy(data, 0, buffer, Constants.LongValue.HeaderSize, data.Length);
        return buffer;
    }

    internal static uint ComputeToken(ReadOnlySpan<byte> data)
    {
        unchecked
        {
            uint hash = 2166136261;
            for (int index = 0; index < data.Length; index++)
            {
                hash ^= data[index];
                hash *= 16777619;
            }

            hash ^= (uint)data.Length;
            hash *= 16777619;
            return hash == 0 ? 1u : hash;
        }
    }

    internal static uint MakeRowPointer(long pageNumber, int rowIndex)
        => unchecked((uint)((pageNumber << 8) | (uint)rowIndex));

    internal static int PageNumber(uint lvalDp) => (int)(lvalDp >> 8);

    internal static int RowIndex(uint lvalDp) => (int)(lvalDp & 0xFF);

    internal static int SinglePagePayloadCapacity(int pageSize)
        => pageSize - Constants.LongValue.LvalRowStart;

    internal static int ChainedPagePayloadCapacity(int pageSize)
        => SinglePagePayloadCapacity(pageSize) - 4;

    internal static byte[] BuildSinglePageBuffer(ReadOnlySpan<byte> payload, uint token, int pageSize, bool packRowsAtEnd)
    {
        byte[] page = ArrayPool<byte>.Shared.Rent(pageSize);
        Array.Clear(page, 0, pageSize);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = packRowsAtEnd ? pageSize - payload.Length : Constants.LongValue.LvalRowStart;
        WriteLvalPageHeader(page, token, rowStart);
        payload.CopyTo(page.AsSpan(rowStart, payload.Length));
        return page;
    }

    internal static byte[] BuildChainedPageBuffer(
        ReadOnlySpan<byte> data,
        int offset,
        int length,
        uint nextDp,
        uint token,
        int pageSize,
        bool packRowsAtEnd)
    {
        byte[] page = ArrayPool<byte>.Shared.Rent(pageSize);
        Array.Clear(page, 0, pageSize);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = packRowsAtEnd ? pageSize - (length + 4) : Constants.LongValue.LvalRowStart;
        WriteLvalPageHeader(page, token, rowStart);
        Wi32(page, rowStart, unchecked((int)nextDp));
        data.Slice(offset, length).CopyTo(page.AsSpan(rowStart + 4, length));
        return page;
    }

    internal static LvalRowLocation LocateRow(
        uint lvalDp,
        byte[] page,
        DataPageLayout dataPage,
        int pageSize,
        ReadOnlySpan<AccessBase.RowBound> liveRows)
    {
        int lvalPage = PageNumber(lvalDp);
        int lvalRow = RowIndex(lvalDp);
        if (lvalPage <= 0)
        {
            return new LvalRowLocation([], 0, 0, $"invalid page {lvalPage}");
        }

        if (page[0] != Constants.PageTypes.Data)
        {
            return new LvalRowLocation(page, 0, 0, $"page {lvalPage} not data page");
        }

        int numRows = Ru16(page, dataPage.NumRows);
        if (lvalRow >= numRows)
        {
            return new LvalRowLocation(page, 0, 0, $"row {lvalRow} >= numRows {numRows}");
        }

        foreach (AccessBase.RowBound rowBound in liveRows)
        {
            if (rowBound.RowIndex != lvalRow)
            {
                continue;
            }

            if (rowBound.RowStart == 0 || rowBound.RowStart >= pageSize)
            {
                return new LvalRowLocation(page, 0, 0, $"invalid rowStart {rowBound.RowStart}");
            }

            return new LvalRowLocation(page, rowBound.RowStart, rowBound.RowSize, null);
        }

        return new LvalRowLocation(page, 0, 0, "deleted/overflow row");
    }

    internal static async ValueTask<LvalChainResult> ReadChainedPayloadAsync(
        uint firstLvalDp,
        int maxLength,
        int pageSize,
        Func<uint, CancellationToken, ValueTask<LvalRowLocation>> locateRowAsync,
        CancellationToken cancellationToken)
    {
        if (maxLength <= 0)
        {
            return LvalChainResult.Failure("no chunks read");
        }

        byte[]? buffer = null;
        int totalLength = 0;
        uint currentDp = firstLvalDp;
        SmallLvalDpSet seen = default;

        try
        {
            while (currentDp != 0 && totalLength < maxLength && seen.Add(currentDp))
            {
                cancellationToken.ThrowIfCancellationRequested();

                LvalRowLocation location = await locateRowAsync(currentDp, cancellationToken).ConfigureAwait(false);
                if (location.Failed)
                {
                    return LvalChainResult.Failure(location.Error!);
                }

                if (location.Size < 4)
                {
                    return LvalChainResult.Failure($"rowSize {location.Size} < 4");
                }

                currentDp = Ru32(location.Page, location.Start);
                int availableData = location.Size - 4;
                int wantedData = Math.Min(availableData, maxLength - totalLength);

                if (wantedData > 0 && location.Start + 4 + wantedData <= pageSize)
                {
                    buffer ??= new byte[maxLength];
                    Buffer.BlockCopy(location.Page, location.Start + 4, buffer, totalLength, wantedData);
                    totalLength += wantedData;
                }
            }

            if (totalLength == 0)
            {
                return LvalChainResult.Failure("no chunks read");
            }

            if (totalLength == buffer!.Length)
            {
                return LvalChainResult.Success(buffer);
            }

            byte[] result = new byte[totalLength];
            Buffer.BlockCopy(buffer, 0, result, 0, totalLength);
            return LvalChainResult.Success(result);
        }
        catch (IOException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
        catch (OverflowException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
    }

    internal static async ValueTask DeallocateExternalPagesAsync(
        LongValueDescriptor descriptor,
        Func<uint, CancellationToken, ValueTask<uint>> readNextDpAsync,
        Func<long, CancellationToken, ValueTask> deallocatePageAsync,
        CancellationToken cancellationToken)
    {
        if (!descriptor.IsExternal || descriptor.FirstDp == 0)
        {
            return;
        }

        if (descriptor.IsSinglePage)
        {
            int singlePageNumber = PageNumber(descriptor.FirstDp);
            if (singlePageNumber > 0)
            {
                await deallocatePageAsync(singlePageNumber, cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        uint currentDp = descriptor.FirstDp;
        SmallLvalDpSet seen = default;
        while (currentDp != 0 && seen.Add(currentDp))
        {
            cancellationToken.ThrowIfCancellationRequested();
            int pageNumber = PageNumber(currentDp);
            if (pageNumber <= 0)
            {
                return;
            }

            uint nextDp = await readNextDpAsync(currentDp, cancellationToken).ConfigureAwait(false);
            await deallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            currentDp = nextDp;
        }
    }

    private static void WriteLvalPageHeader(byte[] page, uint token, int rowStart)
    {
        page[4] = (byte)'L';
        page[5] = (byte)'V';
        page[6] = (byte)'A';
        page[7] = (byte)'L';
        Wi32(page, 8, unchecked((int)token));
        Wu16(page, 12, 1);
        Wu16(page, 14, rowStart);
        Wu16(page, 2, rowStart - 16);
    }

    internal readonly record struct LvalRowLocation(byte[] Page, int Start, int Size, string? Error)
    {
        public bool Failed => this.Error is not null;
    }

    private struct SmallLvalDpSet
    {
        private const int InlineCapacity = 8;

        private uint value0;
        private uint value1;
        private uint value2;
        private uint value3;
        private uint value4;
        private uint value5;
        private uint value6;
        private uint value7;
        private int count;
        private HashSet<uint>? overflow;

        public bool Add(uint value)
        {
            if (this.overflow is not null)
            {
                return this.overflow.Add(value);
            }

            for (int index = 0; index < this.count; index++)
            {
                if (this.GetValue(index) == value)
                {
                    return false;
                }
            }

            if (this.count < InlineCapacity)
            {
                this.SetValue(this.count, value);
                this.count++;
                return true;
            }

            this.overflow = new HashSet<uint>(InlineCapacity + 1)
            {
                this.value0,
                this.value1,
                this.value2,
                this.value3,
                this.value4,
                this.value5,
                this.value6,
                this.value7,
            };

            return this.overflow.Add(value);
        }

        private readonly uint GetValue(int index) => index switch
        {
            0 => this.value0,
            1 => this.value1,
            2 => this.value2,
            3 => this.value3,
            4 => this.value4,
            5 => this.value5,
            6 => this.value6,
            7 => this.value7,
            _ => throw new ArgumentOutOfRangeException(nameof(index)),
        };

        private void SetValue(int index, uint value)
        {
            switch (index)
            {
                case 0:
                    this.value0 = value;
                    break;

                case 1:
                    this.value1 = value;
                    break;

                case 2:
                    this.value2 = value;
                    break;

                case 3:
                    this.value3 = value;
                    break;

                case 4:
                    this.value4 = value;
                    break;

                case 5:
                    this.value5 = value;
                    break;

                case 6:
                    this.value6 = value;
                    break;

                case 7:
                    this.value7 = value;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(index));
            }
        }
    }
}
