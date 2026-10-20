namespace JetDatabaseWriter.LongValues;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
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

    internal static byte[] BuildSinglePageBuffer(ReadOnlySpan<byte> payload, uint token, int pageSize, LvalPageLayout layout)
    {
        byte[] page = ArrayPool<byte>.Shared.Rent(pageSize);
        Array.Clear(page, 0, pageSize);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = pageSize - payload.Length;
        WriteLvalPageHeader(page, layout, token, rowStart);
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
        LvalPageLayout layout)
    {
        byte[] page = ArrayPool<byte>.Shared.Rent(pageSize);
        Array.Clear(page, 0, pageSize);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = pageSize - (length + 4);
        WriteLvalPageHeader(page, layout, token, rowStart);
        Wi32(page, rowStart, unchecked((int)nextDp));
        data.Slice(offset, length).CopyTo(page.AsSpan(rowStart + 4, length));
        return page;
    }

    internal static LvalRowLocation LocateRow(
        int lvalPage,
        int lvalRow,
        byte[] page,
        DataPageLayout dataPage,
        int pageSize,
        ReadOnlySpan<RowBound> rowDirectory)
    {
        if (lvalPage <= 0)
        {
            return new LvalRowLocation([], 0, 0, $"invalid page {lvalPage}");
        }

        if (pageSize <= 0 || page.Length < pageSize || dataPage.NumRows < 0 || dataPage.NumRows > pageSize - 2)
        {
            return new LvalRowLocation(page, 0, 0, "truncated LVAL page or invalid row-count offset");
        }

        if (page[0] != Constants.PageTypes.Data)
        {
            return new LvalRowLocation(page, 0, 0, $"page {lvalPage} not data page");
        }

        int numRows = Ru16(page, dataPage.NumRows);
        if (lvalRow < 0 || lvalRow >= numRows)
        {
            return new LvalRowLocation(page, 0, 0, $"row {lvalRow} >= numRows {numRows}");
        }

        foreach (RowBound rowBound in rowDirectory)
        {
            // LVAL pages hold no overflow rows; a slot flagged overflow is not an LVAL row.
            if (rowBound.RowIndex != lvalRow || rowBound.IsOverflowPointer)
            {
                continue;
            }

            if (rowBound.RowStart <= 0 || rowBound.RowSize < 0 || rowBound.RowStart >= pageSize || rowBound.RowSize > pageSize - rowBound.RowStart)
            {
                return new LvalRowLocation(page, 0, 0, $"invalid rowStart {rowBound.RowStart}");
            }

            return new LvalRowLocation(page, rowBound.RowStart, rowBound.RowSize, null);
        }

        return new LvalRowLocation(page, 0, 0, "deleted/overflow row");
    }

    /// <summary>
    /// Streams exactly the descriptor's declared stored payload without decoding
    /// text, calculated envelopes or OLE content. The descriptor and row padding
    /// are separate preservation obligations. Trailing bytes and next pointers
    /// beyond the declared payload are not interpreted; this is not an ownership
    /// or reclamation proof. Traversal failures can leave a partial destination.
    /// </summary>
    /// <param name="descriptor">The original stored descriptor.</param>
    /// <param name="inlinePayload">The column slice after its descriptor, when inline.</param>
    /// <param name="destination">The staging stream, left open and not flushed.</param>
    /// <param name="pageSize">The physical page size and largest output chunk.</param>
    /// <param name="maxValueBytes">The maximum declared payload length.</param>
    /// <param name="maxChainRows">The maximum external rows, also bounding cycle-tracking storage.</param>
    /// <param name="locateRowAsync">Locates one borrowed row; it must remain valid until its write completes.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="releasePage">Releases each located buffer after its awaited write, including failed locations. If locating throws, the locator retains ownership.</param>
    /// <returns>A task completed after copying the declared payload.</returns>
    /// <exception cref="ArgumentException">The destination is not writable.</exception>
    /// <exception cref="JetCorruptDataException">The descriptor or reachable row content is malformed or incomplete.</exception>
    /// <exception cref="JetLimitationException">The payload or external-row budget is exceeded.</exception>
    internal static async ValueTask CopyPayloadAsync(
        LongValueDescriptor descriptor,
        ReadOnlyMemory<byte> inlinePayload,
        Stream destination,
        int pageSize,
        int maxValueBytes,
        int maxChainRows,
        Func<uint, CancellationToken, ValueTask<LvalRowLocation>> locateRowAsync,
        CancellationToken cancellationToken,
        Action<byte[]>? releasePage = null)
    {
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(locateRowAsync, nameof(locateRowAsync));
        cancellationToken.ThrowIfCancellationRequested();
        ValidatePayload(descriptor, inlinePayload.Length, pageSize, maxValueBytes, maxChainRows);
        if (!destination.CanWrite)
        {
            throw new ArgumentException("The long-value destination must be writable.", nameof(destination));
        }

        if (descriptor.IsInline)
        {
            for (int offset = 0; offset < descriptor.Length;)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int count = Math.Min(pageSize, descriptor.Length - offset);
                await destination.WriteAsync(inlinePayload.Slice(offset, count), cancellationToken).ConfigureAwait(false);
                offset += count;
            }

            return;
        }

        uint currentDp = descriptor.FirstDp;
        int copied = 0;
        int rows = 0;
        SmallLvalDpSet seen = default;
        while (copied < descriptor.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (currentDp == 0)
            {
                throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, $"the chain ended after {copied} of {descriptor.Length} byte(s)");
            }

            if (rows >= maxChainRows)
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, $"The long value exceeds the configured limit of {maxChainRows} external row(s).");
            }

            if (!seen.Add(currentDp))
            {
                throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, "the chain contains a cycle");
            }

            rows++;
            LvalRowLocation location = await locateRowAsync(currentDp, cancellationToken).ConfigureAwait(false);
            try
            {
                if (location.Failed)
                {
                    throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, $"the LVAL row could not be located: {location.Error}");
                }

                int prefixLength = descriptor.IsSinglePage ? 0 : 4;
                int validPageLength = Math.Min(pageSize, location.Page.Length);
                if (location.Start < 0 || location.Size <= prefixLength || location.Size > validPageLength || location.Start > validPageLength - location.Size)
                {
                    throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, $"invalid LVAL row bounds: start {location.Start}, size {location.Size}");
                }

                int available = location.Size - prefixLength;
                if (descriptor.IsSinglePage && available < descriptor.Length)
                {
                    throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, $"the LVAL row is truncated but the descriptor claims {descriptor.Length} byte(s)");
                }

                currentDp = descriptor.IsSinglePage ? 0 : Ru32(location.Page, location.Start);
                int count = Math.Min(available, descriptor.Length - copied);
                await destination.WriteAsync(location.Page.AsMemory(location.Start + prefixLength, count), cancellationToken).ConfigureAwait(false);
                copied += count;
            }
            finally
            {
                if (location.Page.Length > 0)
                {
                    releasePage?.Invoke(location.Page);
                }
            }
        }
    }

    internal static void ValidatePayload(LongValueDescriptor descriptor, int inlineLength, int pageSize, int maxValueBytes, int maxChainRows)
    {
        Guard.Positive(pageSize, nameof(pageSize));
        Guard.InRange(maxValueBytes, 0, int.MaxValue, nameof(maxValueBytes));
        Guard.InRange(maxChainRows, 0, int.MaxValue, nameof(maxChainRows));

        if (descriptor.Length is < 0 or > LongValueDescriptor.MaxLength
            || descriptor.StorageMode is not Constants.LongValue.InlineStorageMode
                and not Constants.LongValue.SinglePageStorageMode
                and not Constants.LongValue.ChainedStorageMode)
        {
            throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, "the long-value descriptor has an invalid length or reserved storage mode");
        }

        if (descriptor.Length > maxValueBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, $"The long value declares {descriptor.Length} stored byte(s), exceeding the configured limit {maxValueBytes}.");
        }

        if (descriptor.IsInline && descriptor.Length > inlineLength)
        {
            throw new JetCorruptDataException(JetErrorCode.UnreadableLongValue, $"the inline value claims {descriptor.Length} byte(s) but the column slice contains fewer bytes");
        }

        if (!descriptor.IsInline && descriptor.Length > 0 && maxChainRows == 0)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The long value requires an external row but the configured row limit is zero.");
        }
    }

    /// <summary>
    /// Releases every LVAL row of an external long value: the one row of a
    /// single-page value, or each row of a chain in order, stopping at a null
    /// pointer or a row already visited.
    /// </summary>
    /// <param name="descriptor">The value's descriptor; inline values hold no LVAL rows.</param>
    /// <param name="releaseRowAsync">
    /// Releases the row a row pointer names and returns the next-row pointer
    /// it held, or 0 when the row is not a live LVAL row, which ends the walk.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    internal static async ValueTask DeallocateExternalPagesAsync(
        LongValueDescriptor descriptor,
        Func<uint, CancellationToken, ValueTask<uint>> releaseRowAsync,
        CancellationToken cancellationToken)
    {
        if (!descriptor.IsExternal || descriptor.FirstDp == 0)
        {
            return;
        }

        if (descriptor.IsSinglePage)
        {
            _ = await releaseRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
            return;
        }

        uint currentDp = descriptor.FirstDp;
        SmallLvalDpSet seen = default;
        while (currentDp != 0 && seen.Add(currentDp))
        {
            cancellationToken.ThrowIfCancellationRequested();
            currentDp = await releaseRowAsync(currentDp, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns whether <paramref name="page"/> is an LVAL page: a data page
    /// (type <c>0x01</c>) whose owner field holds the <c>"LVAL"</c> signature.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <returns><see langword="true"/> for an LVAL page.</returns>
    internal static bool IsLvalPage(ReadOnlySpan<byte> page)
        => page.Length >= 8
            && page[0] == Constants.PageTypes.Data
            && page[4] == (byte)'L'
            && page[5] == (byte)'V'
            && page[6] == (byte)'A'
            && page[7] == (byte)'L';

    private static void WriteLvalPageHeader(byte[] page, LvalPageLayout layout, uint token, int rowStart)
    {
        page[4] = (byte)'L';
        page[5] = (byte)'V';
        page[6] = (byte)'A';
        page[7] = (byte)'L';
        if (layout.WritesToken)
        {
            Wi32(page, 8, unchecked((int)token));
        }

        Wu16(page, layout.DataPage.NumRows, 1);
        Wu16(page, layout.DataPage.RowsStart, rowStart);
        Wu16(page, 2, layout.FreeSpace(rowStart));
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
