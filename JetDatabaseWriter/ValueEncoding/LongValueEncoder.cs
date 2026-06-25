namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.AccessBase;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encodes oversized MEMO / OLE / Attachment payloads into LVAL page chains.
/// Owned by <see cref="AccessWriter"/>; the writer delegates long-value
/// pre-encoding through this class.
/// </summary>
/// <param name="writer">The writer.</param>
/// <param name="pageAllocator">The page allocator.</param>
internal sealed class LongValueEncoder(AccessWriter writer, PageAllocator pageAllocator)
{
    /// <summary>
    /// Pre-encode pass for row insert: any MEMO / OLE value whose payload
    /// exceeds the inline cap is written to one or more freshly-appended LVAL
    /// data pages here, and the in-row value is replaced with a
    /// <see cref="PreEncodedLongValue"/> sentinel carrying the matching 12-byte
    /// header. Returns the same array reference when no large payloads were
    /// found and a defensively-cloned array otherwise so the caller's original
    /// <c>values</c> stays untouched.
    /// </summary>
    /// <param name="ownerTdefPage">The owner TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<object[]> PreEncodeLongValuesAsync(long ownerTdefPage, TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        _ = ownerTdefPage;
        object[]? result = null;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            if (col.IsFixed || (col.Type != OleType && col.Type != MemoType))
            {
                continue;
            }

            object value = values[i];
            if (value is null or DBNull or PreEncodedLongValue)
            {
                continue;
            }

            byte[]? data;
            int inlineCap;
            if (col.Type == OleType)
            {
                data = value as byte[];
                if (data == null)
                {
                    continue;
                }

                if (col.IsCalculated)
                {
                    data = CalculatedColumnUtil.Wrap(data);
                }

                inlineCap = Constants.LongValue.MaxInlineOleBytes;
            }
            else
            {
                string? text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
                if (string.IsNullOrEmpty(text))
                {
                    continue;
                }

                data = writer.EncodeTextForFormat(text, col.IsCompressedUnicode);
                if (col.IsCalculated)
                {
                    data = CalculatedColumnUtil.Wrap(data);
                }

                inlineCap = Constants.LongValue.MaxInlineMemoBytes;
            }

            if (data.Length <= inlineCap)
            {
                continue;
            }

            byte[] header = await this.EncodeAsLvalChainAsync(data, cancellationToken).ConfigureAwait(false);
            result ??= (object[])values.Clone();
            result[i] = new PreEncodedLongValue(header);
        }

        return result ?? values;
    }

    internal async ValueTask<PreEncodedLongValue?> ForceEncodeMemoAsLvalAsync(string? text, bool compress, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        byte[] data = writer.EncodeTextForFormat(text, compress);
        byte[] header = await this.EncodeAsLvalChainAsync(data, cancellationToken, lvalTokenOverride: 0, packRowsAtEnd: true).ConfigureAwait(false);
        return new PreEncodedLongValue(header);
    }

    /// <summary>
    /// Allocates one (single-page LVAL, bitmask <c>0x40</c>) or many (chained
    /// LVAL pages, bitmask <c>0x00</c>) LVAL data pages for a payload that is
    /// too large for the inline form, returning the resulting 12-byte LVAL
    /// header. Pages are appended in reverse so each predecessor row can hold
    /// its successor's <c>lval_dp</c> pointer.
    /// </summary>
    /// <param name="data">The data bytes or values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <param name="lvalTokenOverride">The long value token override.</param>
    /// <param name="packRowsAtEnd">The pack rows at end.</param>
    /// <exception cref="JetLimitationException">Thrown when <paramref name="data"/> exceeds the 24-bit JET LVAL length limit.</exception>
    private async ValueTask<byte[]> EncodeAsLvalChainAsync(
        byte[] data,
        CancellationToken cancellationToken,
        uint? lvalTokenOverride = null,
        bool packRowsAtEnd = false)
    {
        if (data.Length > Constants.LongValue.MaxPayloadBytes)
        {
            throw new JetLimitationException(
                $"Long value is {data.Length} bytes, which exceeds the JET 24-bit LVAL length limit of {Constants.LongValue.MaxPayloadBytes} bytes.");
        }

        int pgSz = writer.PageSizeBytes;
        uint lvalToken = lvalTokenOverride ?? LongValueStore.ComputeToken(data);

        // One row per LVAL page. Access-authored Jet4/ACE LVAL pages use a
        // 20-byte LVAL header area; chained rows reserve their first four bytes
        // for the next-page pointer.
        int singleRowMax = LongValueStore.SinglePagePayloadCapacity(pgSz);
        int chainRowMax = LongValueStore.ChainedPagePayloadCapacity(pgSz);

        if (data.Length <= singleRowMax)
        {
            byte[] page = LongValueStore.BuildSinglePageBuffer(data, lvalToken, pgSz, packRowsAtEnd);
            try
            {
                long pageNumber = await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
                uint lvalDp = LongValueStore.MakeRowPointer(pageNumber, rowIndex: 0);
                return LongValueDescriptor.SinglePage(data.Length, lvalDp, lvalToken).ToHeaderBytes();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(page);
            }
        }

        // Chunk size for chained rows. Allocating in reverse means each newly
        // appended page's row carries the previously-appended page's lval_dp
        // as its [next_dp] prefix.
        int chunkCount = (data.Length + chainRowMax - 1) / chainRowMax;
        uint nextDp = 0;
        for (int i = chunkCount - 1; i >= 0; i--)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int chunkStart = i * chainRowMax;
            int chunkLen = Math.Min(chainRowMax, data.Length - chunkStart);
            byte[] page = LongValueStore.BuildChainedPageBuffer(data, chunkStart, chunkLen, nextDp, lvalToken, pgSz, packRowsAtEnd);
            try
            {
                long pageNumber = await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
                nextDp = LongValueStore.MakeRowPointer(pageNumber, rowIndex: 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(page);
            }
        }

        return LongValueDescriptor.Chained(data.Length, nextDp, lvalToken).ToHeaderBytes();
    }

    internal List<LongValueDescriptor> CollectLongValueRoots(byte[] page, RowBound rowBound, TableDef tableDef)
    {
        var roots = new List<LongValueDescriptor>();
        bool hasVarColumns = false;
        foreach (ColumnInfo column in tableDef.Columns)
        {
            if (!column.IsFixed)
            {
                hasVarColumns = true;
                break;
            }
        }

        if (!writer.TryParseRowLayout(page, rowBound.RowStart, rowBound.RowSize, hasVarColumns, out RowLayout layout))
        {
            return roots;
        }

        foreach (ColumnInfo column in tableDef.Columns)
        {
            if (column.Type is not MemoType and not OleType)
            {
                continue;
            }

            ColumnSlice slice = writer.ResolveColumnSlice(page, rowBound.RowStart, rowBound.RowSize, layout, column);
            if (slice.Kind is not (ColumnSliceKind.Fixed or ColumnSliceKind.Var) || slice.DataLen < Constants.LongValue.HeaderSize)
            {
                continue;
            }

            int valueStart = rowBound.RowStart + slice.DataStart;
            if (!LongValueDescriptor.TryRead(page.AsSpan(valueStart, slice.DataLen), out LongValueDescriptor descriptor)
                || !descriptor.IsExternal
                || descriptor.FirstDp == 0)
            {
                continue;
            }

            roots.Add(descriptor);
        }

        return roots;
    }

    internal async ValueTask DeallocateLongValueAsync(LongValueDescriptor descriptor, CancellationToken cancellationToken)
        => await LongValueStore.DeallocateExternalPagesAsync(descriptor, this.ReadNextLongValueDpAsync, pageAllocator.DeallocatePageAsync, cancellationToken).ConfigureAwait(false);

    private async ValueTask<uint> ReadNextLongValueDpAsync(uint currentDp, CancellationToken cancellationToken)
    {
        long pageNumber = LongValueStore.PageNumber(currentDp);
        int rowIndex = LongValueStore.RowIndex(currentDp);
        if (pageNumber <= 0)
        {
            return 0;
        }

        byte[] lvalPage = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (lvalPage[0] != Constants.PageTypes.Data)
            {
                return 0;
            }

            foreach (RowBound rowBound in writer.EnumerateLiveRowBounds(lvalPage))
            {
                if (rowBound.RowIndex == rowIndex && rowBound.RowSize >= 4)
                {
                    return Ru32(lvalPage, rowBound.RowStart);
                }
            }

            return 0;
        }
        finally
        {
            ReturnPage(lvalPage);
        }
    }
}
