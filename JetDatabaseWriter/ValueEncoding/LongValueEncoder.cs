namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.DatabaseFile;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encodes oversized MEMO / OLE / Attachment payloads into LVAL page chains.
/// Owned by <see cref="AccessWriter"/>; the writer delegates long-value
/// pre-encoding through this class.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="pageAllocator">The page allocator.</param>
internal sealed class LongValueEncoder(DatabaseFile db, PageAllocator pageAllocator)
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

            // A calculated column stores its cached value by its result type,
            // so a Memo result in a Text descriptor is a long value too.
            ColumnType valueType = ResolveValueType(col);
            if (col.IsFixed || (valueType != OleType && valueType != MemoType))
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
            if (valueType == OleType)
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

                data = db.EncodeTextForFormat(text, col.IsCompressedUnicode);
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

        byte[] data = db.EncodeTextForFormat(text, compress);
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
    /// <param name="lvalTokenOverride">The token to store instead of the payload hash; ignored on Jet3, which stores none.</param>
    /// <param name="packRowsAtEnd">Whether to write each row at the end of its page on Jet4/ACE too; Jet3 always does.</param>
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

        int pgSz = db.PageSizeBytes;
        LvalPageLayout layout = db.LvalPage;

        // Jet3 LVAL pages have no room for a token, and Access 97 leaves the
        // descriptor's token bytes zero.
        uint lvalToken = layout.WritesToken ? lvalTokenOverride ?? LongValueStore.ComputeToken(data) : 0u;

        // One row per LVAL page; chained rows reserve their first four bytes
        // for the next-page pointer. LvalPageLayout holds the per-format
        // header area and row placement Access uses.
        int singleRowMax = layout.SinglePagePayloadCapacity(pgSz);
        int chainRowMax = layout.ChainedPagePayloadCapacity(pgSz);

        if (data.Length <= singleRowMax)
        {
            byte[] page = LongValueStore.BuildSinglePageBuffer(data, lvalToken, pgSz, layout, packRowsAtEnd);
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
            byte[] page = LongValueStore.BuildChainedPageBuffer(data, chunkStart, chunkLen, nextDp, lvalToken, pgSz, layout, packRowsAtEnd);
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

        if (!db.TryParseRowLayout(page, rowBound.RowStart, rowBound.RowSize, hasVarColumns, out RowLayout layout))
        {
            return roots;
        }

        foreach (ColumnInfo column in tableDef.Columns)
        {
            if (ResolveValueType(column) is not MemoType and not OleType)
            {
                continue;
            }

            ColumnSlice slice = db.ResolveColumnSlice(page, rowBound.RowStart, rowBound.RowSize, layout, column);
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

        byte[] lvalPage = await db.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (lvalPage[0] != Constants.PageTypes.Data)
            {
                return 0;
            }

            foreach (RowBound rowBound in db.EnumerateLiveRowBounds(lvalPage))
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
