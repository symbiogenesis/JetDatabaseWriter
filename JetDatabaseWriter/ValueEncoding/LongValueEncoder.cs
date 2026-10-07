namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encodes oversized MEMO / OLE / Attachment payloads into LVAL page chains.
/// Owned by <see cref="AccessWriter"/>; the writer delegates long-value
/// pre-encoding through this class.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pager">The writer's page file, through which released LVAL pages are written.</param>
/// <param name="pageAllocator">The page allocator.</param>
/// <param name="options">The writer options; supplies the secure-erase policy for released LVAL rows.</param>
internal sealed class LongValueEncoder(JetFormat format, Pager pager, PageAllocator pageAllocator, AccessWriterOptions options)
{
    /// <summary>Throws when <paramref name="data"/> is too long for an LVAL descriptor's 24-bit length.</summary>
    /// <param name="data">The long value's payload.</param>
    /// <exception cref="JetLimitationException">Thrown when <paramref name="data"/> exceeds the 24-bit JET LVAL length limit.</exception>
    private static void ThrowIfLongerThanLvalLimit(byte[] data)
    {
        if (data.Length > Constants.LongValue.MaxPayloadBytes)
        {
            throw new JetLimitationException(
                $"Long value is {data.Length} bytes, which exceeds the JET 24-bit LVAL length limit of {Constants.LongValue.MaxPayloadBytes} bytes.");
        }
    }

    /// <summary>
    /// First half of a row insert's long-value pass, which writes nothing: any
    /// MEMO / OLE value whose payload exceeds the inline cap is replaced with a
    /// pending <see cref="PreEncodedLongValue"/> that holds the payload and a
    /// zeroed 12-byte header. The caller can then serialize the row, which
    /// checks its values and its size, before any page is written, move more
    /// values out of a row still too long (<see cref="CollectInlineLongValues"/>),
    /// and call <see cref="WriteLongValuesAsync"/> once the row is known to
    /// fit. Returns the same array reference when no value leaves the row and
    /// a clone otherwise, so the caller's original <c>values</c> stays untouched.
    /// </summary>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values.</param>
    /// <returns><paramref name="values"/>, or a clone with the pending sentinels.</returns>
    /// <exception cref="JetLimitationException">Thrown when a payload exceeds the 24-bit JET LVAL length limit.</exception>
    internal object[] PrepareLongValues(TableDef tableDef, object[] values)
    {
        object[]? result = null;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            if (!this.TryGetLongValuePayload(tableDef.Columns[i], values[i], out byte[]? data, out int inlineCap)
                || data.Length <= inlineCap)
            {
                continue;
            }

            ThrowIfLongerThanLvalLimit(data);
            result ??= (object[])values.Clone();
            result[i] = new PreEncodedLongValue(new byte[Constants.LongValue.HeaderSize], data);
        }

        return result ?? values;
    }

    /// <summary>
    /// Returns the MEMO / OLE values of <paramref name="values"/> that are
    /// still stored in the row, each with the payload it would store on LVAL
    /// pages, in the order a row too long for a data page moves them there:
    /// the largest payload first, and of equal ones the first in column order.
    /// Moving a value takes its payload out of the row and leaves a 12-byte
    /// header, as long as the value's inline form. Writes nothing.
    /// </summary>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values <see cref="PrepareLongValues"/> returned.</param>
    /// <returns>Each inline long value's column index and payload, largest payload first.</returns>
    internal List<(int Column, byte[] Payload)> CollectInlineLongValues(TableDef tableDef, object[] values)
    {
        var inline = new List<(int Column, byte[] Payload)>();
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            if (this.TryGetLongValuePayload(tableDef.Columns[i], values[i], out byte[]? data, out _) && data.Length > 0)
            {
                inline.Add((i, data));
            }
        }

        inline.Sort(static (a, b) =>
        {
            int bySize = b.Payload.Length.CompareTo(a.Payload.Length);
            return bySize != 0 ? bySize : a.Column.CompareTo(b.Column);
        });
        return inline;
    }

    /// <summary>
    /// Returns the payload <paramref name="value"/> stores in a MEMO / OLE
    /// column, inline or on LVAL pages, and the column's inline cap: the text
    /// in its stored encoding (compressed Unicode where the column asks for
    /// it) or the bytes, wrapped for a calculated column, whose cached value
    /// is a long value when its result type is Memo or OLE.
    /// </summary>
    /// <param name="col">The column.</param>
    /// <param name="value">The column's value.</param>
    /// <param name="data">The payload.</param>
    /// <param name="inlineCap">The largest payload the row holds inline.</param>
    /// <returns>
    /// <see langword="false"/> for a column that holds no long value, and for
    /// a Null, empty or already encoded value or an OLE value that is not a
    /// byte array, which this pass leaves to the row encoder.
    /// </returns>
    private bool TryGetLongValuePayload(ColumnInfo col, object? value, [NotNullWhen(true)] out byte[]? data, out int inlineCap)
    {
        data = null;
        inlineCap = 0;

        // A calculated column stores its cached value by its result type,
        // so a Memo result in a Text descriptor is a long value too.
        ColumnType valueType = ResolveValueType(col);
        if (col.IsFixed || (valueType != OleType && valueType != MemoType) || value is null or DBNull or PreEncodedLongValue)
        {
            return false;
        }

        if (valueType == OleType)
        {
            if (value is not byte[] bytes)
            {
                return false;
            }

            data = col.IsCalculated ? CalculatedColumnUtil.Wrap(bytes) : bytes;
            inlineCap = Constants.LongValue.MaxInlineOleBytes;
            return true;
        }

        string? text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        data = format.EncodeText(text, col.IsCompressedUnicode);
        if (col.IsCalculated)
        {
            data = CalculatedColumnUtil.Wrap(data);
        }

        inlineCap = Constants.LongValue.MaxInlineMemoBytes;
        return true;
    }

    /// <summary>
    /// Second half of a row insert's long-value pass: writes the payload of each
    /// pending <see cref="PreEncodedLongValue"/> in <paramref name="values"/>,
    /// the array <see cref="PrepareLongValues"/> returned, to LVAL pages, and
    /// replaces it in place with a sentinel carrying the finished header. The
    /// header has the placeholder's size, so the row's length does not change.
    /// </summary>
    /// <param name="values">The values <see cref="PrepareLongValues"/> returned.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>Whether any value was written, in which case the row must be serialized again.</returns>
    internal async ValueTask<bool> WriteLongValuesAsync(object[] values, CancellationToken cancellationToken)
    {
        bool written = false;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is PreEncodedLongValue { PendingPayload: { } payload })
            {
                byte[] header = await this.EncodeAsLvalChainAsync(payload, cancellationToken).ConfigureAwait(false);
                values[i] = new PreEncodedLongValue(header);
                written = true;
            }
        }

        return written;
    }

    internal async ValueTask<PreEncodedLongValue?> ForceEncodeMemoAsLvalAsync(string? text, bool compress, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        byte[] data = format.EncodeText(text, compress);
        byte[] header = await this.EncodeAsLvalChainAsync(data, cancellationToken, lvalTokenOverride: 0).ConfigureAwait(false);
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
    /// <exception cref="JetLimitationException">Thrown when <paramref name="data"/> exceeds the 24-bit JET LVAL length limit.</exception>
    private async ValueTask<byte[]> EncodeAsLvalChainAsync(
        byte[] data,
        CancellationToken cancellationToken,
        uint? lvalTokenOverride = null)
    {
        ThrowIfLongerThanLvalLimit(data);
        int pgSz = format.PageSize;
        LvalPageLayout layout = format.LvalPage;

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
            byte[] page = LongValueStore.BuildSinglePageBuffer(data, lvalToken, pgSz, layout);
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
            byte[] page = LongValueStore.BuildChainedPageBuffer(data, chunkStart, chunkLen, nextDp, lvalToken, pgSz, layout);
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

        if (!RowDecodePlan.TryParseRowLayout(format.RowFields, page, rowBound.RowStart, rowBound.RowSize, hasVarColumns, out RowLayout layout))
        {
            return roots;
        }

        foreach (ColumnInfo column in tableDef.Columns)
        {
            if (ResolveValueType(column) is not MemoType and not OleType)
            {
                continue;
            }

            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, rowBound.RowStart, rowBound.RowSize, layout, column);
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

    /// <summary>
    /// Releases the LVAL rows of a long value that is being deleted, as
    /// <see cref="ReleaseLvalRowAsync"/> describes for each row.
    /// </summary>
    /// <param name="descriptor">The value's descriptor.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    internal async ValueTask DeallocateLongValueAsync(LongValueDescriptor descriptor, CancellationToken cancellationToken)
        => await LongValueStore.DeallocateExternalPagesAsync(descriptor, this.ReleaseLvalRowAsync, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Releases the LVAL row <paramref name="lvalDp"/> names and returns the
    /// next-row pointer it starts with. Access packs several values onto one
    /// LVAL page, so while another live row is on the page only this row's
    /// slot is marked deleted, and its bytes are zeroed under
    /// <see cref="SecureEraseMode.DeletedRowsAndFreedPages"/>. The page is
    /// freed once no other live row is left on it. A pointer that does not
    /// name a live row of an LVAL page releases nothing and returns 0, so a
    /// damaged chain never frees a page of another kind.
    /// </summary>
    /// <param name="lvalDp">The row pointer (<c>page &lt;&lt; 8 | row</c>).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The next-row pointer stored at the start of the row, or 0.</returns>
    private async ValueTask<uint> ReleaseLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        long pageNumber = LongValueStore.PageNumber(lvalDp);
        int rowIndex = LongValueStore.RowIndex(lvalDp);
        if (pageNumber <= 1 || pageNumber >= pager.PageCount)
        {
            return 0;
        }

        byte[] lvalPage = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!LongValueStore.IsLvalPage(lvalPage))
            {
                return 0;
            }

            RowBound? released = null;
            bool otherLiveRows = false;
            foreach (RowBound rowBound in DataPageRows.EnumerateLiveRowBounds(format, lvalPage))
            {
                if (rowBound.RowIndex == rowIndex)
                {
                    released = rowBound;
                }
                else
                {
                    otherLiveRows = true;
                }
            }

            if (released is not RowBound row)
            {
                return 0;
            }

            uint nextDp = row.RowSize >= 4 ? Ru32(lvalPage, row.RowStart) : 0;
            if (otherLiveRows)
            {
                if (options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages)
                {
                    Array.Clear(lvalPage, row.RowStart, row.RowSize);
                }

                int slotOffset = format.DataPage.RowsStart + (rowIndex * 2);
                Wu16(lvalPage, slotOffset, Ru16(lvalPage, slotOffset) | Constants.DataPage.DeletedRowFlag);
                await pager.WritePageAsync(pageNumber, lvalPage, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await pageAllocator.DeallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }

            return nextDp;
        }
        finally
        {
            PageBuffers.Return(lvalPage);
        }
    }
}
