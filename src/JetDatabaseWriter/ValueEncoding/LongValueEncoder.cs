namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
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
/// <param name="usageMaps">Maintains the owning column maps when the encoder belongs to a writer.</param>
internal sealed class LongValueEncoder(JetFormat format, Pager pager, PageAllocator pageAllocator, AccessWriterOptions options, LongValueUsageMapMaintainer? usageMaps = null)
{
    /// <summary>Validates an external payload before preparing its deferred allocation.</summary>
    /// <param name="data">The unchanged payload bytes.</param>
    /// <returns>The placeholder used to measure the row before writing pages.</returns>
    /// <exception cref="JetLimitationException">The payload exceeds the supported writer limit.</exception>
    internal static PreEncodedLongValue PrepareExternalValue(byte[] data)
    {
        ThrowIfLongerThanLvalLimit(data);
        return new PreEncodedLongValue(new byte[Constants.LongValue.HeaderSize], data);
    }

    /// <summary>Throws when <paramref name="data"/> is too long for an supported writer payload limit.</summary>
    /// <param name="data">The long value's payload.</param>
    /// <exception cref="JetLimitationException">Thrown when <paramref name="data"/> exceeds the supported writer LVAL payload limit.</exception>
    private static void ThrowIfLongerThanLvalLimit(byte[] data)
    {
        if (data.Length > Constants.LongValue.MaxPayloadBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, $"Long value is {data.Length} bytes, which exceeds the supported writer LVAL payload limit of {Constants.LongValue.MaxPayloadBytes} bytes.");
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
    /// <exception cref="JetLimitationException">Thrown when a payload exceeds the supported writer LVAL payload limit.</exception>
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
        if (value is CalculatedError error)
        {
            if (error.NativeCode is null)
            {
                return false;
            }

            if (col.IsCalculated && UsesLongValueStorage(col))
            {
                data = CalculatedColumnUtil.EncodeError(error, isOle: valueType != MemoType);
                inlineCap = Constants.LongValue.MaxInlineBytes;
                return true;
            }

            return false;
        }

        if (col.IsFixed || !UsesLongValueStorage(col) || value is null or DBNull or PreEncodedLongValue)
        {
            return false;
        }

        if (valueType is OleType or BinaryType)
        {
            if (value is not byte[] bytes)
            {
                return false;
            }

            data = col.IsCalculated ? CalculatedColumnUtil.Wrap(bytes) : bytes;
            inlineCap = Constants.LongValue.MaxInlineBytes;
            return true;
        }

        string? text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        data = col.IsCalculated ? format.EncodeText(text, compress: false) : format.EncodeMemoText(text, col.IsCompressedUnicode);
        if (col.IsCalculated)
        {
            data = CalculatedColumnUtil.Wrap(data);
        }

        inlineCap = Constants.LongValue.MaxInlineBytes;
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
    /// <param name="tableTdefPage">The owning table identity.</param>
    /// <param name="tableDef">The column layout assigning every value to its native column.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>Whether any value was written, in which case the row must be serialized again.</returns>
    internal async ValueTask<bool> WriteLongValuesAsync(object[] values, long tableTdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        bool written = false;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is PreEncodedLongValue { PendingPayload: { } payload })
            {
                (UsageMapPointer Owned, UsageMapPointer Free)? maps = usageMaps is not null && tableTdefPage > 0
                    ? await usageMaps.PrepareColumnAsync(tableTdefPage, tableDef.Columns[i].ColNum, cancellationToken).ConfigureAwait(false)
                    : null;
                byte[] header = await this.EncodeAsLvalChainAsync(payload, cancellationToken, maps: maps).ConfigureAwait(false);
                values[i] = new PreEncodedLongValue(header);
                written = true;
            }
        }

        return written;
    }

    internal async ValueTask<PreEncodedLongValue?> ForceEncodeMemoAsLvalAsync(string? text, bool compress, long tableTdefPage, int columnNumber, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        byte[] data = format.EncodeMemoText(text, compress);
        (UsageMapPointer Owned, UsageMapPointer Free)? maps = usageMaps is not null
            ? await usageMaps.PrepareColumnAsync(tableTdefPage, columnNumber, cancellationToken).ConfigureAwait(false)
            : null;
        byte[] header = await this.EncodeAsLvalChainAsync(data, cancellationToken, lvalTokenOverride: 0, maps).ConfigureAwait(false);
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
    /// <param name="maps">The owning column maps, when allocated through a writer.</param>
    /// <exception cref="JetLimitationException">Thrown when <paramref name="data"/> exceeds the supported writer LVAL payload limit.</exception>
    private async ValueTask<byte[]> EncodeAsLvalChainAsync(
        byte[] data,
        CancellationToken cancellationToken,
        uint? lvalTokenOverride = null,
        (UsageMapPointer Owned, UsageMapPointer Free)? maps = null)
    {
        ThrowIfLongerThanLvalLimit(data);
        int pgSz = format.PageSize;
        LvalPageLayout layout = format.LvalPage;

        // Jet3 LVAL pages have no room for a token, and Access 97 leaves the
        // descriptor's token bytes zero.
        uint lvalToken = layout.WritesToken ? lvalTokenOverride ?? LongValueStore.ComputeToken(data) : 0u;

        // Single-page values share pages through their column's free map.
        // Chained rows use dedicated pages and reserve their first four bytes
        // for the next-row pointer. The format supplies native row placement.
        int singleRowMax = layout.SinglePagePayloadCapacity(pgSz);
        int chainRowMax = layout.ChainedPagePayloadCapacity(pgSz);

        if (data.Length <= singleRowMax)
        {
            byte[] page = LongValueStore.BuildSinglePageBuffer(data, lvalToken, pgSz, layout);
            try
            {
                uint reused = usageMaps is not null && maps is { } existingMaps
                    ? await usageMaps.TryAppendRowAsync(existingMaps, page, cancellationToken).ConfigureAwait(false)
                    : 0;
                if (reused != 0)
                {
                    return LongValueDescriptor.SinglePage(data.Length, reused, lvalToken).ToHeaderBytes();
                }

                long pageNumber = await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
                if (usageMaps is not null && maps is { } columnMaps)
                {
                    await usageMaps.MarkPageAsync(columnMaps, pageNumber, Ru16(page, 2) >= 3, cancellationToken).ConfigureAwait(false);
                }

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
                if (usageMaps is not null && maps is { } columnMaps)
                {
                    await usageMaps.MarkPageAsync(columnMaps, pageNumber, hasSpace: false, cancellationToken).ConfigureAwait(false);
                }

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
            if (!UsesLongValueStorage(column))
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
    /// <param name="tableTdefPage">The owning table identity, or zero for a standalone storage test.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    internal async ValueTask DeallocateLongValueAsync(LongValueDescriptor descriptor, CancellationToken cancellationToken, long tableTdefPage = 0)
        => await this.DeallocateLongValuesAsync([descriptor], cancellationToken, tableTdefPage).ConfigureAwait(false);

    /// <summary>Releases a group of values under one temporary ownership topology observer.</summary>
    /// <param name="descriptors">The external values to release.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="tableTdefPage">The owning table, or zero for standalone storage tests.</param>
    /// <returns>A task completed after releasing each reachable LVAL row.</returns>
    internal async ValueTask DeallocateLongValuesAsync(IReadOnlyList<LongValueDescriptor> descriptors, CancellationToken cancellationToken, long tableTdefPage = 0)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!descriptors.Any(descriptor => descriptor.IsExternal && descriptor.FirstDp != 0))
        {
            return;
        }

        LongValueReleaseScope? release = usageMaps is not null && tableTdefPage > 0
            ? new LongValueReleaseScope(format, pager, ct => usageMaps.PrepareTableAsync(tableTdefPage, ct))
            : null;
        try
        {
            if (release is not null)
            {
                _ = await release.GetMapsAsync(cancellationToken).ConfigureAwait(false);
            }

            foreach (LongValueDescriptor descriptor in descriptors)
            {
                await LongValueStore.DeallocateExternalPagesAsync(descriptor, (pointer, ct) => this.ReleaseLvalRowAsync(pointer, release, tableTdefPage, ct), cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            release?.Dispose();
        }
    }

    /// <summary>
    /// Releases the LVAL row <paramref name="lvalDp"/> names and returns the
    /// next-row pointer it starts with. Access packs several values onto one
    /// LVAL page, so while another live row is on the page only this row's
    /// slot is marked deleted and its payload is reclaimed while surviving
    /// slot identities are retained. Reclaimed bytes are zeroed under
    /// <see cref="SecureEraseMode.DeletedRowsAndFreedPages"/>. The page is
    /// freed once no other live row is left on it. A pointer that does not
    /// name a live row of an LVAL page releases nothing and returns 0, so a
    /// damaged chain never frees a page of another kind.
    /// </summary>
    /// <param name="lvalDp">The row pointer (<c>page &lt;&lt; 8 | row</c>).</param>
    /// <param name="release">The temporary map preparation for this release.</param>
    /// <param name="tableTdefPage">The owning table identity.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The next-row pointer stored at the start of the row, or 0.</returns>
    private async ValueTask<uint> ReleaseLvalRowAsync(uint lvalDp, LongValueReleaseScope? release, long tableTdefPage, CancellationToken cancellationToken)
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
                // Refresh can promote a free map. Keep its original fresh
                // preparation boundary, outside the dedicated-page reuse.
                release?.OnInvalidateAll();
                try
                {
                    if (options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages)
                    {
                        Array.Clear(lvalPage, row.RowStart, row.RowSize);
                    }

                    int slotOffset = format.DataPage.RowsStart + (rowIndex * 2);
                    Wu16(lvalPage, slotOffset, Ru16(lvalPage, slotOffset) | Constants.DataPage.DeletedRowFlag);
                    DataPageRows.ReclaimDeletedRowSpace(format, lvalPage, rowIndex, options.SecureEraseMode == SecureEraseMode.DeletedRowsAndFreedPages);
                    await pager.WritePageAsync(pageNumber, lvalPage, cancellationToken).ConfigureAwait(false);
                    if (usageMaps is not null && release is not null)
                    {
                        List<(UsageMapPointer Owned, UsageMapPointer Free)> maps = await usageMaps.PrepareTableAsync(tableTdefPage, cancellationToken).ConfigureAwait(false);
                        await usageMaps.RefreshFreeSpaceAsync(maps, pageNumber, Ru16(lvalPage, 2) >= 3 && Ru16(lvalPage, format.DataPage.NumRows) < Constants.DataPage.MaxRowsPerPage, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    release?.OnInvalidateAll();
                }
            }
            else
            {
                await pageAllocator.DeallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                if (usageMaps is not null && release is not null)
                {
                    // Freeing can promote the global allocation map. Observe
                    // that topology before editing any retained column pointer.
                    IReadOnlyList<(UsageMapPointer Owned, UsageMapPointer Free)> maps = await release.GetMapsAsync(cancellationToken).ConfigureAwait(false);
                    await usageMaps.ClearPageAsync(maps, pageNumber, cancellationToken).ConfigureAwait(false);
                }
            }

            return nextDp;
        }
        finally
        {
            PageBuffers.Return(lvalPage);
        }
    }
}
