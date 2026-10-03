namespace JetDatabaseWriter.ValueDecoding;

using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.ValueDecoding.Models;

/// <summary>
/// Decodes the rows of a cached data page into typed values or strings,
/// following MEMO / OLE long-value chains when a row needs them.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="pages">The reader's page cache.</param>
/// <param name="longValues">Resolves MEMO / OLE long-value chains.</param>
/// <param name="strictParsing">Whether malformed values throw instead of decoding to a fallback.</param>
internal sealed class RowDecoder(DatabaseFile db, ReaderPageCache pages, LongValueDecoder longValues, bool strictParsing)
{
    /// <summary>Gets a value indicating whether malformed values throw instead of decoding to a fallback.</summary>
    internal bool StrictParsing => strictParsing;

    /// <summary>
    /// Yields rows from every data page whose owning TDEF page equals <paramref name="tdefPage"/>.
    /// Centralises the common scan-all-pages-and-decode-rows pattern used by catalog/system-table readers.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async IAsyncEnumerable<string[]> EnumerateRowsForTdefAsync(
        long tdefPage,
        TableDef td,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateStrings(td, strictParsing);
        foreach (long pageNumber in pageNumbers)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);

            await foreach (string[] row in this.EnumerateRowsAsync(pageNumber, page, decodePlan, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }
        }
    }

    /// <summary>Yields decoded rows from a single data page.</summary>
    /// <param name="pageNumber">The page number, used to memoize the parsed live-row directory in the row-bounds cache.</param>
    /// <param name="page">The data page to enumerate rows from.</param>
    /// <param name="td">The table definition containing column information.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for rows.</param>
    internal IAsyncEnumerable<string[]> EnumerateRowsAsync(long pageNumber, byte[] page, TableDef td, CancellationToken cancellationToken)
    {
        var decodePlan = RowDecodePlan.CreateStrings(td, strictParsing);
        return this.EnumerateRowsAsync(pageNumber, page, decodePlan, cancellationToken);
    }

    /// <summary>Yields the rows of a single data page decoded as strings.</summary>
    /// <param name="pageNumber">The page number, used to memoize the parsed live-row directory in the row-bounds cache.</param>
    /// <param name="page">The data page to enumerate rows from.</param>
    /// <param name="decodePlan">The string decode plan.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for rows.</param>
    internal async IAsyncEnumerable<string[]> EnumerateRowsAsync(long pageNumber, byte[] page, RowDecodePlan decodePlan, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (RowBound rb in pages.GetLiveRowBounds(pageNumber, page))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (rb.RowSize < db.RowFields.NumCols)
            {
                continue;
            }

            string[]? values = await decodePlan.TryDecodeStringRowAsync(db, page, rb.RowStart, rb.RowSize, longValues, cancellationToken).ConfigureAwait(false);
            if (values != null)
            {
                yield return values;
            }
        }
    }

    // ── Typed row cracker ────────────────────────────────────
    //
    // CrackRowTypedAsync fills an object?[] of length td.Columns.Count
    // directly from the page bytes — no intermediate List<string> + per-
    // column culture-invariant formatting + re-parse round-trip. Fixed-
    // width primitives go through JetTypeInfo.ReadFixedTyped; variable-
    // width text goes straight to a managed string; Binary is copied as
    // byte[]; Memo/Ole keep their async branch only when the LVAL
    // chain actually needs to be walked (the inline 0x80 case stays sync).
    // RowDecodePlan carries the optional projection mask: unwanted columns
    // are left as null, while the row layout is still parsed once so variable
    // offsets remain valid for every wanted column.
    //
    // The split is exposed as TryCrackRowSync — callers that know they
    // are on the fully-sync hot path (e.g. fixed-only / inline-only
    // tables) can avoid the await/state-machine cost entirely.
    // Cancellation is checked once per row, not per column.
    //
    // The table readers wire this in; complex-attachment resolution and
    // Hyperlink wrapping are applied as post-processing passes gated by the
    // per-table HasComplexColumns / HasHyperlinkColumns flags.

    /// <summary>Decodes one row into a freshly allocated typed value array.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="decodePlan">The decode plan.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The row values, or <see langword="null"/> when the row is malformed.</returns>
    internal ValueTask<object?[]?> CrackRowTypedAsync(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!this.TryCrackRowSync(page, rowStart, rowSize, decodePlan, out object?[]? row, out bool needsLongValue))
        {
            return new ValueTask<object?[]?>((object?[]?)null);
        }

        // Fast path: no Memo/Ole LVAL chain walk needed — return a
        // sync-completed ValueTask so the caller never builds an async
        // state machine for fixed-only / inline-only rows.
        if (!needsLongValue)
        {
            return new ValueTask<object?[]?>(row);
        }

        return this.ResolveLongValueRefsAsync(row!, page, cancellationToken);
    }

    /// <summary>
    /// Buffer-filling counterpart to <c>CrackRowTypedAsync</c>.
    /// Returns <see langword="true"/> when the row was successfully decoded
    /// into the first <c>td.Columns.Count</c> slots of
    /// <paramref name="buffer"/>; <see langword="false"/> when the row
    /// trailer was malformed (caller should skip without resetting the
    /// buffer — the next iteration will overwrite it). Lets non-yielding
    /// scans reuse a single <see cref="ArrayPool{T}.Shared"/>-rented array
    /// across the entire scan.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="decodePlan">The decode plan.</param>
    /// <param name="buffer">The buffer.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<bool> CrackRowTypedIntoBufferAsync(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, object?[] buffer, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!this.TryCrackRowSyncIntoBuffer(page, rowStart, rowSize, decodePlan, buffer, out bool needsLongValue))
        {
            return new ValueTask<bool>(false);
        }

        if (!needsLongValue)
        {
            return new ValueTask<bool>(true);
        }

        return this.ResolveLongValueRefsIntoBufferAsync(buffer, decodePlan.ColumnCount, page, cancellationToken);
    }

    /// <summary>
    /// Buffer-aware mirror of <c>ResolveLongValueRefsAsync</c>: walks only
    /// the first <paramref name="validLength"/> slots of
    /// <paramref name="buffer"/> (the pooled array may be larger than
    /// <c>td.Columns.Count</c>).
    /// </summary>
    /// <param name="buffer">The buffer.</param>
    /// <param name="validLength">The valid length.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<bool> ResolveLongValueRefsIntoBufferAsync(object?[] buffer, int validLength, byte[] page, CancellationToken cancellationToken)
    {
        for (int i = 0; i < validLength; i++)
        {
            if (buffer[i] is LongValueRef lvr)
            {
                buffer[i] = lvr.IsOle
                    ? await longValues.ReadOleValueBytesAsync(page, lvr.Start, lvr.Len, cancellationToken).ConfigureAwait(false)
                    : await longValues.ReadLongValueAsync(page, lvr.Start, lvr.Len, isOle: false, cancellationToken).ConfigureAwait(false);
            }
            else if (buffer[i] is CalculatedLongValueRef clvr)
            {
                buffer[i] = await this.ResolveCalculatedLongValueRefAsync(page, clvr, cancellationToken).ConfigureAwait(false);
            }
        }

        return true;
    }

    /// <summary>
    /// Async slow-path that walks the LVAL chain for any
    /// <see cref="LongValueRef"/> sentinels left in <paramref name="row"/>
    /// by <c>TryCrackRowSync</c>. Only invoked when at least one
    /// such sentinel was emitted — fixed-only / inline-only rows skip this
    /// entirely and never allocate an async state machine.
    /// </summary>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<object?[]?> ResolveLongValueRefsAsync(object?[] row, byte[] page, CancellationToken cancellationToken)
    {
        _ = await this.ResolveLongValueRefsIntoBufferAsync(row, row.Length, page, cancellationToken).ConfigureAwait(false);
        return row;
    }

    private async ValueTask<object> ResolveCalculatedLongValueRefAsync(byte[] page, CalculatedLongValueRef reference, CancellationToken cancellationToken)
    {
        byte[] raw = await longValues.ReadLongValueRawBytesAsync(page, reference.Start, reference.Len, cancellationToken).ConfigureAwait(false);
        byte[] payload = CalculatedColumnUtil.Unwrap(raw);
        return reference.IsOle
            ? OleObjectDecoder.DecodeOleValueBytes(payload, 0, payload.Length)
            : longValues.DecodeLongValue(payload, 0, payload.Length, isOle: false);
    }

    /// <summary>
    /// Synchronously decodes a row into a typed <c>object?[]</c>. Returns
    /// <see langword="false"/> when the row trailer is malformed or the
    /// schema sanity-check rejects the row (caller should skip).
    /// <paramref name="needsLongValue"/> is set when one or more
    /// <c>Memo</c>/<c>Ole</c> slots require an LVAL-chain walk; those
    /// slots are filled with a <see cref="LongValueRef"/> sentinel that the
    /// async wrapper (<c>CrackRowTypedAsync</c>) replaces.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="decodePlan">The decode plan.</param>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="needsLongValue">The needs long value.</param>
    private bool TryCrackRowSync(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, out object?[]? row, out bool needsLongValue)
    {
        object?[] result = new object?[decodePlan.ColumnCount];
        if (!this.TryCrackRowSyncIntoBuffer(page, rowStart, rowSize, decodePlan, result, out needsLongValue))
        {
            row = null;
            return false;
        }

        row = result;
        return true;
    }

    /// <summary>
    /// Buffer-filling core of <c>TryCrackRowSync</c>: lets non-yielding callers
    /// rent a single <c>object?[]</c> from <see cref="ArrayPool{T}.Shared"/>
    /// and re-use it across every row instead of allocating a fresh array
    /// per row. <paramref name="buffer"/> must have length
    /// &gt;= <c>td.Columns.Count</c>; the first <c>td.Columns.Count</c>
    /// slots are fully overwritten on success.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="decodePlan">The decode plan.</param>
    /// <param name="buffer">The buffer.</param>
    /// <param name="needsLongValue">The needs long value.</param>
    private bool TryCrackRowSyncIntoBuffer(byte[] page, int rowStart, int rowSize, RowDecodePlan decodePlan, object?[] buffer, out bool needsLongValue)
        => decodePlan.TryDecodeTypedIntoBuffer(db, page, rowStart, rowSize, longValues, buffer, out needsLongValue);
}
