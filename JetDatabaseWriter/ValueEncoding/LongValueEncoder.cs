namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

#pragma warning disable CA1822 // Mark members as static

/// <summary>
/// Encodes oversized MEMO / OLE / Attachment payloads into LVAL page chains.
/// Owned by <see cref="AccessWriter"/>; the writer delegates long-value
/// pre-encoding through this class.
/// </summary>
internal sealed class LongValueEncoder(AccessWriter writer)
{
    private const int MaxInlineMemoBytes = 1024;
    private const int MaxInlineOleBytes = 256;

    /// <summary>
    /// Wraps short data (≤ inline cap) into the 12-byte inline LVAL header form
    /// (bitmask <c>0x80</c>): header + raw payload contiguous in the row body.
    /// </summary>
    internal static byte[]? WrapInlineLongValue(byte[]? data)
    {
        if (data == null)
        {
            return null;
        }

        var buffer = new byte[12 + data.Length];
        AccessBase.WriteUInt24(buffer, 0, data.Length);
        buffer[3] = 0x80;
        Buffer.BlockCopy(data, 0, buffer, 12, data.Length);
        return buffer;
    }

    /// <summary>
    /// Pre-encode pass for row insert: any MEMO / OLE value whose payload
    /// exceeds the inline cap is written to one or more freshly-appended LVAL
    /// data pages here, and the in-row value is replaced with a
    /// <see cref="PreEncodedLongValue"/> sentinel carrying the matching 12-byte
    /// header. Returns the same array reference when no large payloads were
    /// found and a defensively-cloned array otherwise so the caller's original
    /// <c>values</c> stays untouched.
    /// </summary>
    internal async ValueTask<object[]> PreEncodeLongValuesAsync(TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        object[]? result = null;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            if (col.IsFixed || (col.Type != T_OLE && col.Type != T_MEMO))
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
            if (col.Type == T_OLE)
            {
                data = value as byte[];
                if (data == null)
                {
                    continue;
                }

                inlineCap = MaxInlineOleBytes;
            }
            else
            {
                string? text = value as string ?? Convert.ToString(value, CultureInfo.InvariantCulture);
                if (string.IsNullOrEmpty(text))
                {
                    continue;
                }

                data = writer._format != DatabaseFormat.Jet3Mdb
                    ? AccessBase.EncodeJet4Text(text)
                    : writer.AnsiEncoding.GetBytes(text);
                inlineCap = MaxInlineMemoBytes;
            }

            if (data.Length <= inlineCap)
            {
                continue;
            }

            byte[] header = await EncodeAsLvalChainAsync(data, cancellationToken).ConfigureAwait(false);
            result ??= (object[])values.Clone();
            result[i] = new PreEncodedLongValue(header);
        }

        return result ?? values;
    }

    /// <summary>
    /// Allocates one (single-page LVAL, bitmask <c>0x40</c>) or many (chained
    /// LVAL pages, bitmask <c>0x00</c>) LVAL data pages for a payload that is
    /// too large for the inline form, returning the resulting 12-byte LVAL
    /// header. Pages are appended in reverse so each predecessor row can hold
    /// its successor's <c>lval_dp</c> pointer.
    /// </summary>
    private async ValueTask<byte[]> EncodeAsLvalChainAsync(byte[] data, CancellationToken cancellationToken)
    {
        if (data.Length > Constants.LongValue.MaxPayloadBytes)
        {
            throw new JetLimitationException(
                $"Long value is {data.Length} bytes, which exceeds the JET 24-bit LVAL length limit of {Constants.LongValue.MaxPayloadBytes} bytes.");
        }

        DataPageLayout dataPage = writer._dataPage;
        int pgSz = writer._pgSz;

        // One row per LVAL page. The row table costs 2 bytes for a single offset.
        int singleRowMax = pgSz - dataPage.RowsStart - 2;
        int chainRowMax = singleRowMax - 4; // first 4 bytes of each chained row are the next-pointer

        var header = new byte[12];
        AccessBase.WriteUInt24(header, 0, data.Length);

        if (data.Length <= singleRowMax)
        {
            byte[] page = BuildSingleLvalPageBuffer(data);
            long pageNumber = await writer.AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
            header[3] = 0x40;
            uint lvalDp = unchecked((uint)((pageNumber << 8) | 0));
            AccessBase.Wi32(header, 4, (int)lvalDp);
            return header;
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
            byte[] page = BuildChainLvalPageBuffer(data, chunkStart, chunkLen, nextDp);
            long pageNumber = await writer.AppendPageAsync(page, cancellationToken).ConfigureAwait(false);
            nextDp = unchecked((uint)((pageNumber << 8) | 0));
        }

        header[3] = 0x00;
        AccessBase.Wi32(header, 4, (int)nextDp);
        return header;
    }

    /// <summary>
    /// Builds a single-row LVAL data page (bitmask <c>0x40</c> form): the row
    /// body is the entire payload with no next-pointer prefix.
    /// </summary>
    private byte[] BuildSingleLvalPageBuffer(byte[] payload)
    {
        DataPageLayout dataPage = writer._dataPage;
        int pgSz = writer._pgSz;

        byte[] page = new byte[pgSz];
        page[0] = 0x01; // page_type = data page
        page[1] = 0x01;
        AccessBase.Wi32(page, dataPage.TDefOff, 0);
        AccessBase.Wu16(page, dataPage.NumRows, 1);

        int rowStart = pgSz - payload.Length;
        Buffer.BlockCopy(payload, 0, page, rowStart, payload.Length);
        AccessBase.Wu16(page, dataPage.RowsStart, rowStart);

        int freeSpace = rowStart - (dataPage.RowsStart + 2);
        AccessBase.Wu16(page, 2, freeSpace);
        return page;
    }

    /// <summary>
    /// Builds a single-row LVAL data page in chained form (bitmask <c>0x00</c>):
    /// the first 4 bytes of the row are the next-row pointer (<c>page&lt;&lt;8 | row</c>,
    /// little-endian; <c>0</c> on the terminal page) and the remainder is the chunk payload.
    /// </summary>
    private byte[] BuildChainLvalPageBuffer(byte[] data, int offset, int length, uint nextDp)
    {
        DataPageLayout dataPage = writer._dataPage;
        int pgSz = writer._pgSz;

        byte[] page = new byte[pgSz];
        page[0] = 0x01;
        page[1] = 0x01;
        AccessBase.Wi32(page, dataPage.TDefOff, 0);
        AccessBase.Wu16(page, dataPage.NumRows, 1);

        int rowLen = 4 + length;
        int rowStart = pgSz - rowLen;
        AccessBase.Wi32(page, rowStart, (int)nextDp);
        Buffer.BlockCopy(data, offset, page, rowStart + 4, length);
        AccessBase.Wu16(page, dataPage.RowsStart, rowStart);

        int freeSpace = rowStart - (dataPage.RowsStart + 2);
        AccessBase.Wu16(page, 2, freeSpace);
        return page;
    }
}
