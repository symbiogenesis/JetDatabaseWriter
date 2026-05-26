namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

/// <summary>
/// Encodes oversized MEMO / OLE / Attachment payloads into LVAL page chains.
/// Owned by <see cref="AccessWriter"/>; the writer delegates long-value
/// pre-encoding through this class.
/// </summary>
internal sealed class LongValueEncoder(AccessWriter writer, PageAllocator pageAllocator)
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

        var buffer = new byte[Constants.LongValue.HeaderSize + data.Length];
        AccessBase.WriteUInt24(buffer, 0, data.Length);
        buffer[3] = Constants.LongValue.InlineStorageMode;
        Buffer.BlockCopy(data, 0, buffer, Constants.LongValue.HeaderSize, data.Length);
        return buffer;
    }

    private static void WriteLvalPageHeader(byte[] page, uint lvalToken, int rowStart)
    {
        page[4] = (byte)'L';
        page[5] = (byte)'V';
        page[6] = (byte)'A';
        page[7] = (byte)'L';
        AccessBase.Wi32(page, 8, unchecked((int)lvalToken));
        AccessBase.Wu16(page, 12, 1);
        AccessBase.Wu16(page, 14, rowStart);
        AccessBase.Wu16(page, 2, rowStart - 16);
    }

    private static uint ComputeLvalToken(byte[] data)
    {
        unchecked
        {
            uint hash = 2166136261;
            for (int i = 0; i < data.Length; i++)
            {
                hash ^= data[i];
                hash *= 16777619;
            }

            hash ^= (uint)data.Length;
            hash *= 16777619;
            return hash == 0 ? 1u : hash;
        }
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
    internal async ValueTask<object[]> PreEncodeLongValuesAsync(long ownerTdefPage, TableDef tableDef, object[] values, CancellationToken cancellationToken)
    {
        _ = ownerTdefPage;
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

                if (col.IsCalculated)
                {
                    data = CalculatedColumnUtil.Wrap(data);
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
                    ? AccessBase.EncodeJet4Text(text, col.IsCompressedUnicode)
                    : writer.AnsiEncoding.GetBytes(text);
                if (col.IsCalculated)
                {
                    data = CalculatedColumnUtil.Wrap(data);
                }

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

    internal async ValueTask<PreEncodedLongValue?> ForceEncodeMemoAsLvalAsync(string? text, bool compress, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        byte[] data = writer._format != DatabaseFormat.Jet3Mdb
            ? AccessBase.EncodeJet4Text(text, compress)
            : writer.AnsiEncoding.GetBytes(text);
        byte[] header = await EncodeAsLvalChainAsync(data, cancellationToken, lvalTokenOverride: 0, packRowsAtEnd: true).ConfigureAwait(false);
        return new PreEncodedLongValue(header);
    }

    /// <summary>
    /// Allocates one (single-page LVAL, bitmask <c>0x40</c>) or many (chained
    /// LVAL pages, bitmask <c>0x00</c>) LVAL data pages for a payload that is
    /// too large for the inline form, returning the resulting 12-byte LVAL
    /// header. Pages are appended in reverse so each predecessor row can hold
    /// its successor's <c>lval_dp</c> pointer.
    /// </summary>
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

        int pgSz = writer._pgSz;
        uint lvalToken = lvalTokenOverride ?? ComputeLvalToken(data);

        // One row per LVAL page. Access-authored Jet4/ACE LVAL pages use a
        // 20-byte LVAL header area; chained rows reserve their first four bytes
        // for the next-page pointer.
        int singleRowMax = pgSz - Constants.LongValue.LvalRowStart;
        int chainRowMax = singleRowMax - 4;

        var header = new byte[Constants.LongValue.HeaderSize];
        AccessBase.WriteUInt24(header, 0, data.Length);
        AccessBase.Wi32(header, 8, unchecked((int)lvalToken));

        if (data.Length <= singleRowMax)
        {
            byte[] page = BuildSingleLvalPageBuffer(data, lvalToken, packRowsAtEnd);
            try
            {
                long pageNumber = await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
                header[3] = Constants.LongValue.SinglePageStorageMode;
                uint lvalDp = unchecked((uint)((pageNumber << 8) | 0));
                AccessBase.Wi32(header, 4, (int)lvalDp);
                return header;
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
            byte[] page = BuildChainLvalPageBuffer(data, chunkStart, chunkLen, nextDp, lvalToken, packRowsAtEnd);
            try
            {
                long pageNumber = await pageAllocator.AllocatePageAsync(page, cancellationToken).ConfigureAwait(false);
                nextDp = unchecked((uint)((pageNumber << 8) | 0));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(page);
            }
        }

        header[3] = Constants.LongValue.ChainedStorageMode;
        AccessBase.Wi32(header, 4, (int)nextDp);
        return header;
    }

    /// <summary>
    /// Builds a single-row LVAL data page (bitmask <c>0x40</c> form): the row
    /// body is the entire payload with no next-pointer prefix.
    /// </summary>
    private byte[] BuildSingleLvalPageBuffer(byte[] payload, uint lvalToken, bool packRowsAtEnd)
    {
        int pgSz = writer._pgSz;

        byte[] page = ArrayPool<byte>.Shared.Rent(pgSz);
        Array.Clear(page, 0, pgSz);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = packRowsAtEnd ? pgSz - payload.Length : Constants.LongValue.LvalRowStart;
        WriteLvalPageHeader(page, lvalToken, rowStart);
        Buffer.BlockCopy(payload, 0, page, rowStart, payload.Length);
        return page;
    }

    /// <summary>
    /// Builds a single-row LVAL data page in chained form (bitmask <c>0x00</c>):
    /// the first 4 bytes of the row are the next-row pointer (<c>page&lt;&lt;8 | row</c>,
    /// little-endian; <c>0</c> on the terminal page) and the remainder is the chunk payload.
    /// </summary>
    private byte[] BuildChainLvalPageBuffer(byte[] data, int offset, int length, uint nextDp, uint lvalToken, bool packRowsAtEnd)
    {
        int pgSz = writer._pgSz;

        byte[] page = ArrayPool<byte>.Shared.Rent(pgSz);
        Array.Clear(page, 0, pgSz);
        page[0] = Constants.PageTypes.Data;
        page[1] = 0x01;
        int rowStart = packRowsAtEnd ? pgSz - (length + 4) : Constants.LongValue.LvalRowStart;
        WriteLvalPageHeader(page, lvalToken, rowStart);
        AccessBase.Wi32(page, rowStart, (int)nextDp);
        Buffer.BlockCopy(data, offset, page, rowStart + 4, length);
        return page;
    }
}
