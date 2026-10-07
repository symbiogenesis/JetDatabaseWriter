namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// Reads LVAL (Long Value) pages from a JET database, resolving MEMO and
/// OLE field chains.
/// </summary>
/// <param name="format">The file's format profile: the page size, the data-page layout and the text codec.</param>
/// <param name="pages">The reader's page cache, which LVAL pages are read through.</param>
internal sealed class LongValueDecoder(JetFormat format, ReaderPageCache pages)
{
    internal ValueTask<LvalRowLocation> LocateLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        int lvalPage = LongValueStore.PageNumber(lvalDp);
        if (lvalPage <= 0)
        {
            return new ValueTask<LvalRowLocation>(new LvalRowLocation([], 0, 0, $"invalid page {lvalPage}"));
        }

        if (pages.TryGetCachedPage(lvalPage, out byte[] page))
        {
            return new ValueTask<LvalRowLocation>(this.LocateLvalRow(lvalPage, LongValueStore.RowIndex(lvalDp), page));
        }

        return this.LocateLvalRowSlowAsync(lvalPage, LongValueStore.RowIndex(lvalDp), cancellationToken);
    }

    private async ValueTask<LvalRowLocation> LocateLvalRowSlowAsync(int lvalPage, int lvalRow, CancellationToken cancellationToken)
    {
        byte[] page = await pages.ReadPageAsync(lvalPage, cancellationToken).ConfigureAwait(false);
        return this.LocateLvalRow(lvalPage, lvalRow, page);
    }

    private LvalRowLocation LocateLvalRow(int lvalPage, int lvalRow, byte[] page)
    {
        RowBound[] rowDirectory = pages.GetRowDirectory(lvalPage, page);
        return LongValueStore.LocateRow(lvalPage, lvalRow, page, format.DataPage, format.PageSize, rowDirectory);
    }

    internal async ValueTask<LvalChainResult> ReadLvalChainAsync(uint firstLvalDp, int maxLen, CancellationToken cancellationToken)
        => await LongValueStore.ReadChainedPayloadAsync(firstLvalDp, maxLen, format.PageSize, this.LocateLvalRowAsync, cancellationToken).ConfigureAwait(false);

    internal async ValueTask<string> ReadLongValueAsync(byte[] row, int start, int len, bool isOle, CancellationToken cancellationToken)
    {
        byte[] bytes = await this.ReadLongValueBytesExactAsync(row, start, len, cancellationToken).ConfigureAwait(false);
        return this.DecodeLongValue(bytes, 0, bytes.Length, isOle);
    }

    internal ValueTask<byte[]> ReadLongValueRawBytesAsync(byte[] row, int start, int len, CancellationToken cancellationToken)
        => this.ReadLongValueBytesExactAsync(row, start, len, cancellationToken);

    /// <summary>
    /// Reads the stored bytes of a MEMO / OLE value from its inline payload,
    /// single LVAL row or LVAL chain. A value that cannot be resolved throws
    /// instead of returning an empty
    /// array, so callers that write the value back never persist a loss.
    /// </summary>
    /// <param name="row">The page holding the row.</param>
    /// <param name="start">The offset of the 12-byte long-value descriptor.</param>
    /// <param name="len">The length of the column slice.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">The descriptor is truncated or its LVAL data cannot be read.</exception>
    internal async ValueTask<byte[]> ReadLongValueBytesExactAsync(byte[] row, int start, int len, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (start < 0 || len < 0 || start > row.Length - len)
        {
            throw new InvalidDataException("the long-value column slice is outside the row buffer");
        }

        if (!LongValueDescriptor.TryRead(row.AsSpan(start, len), out LongValueDescriptor descriptor))
        {
            throw new InvalidDataException($"the long-value descriptor is {len} byte(s), shorter than the {Constants.LongValue.HeaderSize}-byte header");
        }

        if (descriptor.Length <= 0)
        {
            return [];
        }

        switch (descriptor.StorageMode)
        {
            case Constants.LongValue.InlineStorageMode:
                int valueStart = start + Constants.LongValue.HeaderSize;
                int inlineLen = descriptor.Length;
                if (inlineLen > len - Constants.LongValue.HeaderSize)
                {
                    throw new InvalidDataException($"the inline value claims {descriptor.Length} byte(s) but the column slice contains fewer bytes");
                }

                return BinaryBuffer.CopySlice(row, valueStart, inlineLen);

            case Constants.LongValue.SinglePageStorageMode:
                LvalRowLocation location = await this.LocateLvalRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
                if (location.Failed)
                {
                    throw new InvalidDataException($"the LVAL row could not be located: {location.Error}");
                }

                int size = descriptor.Length;
                if (location.Size < size || location.Start < 0 || location.Start > location.Page.Length - size)
                {
                    throw new InvalidDataException($"the LVAL row is truncated but the descriptor claims {descriptor.Length} byte(s)");
                }

                return BinaryBuffer.CopySlice(location.Page, location.Start, size);

            default:
                LvalChainResult chain = await this.ReadLvalChainAsync(descriptor.FirstDp, descriptor.Length, cancellationToken).ConfigureAwait(false);
                return chain.Data ?? throw new InvalidDataException($"the LVAL chain could not be read: {chain.Error}");
        }
    }

    /// <summary>
    /// Decodes a MEMO value's stored bytes as text, or renders an OLE value's stored
    /// bytes as a <c>data:</c> URI (<see cref="OleObjectDecoder.ToDataUri"/>): the
    /// media type of a file signature at the first byte, else
    /// <c>application/octet-stream</c>, and every stored byte.
    /// </summary>
    /// <param name="buffer">The buffer holding the value.</param>
    /// <param name="offset">The value's offset.</param>
    /// <param name="length">The value's length.</param>
    /// <param name="isOle">Whether the value is an OLE value.</param>
    internal string DecodeLongValue(byte[] buffer, int offset, int length, bool isOle)
        => isOle
            ? OleObjectDecoder.ToDataUri(buffer, offset, length)
            : format.DecodeText(buffer, offset, length);
}
