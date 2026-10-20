namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
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
/// <param name="maxValueBytes">The maximum stored size of one value.</param>
internal sealed class LongValueDecoder(JetFormat format, ReaderPageCache pages, int maxValueBytes = 0xFFFFFF)
{
    internal void ValidateStoredLength(int length)
    {
        if (length > maxValueBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, $"The long value declares {length} stored byte(s), exceeding the configured limit {maxValueBytes}.");
        }
    }

    internal ValueTask<LvalRowLocation> LocateLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        int lvalPage = LongValueStore.PageNumber(lvalDp);
        if (lvalPage <= 0 || lvalPage >= pages.PageCount)
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
        try
        {
            return this.LocateLvalRow(lvalPage, lvalRow, page);
        }
        catch
        {
            pages.ReturnUncachedPage(page);
            throw;
        }
    }

    private LvalRowLocation LocateLvalRow(int lvalPage, int lvalRow, byte[] page)
    {
        if (page.Length < format.PageSize)
        {
            return new LvalRowLocation(page, 0, 0, "truncated LVAL page");
        }

        if (page[0] != Constants.PageTypes.Data)
        {
            return new LvalRowLocation(page, 0, 0, $"page {lvalPage} not data page");
        }

        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows));
        if (numRows > (format.PageSize - format.DataPage.RowsStart) / sizeof(ushort))
        {
            return new LvalRowLocation(page, 0, 0, "the LVAL row count exceeds its page directory capacity");
        }

        RowBound[] rowDirectory = pages.GetRowDirectory(lvalPage, page);
        LvalRowLocation location = LongValueStore.LocateRow(lvalPage, lvalRow, page, format.DataPage, format.PageSize, rowDirectory);
        if (!location.Failed && location.Start < format.DataPage.RowsStart + (numRows * sizeof(ushort)))
        {
            return new LvalRowLocation(page, 0, 0, "the LVAL row overlaps its page header or row directory");
        }

        return location;
    }

    internal async ValueTask<string> ReadLongValueAsync(byte[] row, int start, int len, bool isOle, CancellationToken cancellationToken)
    {
        byte[] bytes = await this.ReadLongValueBytesExactAsync(row, start, len, cancellationToken).ConfigureAwait(false);
        LongValueDescriptor.TryRead(row.AsSpan(start, len), out LongValueDescriptor descriptor);
        return this.DecodeLongValue(bytes, 0, bytes.Length, isOle, compressedStorage: descriptor.IsInline || descriptor.IsSinglePage);
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
        LongValueDescriptor descriptor = this.ReadDescriptor(row, start, len, int.MaxValue);
        if (descriptor.Length == 0)
        {
            return [];
        }

        if (descriptor.IsInline)
        {
            return BinaryBuffer.CopySlice(row, start + Constants.LongValue.HeaderSize, descriptor.Length);
        }

        // Grow only from validated chunks: a corrupt descriptor must not
        // reserve its claimed payload before its first row is readable.
        var output = new MemoryStream();
        try
        {
            await this.CopyPayloadAsync(row, start, len, descriptor, output, descriptor.Length, cancellationToken).ConfigureAwait(false);
            return output.ToArray();
        }
        finally
        {
            await output.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Streams the stored payload into a staging stream without decoding it.
    /// The destination stays open and can contain a partial value on failure.
    /// Descriptor bytes, padding and ownership remain separate preservation obligations.
    /// </summary>
    /// <param name="row">The buffer holding the column.</param>
    /// <param name="start">The descriptor offset.</param>
    /// <param name="len">The column slice length.</param>
    /// <param name="destination">The staging destination.</param>
    /// <param name="maxChainRows">The maximum external rows and cycle-tracking entries.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completed after copying the declared stored bytes.</returns>
    internal async ValueTask CopyLongValueRawBytesAsync(byte[] row, int start, int len, Stream destination, int maxChainRows, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        LongValueDescriptor descriptor = this.ReadDescriptor(row, start, len, maxChainRows);
        await this.CopyPayloadAsync(row, start, len, descriptor, destination, maxChainRows, cancellationToken).ConfigureAwait(false);
    }

    private ValueTask CopyPayloadAsync(byte[] row, int start, int len, LongValueDescriptor descriptor, Stream destination, int maxChainRows, CancellationToken cancellationToken)
        => LongValueStore.CopyPayloadAsync(
            descriptor,
            row.AsMemory(start + Constants.LongValue.HeaderSize, len - Constants.LongValue.HeaderSize),
            destination,
            format.PageSize,
            maxValueBytes,
            maxChainRows,
            this.LocateLvalRowAsync,
            cancellationToken,
            this.ReleaseLvalPage);

    private void ReleaseLvalPage(byte[] page) => pages.ReturnUncachedPage(page);

    private LongValueDescriptor ReadDescriptor(byte[] row, int start, int len, int maxChainRows)
    {
        if (start < 0 || len < 0 || start > row.Length - len)
        {
            throw new InvalidDataException("the long-value column slice is outside the row buffer");
        }

        if (!LongValueDescriptor.TryRead(row.AsSpan(start, len), out LongValueDescriptor descriptor))
        {
            throw new InvalidDataException($"the long-value descriptor is {len} byte(s), shorter than the {Constants.LongValue.HeaderSize}-byte header");
        }

        LongValueStore.ValidatePayload(descriptor, len - Constants.LongValue.HeaderSize, format.PageSize, maxValueBytes, maxChainRows);
        return descriptor;
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
    /// <param name="compressedStorage">Whether an ordinary inline or single-row Memo may contain the compressed marker.</param>
    internal string DecodeLongValue(byte[] buffer, int offset, int length, bool isOle, bool compressedStorage = true)
        => isOle
            ? OleObjectDecoder.ToDataUri(buffer, offset, length)
            : format.DecodeMemoText(buffer, offset, length, compressedStorage);
}
