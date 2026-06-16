namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// Reads LVAL (Long Value) pages from a JET database, resolving MEMO and
/// OLE field chains. Extracted from <see cref="AccessReader"/>.
/// </summary>
/// <param name="reader">The reader.</param>
internal sealed class LongValueDecoder(AccessReader reader)
{
    internal ValueTask<LvalRowLocation> LocateLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        int lvalPage = LongValueStore.PageNumber(lvalDp);
        if (lvalPage <= 0)
        {
            return new ValueTask<LvalRowLocation>(new LvalRowLocation([], 0, 0, $"invalid page {lvalPage}"));
        }

        if (reader.TryGetCachedPage(lvalPage, out byte[] page))
        {
            return new ValueTask<LvalRowLocation>(this.LocateLvalRow(lvalPage, LongValueStore.RowIndex(lvalDp), page));
        }

        return this.LocateLvalRowSlowAsync(lvalPage, LongValueStore.RowIndex(lvalDp), cancellationToken);
    }

    private async ValueTask<LvalRowLocation> LocateLvalRowSlowAsync(int lvalPage, int lvalRow, CancellationToken cancellationToken)
    {
        byte[] page = await reader.ReadPageCachedAsync(lvalPage, cancellationToken).ConfigureAwait(false);
        return this.LocateLvalRow(lvalPage, lvalRow, page);
    }

    private LvalRowLocation LocateLvalRow(int lvalPage, int lvalRow, byte[] page)
    {
        RowBound[] liveRows = reader.GetLiveRowBoundsCached(lvalPage, page);
        return LongValueStore.LocateRow(lvalPage, lvalRow, page, reader.DataPage, reader.PageSizeBytes, liveRows);
    }

    internal async ValueTask<LvalChainResult> ReadLvalChainAsync(uint firstLvalDp, int maxLen, CancellationToken cancellationToken)
        => await LongValueStore.ReadChainedPayloadAsync(firstLvalDp, maxLen, reader.PageSizeBytes, this.LocateLvalRowAsync, cancellationToken).ConfigureAwait(false);

    internal async ValueTask<string> ReadLongValueAsync(byte[] row, int start, int len, bool isOle, CancellationToken cancellationToken)
    {
        if (!LongValueDescriptor.TryRead(row.AsSpan(start, len), out LongValueDescriptor descriptor))
        {
            return isOle ? "(OLE)" : "(memo)";
        }

        switch (descriptor.StorageMode)
        {
            case Constants.LongValue.InlineStorageMode:
                int memoStart = start + Constants.LongValue.HeaderSize;
                int inlineLen = Math.Min(descriptor.Length, row.Length - memoStart);
                return inlineLen <= 0 ? string.Empty : this.DecodeLongValue(row, memoStart, inlineLen, isOle);

            case Constants.LongValue.SinglePageStorageMode:
                LvalRowLocation memoLoc = await this.LocateLvalRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
                int memoSize = Math.Min(memoLoc.Size, descriptor.Length);
                if (!memoLoc.Failed && memoSize > 0)
                {
                    return this.DecodeLongValue(memoLoc.Page, memoLoc.Start, memoSize, isOle);
                }

                return isOle ? "(OLE)" : "(memo on LVAL page)";

            default:
                LvalChainResult chain = await this.ReadLvalChainAsync(descriptor.FirstDp, descriptor.Length, cancellationToken).ConfigureAwait(false);
                if (chain.Data != null)
                {
                    return this.DecodeLongValue(chain.Data, 0, chain.Data.Length, isOle);
                }

                return isOle ? $"(OLE chain error: {chain.Error})" : $"(memo chain error: {chain.Error})";
        }
    }

    internal async ValueTask<byte[]> ReadLongValueRawBytesAsync(byte[] row, int start, int len, CancellationToken cancellationToken)
    {
        if (!LongValueDescriptor.TryRead(row.AsSpan(start, len), out LongValueDescriptor descriptor))
        {
            return [];
        }

        switch (descriptor.StorageMode)
        {
            case Constants.LongValue.InlineStorageMode:
                int memoStart = start + Constants.LongValue.HeaderSize;
                int inlineLen = Math.Min(descriptor.Length, row.Length - memoStart);
                if (inlineLen <= 0)
                {
                    return [];
                }

                return BinaryBuffer.CopySlice(row, memoStart, inlineLen);

            case Constants.LongValue.SinglePageStorageMode:
                LvalRowLocation memoLoc = await this.LocateLvalRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
                int memoSize = Math.Min(memoLoc.Size, descriptor.Length);
                if (memoLoc.Failed || memoSize <= 0)
                {
                    return [];
                }

                return BinaryBuffer.CopySlice(memoLoc.Page, memoLoc.Start, memoSize);

            default:
                LvalChainResult chain = await this.ReadLvalChainAsync(descriptor.FirstDp, descriptor.Length, cancellationToken).ConfigureAwait(false);
                return chain.Data ?? [];
        }
    }

    internal async ValueTask<byte[]> ReadOleValueBytesAsync(byte[] row, int start, int len, CancellationToken cancellationToken)
    {
        if (!LongValueDescriptor.TryRead(row.AsSpan(start, len), out LongValueDescriptor descriptor))
        {
            return [];
        }

        switch (descriptor.StorageMode)
        {
            case Constants.LongValue.InlineStorageMode:
                int memoStart = start + Constants.LongValue.HeaderSize;
                int inlineLen = Math.Min(descriptor.Length, row.Length - memoStart);
                return inlineLen <= 0 ? [] : OleObjectDecoder.DecodeOleValueBytes(row, memoStart, inlineLen);

            case Constants.LongValue.SinglePageStorageMode:
                LvalRowLocation oleLoc = await this.LocateLvalRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
                int oleSize = Math.Min(oleLoc.Size, descriptor.Length);
                return !oleLoc.Failed && oleSize > 0
                    ? OleObjectDecoder.DecodeOleValueBytes(oleLoc.Page, oleLoc.Start, oleSize)
                    : [];

            default:
                LvalChainResult chain = await this.ReadLvalChainAsync(descriptor.FirstDp, descriptor.Length, cancellationToken).ConfigureAwait(false);
                return chain.Data != null
                    ? OleObjectDecoder.DecodeOleValueBytes(chain.Data, 0, chain.Data.Length, allowInputReuse: true)
                    : [];
        }
    }

    internal string DecodeLongValue(byte[] buffer, int offset, int length, bool isOle)
    {
        if (isOle)
        {
            return OleObjectDecoder.TryDecodeOleObject(buffer, offset, length)
                ?? ("data:application/octet-stream;base64," + Convert.ToBase64String(buffer, offset, length));
        }

        return reader.DecodeTextForFormat(buffer, offset, length);
    }
}
