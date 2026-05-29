namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;

/// <summary>
/// Reads LVAL (Long Value) pages from a JET database, resolving MEMO and
/// OLE field chains. Extracted from <see cref="AccessReader"/>.
/// </summary>
/// <param name="reader">The reader.</param>
internal sealed class LongValueDecoder(AccessReader reader)
{
    internal ValueTask<LongValueStore.LvalRowLocation> LocateLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        if (TryLocateLvalRowSync(lvalDp, out LongValueStore.LvalRowLocation cached))
        {
            return new ValueTask<LongValueStore.LvalRowLocation>(cached);
        }

        return LocateLvalRowSlowAsync(lvalDp, cancellationToken);
    }

    private async ValueTask<LongValueStore.LvalRowLocation> LocateLvalRowSlowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        int lvalPage = LongValueStore.PageNumber(lvalDp);
        if (lvalPage <= 0)
        {
            return new LongValueStore.LvalRowLocation([], 0, 0, $"invalid page {lvalPage}");
        }

        byte[] page = await reader.ReadPageCachedAsync(lvalPage, cancellationToken).ConfigureAwait(false);
        return ParseLvalRowLocation(lvalDp, page);
    }

    private bool TryLocateLvalRowSync(uint lvalDp, out LongValueStore.LvalRowLocation location)
    {
        int lvalPage = LongValueStore.PageNumber(lvalDp);
        if (lvalPage <= 0)
        {
            location = new LongValueStore.LvalRowLocation([], 0, 0, $"invalid page {lvalPage}");
            return true;
        }

        if (!reader.TryGetCachedPage(lvalPage, out byte[] page))
        {
            location = default;
            return false;
        }

        location = ParseLvalRowLocation(lvalDp, page);
        return true;
    }

    private LongValueStore.LvalRowLocation ParseLvalRowLocation(uint lvalDp, byte[] page)
        => LongValueStore.LocateRow(lvalDp, page, reader.dataPage, reader.pgSz, reader.GetLiveRowBoundsCached(LongValueStore.PageNumber(lvalDp), page));

    internal async ValueTask<LvalChainResult> ReadLvalChainAsync(uint firstLvalDp, int maxLen, CancellationToken cancellationToken)
        => await LongValueStore.ReadChainedPayloadAsync(firstLvalDp, maxLen, reader.pgSz, LocateLvalRowAsync, cancellationToken).ConfigureAwait(false);

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
                return inlineLen <= 0 ? string.Empty : DecodeLongValue(row, memoStart, inlineLen, isOle);

            case Constants.LongValue.SinglePageStorageMode:
                LongValueStore.LvalRowLocation memoLoc = await LocateLvalRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
                int memoSize = Math.Min(memoLoc.Size, descriptor.Length);
                if (!memoLoc.Failed && memoSize > 0)
                {
                    return DecodeLongValue(memoLoc.Page, memoLoc.Start, memoSize, isOle);
                }

                return isOle ? "(OLE)" : "(memo on LVAL page)";

            default:
                LvalChainResult chain = await ReadLvalChainAsync(descriptor.FirstDp, descriptor.Length, cancellationToken).ConfigureAwait(false);
                if (chain.Data != null)
                {
                    return DecodeLongValue(chain.Data, 0, chain.Data.Length, isOle);
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

                return row.AsSpan(memoStart, inlineLen).ToArray();

            case Constants.LongValue.SinglePageStorageMode:
                LongValueStore.LvalRowLocation memoLoc = await LocateLvalRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
                int memoSize = Math.Min(memoLoc.Size, descriptor.Length);
                if (memoLoc.Failed || memoSize <= 0)
                {
                    return [];
                }

                return memoLoc.Page.AsSpan(memoLoc.Start, memoSize).ToArray();

            default:
                LvalChainResult chain = await ReadLvalChainAsync(descriptor.FirstDp, descriptor.Length, cancellationToken).ConfigureAwait(false);
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
                return inlineLen <= 0 ? [] : AccessReader.DecodeOleValueBytes(row, memoStart, inlineLen);

            case Constants.LongValue.SinglePageStorageMode:
                LongValueStore.LvalRowLocation oleLoc = await LocateLvalRowAsync(descriptor.FirstDp, cancellationToken).ConfigureAwait(false);
                int oleSize = Math.Min(oleLoc.Size, descriptor.Length);
                return !oleLoc.Failed && oleSize > 0
                    ? AccessReader.DecodeOleValueBytes(oleLoc.Page, oleLoc.Start, oleSize)
                    : [];

            default:
                LvalChainResult chain = await ReadLvalChainAsync(descriptor.FirstDp, descriptor.Length, cancellationToken).ConfigureAwait(false);
                return chain.Data != null
                    ? AccessReader.DecodeOleValueBytes(chain.Data, 0, chain.Data.Length, allowInputReuse: true)
                    : [];
        }
    }

    internal string DecodeLongValue(byte[] buffer, int offset, int length, bool isOle)
    {
        if (isOle)
        {
            return AccessReader.TryDecodeOleObject(buffer, offset, length)
                ?? "data:application/octet-stream;base64," + Convert.ToBase64String(buffer, offset, length);
        }

        return reader.DecodeTextForFormat(buffer, offset, length);
    }
}
