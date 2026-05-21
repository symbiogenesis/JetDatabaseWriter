namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.AccessBase;

/// <summary>
/// Reads LVAL (Long Value) pages from a JET database, resolving MEMO and
/// OLE field chains. Extracted from <see cref="AccessReader"/>.
/// </summary>
internal sealed class LongValueDecoder(AccessReader reader)
{
    internal ValueTask<LvalRowLocation> LocateLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        if (TryLocateLvalRowSync(lvalDp, out LvalRowLocation cached))
        {
            return new ValueTask<LvalRowLocation>(cached);
        }

        return LocateLvalRowSlowAsync(lvalDp, cancellationToken);
    }

    private async ValueTask<LvalRowLocation> LocateLvalRowSlowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        int lvalPage = (int)(lvalDp >> 8);
        int lvalRow = (int)(lvalDp & 0xFF);
        if (lvalPage <= 0)
        {
            return new([], 0, 0, $"invalid page {lvalPage}");
        }

        byte[] page = await reader.ReadPageCachedAsync(lvalPage, cancellationToken).ConfigureAwait(false);
        return ParseLvalRowLocation(page, lvalPage, lvalRow);
    }

    private bool TryLocateLvalRowSync(uint lvalDp, out LvalRowLocation location)
    {
        int lvalPage = (int)(lvalDp >> 8);
        if (lvalPage <= 0)
        {
            location = new([], 0, 0, $"invalid page {lvalPage}");
            return true;
        }

        if (!reader.TryGetCachedPage(lvalPage, out byte[] page))
        {
            location = default;
            return false;
        }

        int lvalRow = (int)(lvalDp & 0xFF);
        location = ParseLvalRowLocation(page, lvalPage, lvalRow);
        return true;
    }

    private LvalRowLocation ParseLvalRowLocation(byte[] page, int lvalPage, int lvalRow)
    {
        if (page[0] != 0x01)
        {
            return new(page, 0, 0, $"page {lvalPage} not data page");
        }

        int numRows = Ru16(page, reader._dataPage.NumRows);
        if (lvalRow >= numRows)
        {
            return new(page, 0, 0, $"row {lvalRow} >= numRows {numRows}");
        }

        foreach (AccessBase.RowBound rowBound in reader.GetLiveRowBoundsCached(lvalPage, page))
        {
            if (rowBound.RowIndex != lvalRow)
            {
                continue;
            }

            if (rowBound.RowStart == 0 || rowBound.RowStart >= reader._pgSz)
            {
                return new(page, 0, 0, $"invalid rowStart {rowBound.RowStart}");
            }

            return new(page, rowBound.RowStart, rowBound.RowSize, null);
        }

        return new(page, 0, 0, "deleted/overflow row");
    }

    internal async ValueTask<LvalChainResult> ReadLvalChainAsync(uint firstLvalDp, int maxLen, CancellationToken cancellationToken)
    {
        if (maxLen <= 0)
        {
            return LvalChainResult.Failure("no chunks read");
        }

        byte[]? buffer = null;
        int totalLen = 0;
        uint currentDp = firstLvalDp;
        SmallLvalDpSet seen = default;

        try
        {
            while (currentDp != 0 && totalLen < maxLen && seen.Add(currentDp))
            {
                cancellationToken.ThrowIfCancellationRequested();

                LvalRowLocation loc = await LocateLvalRowAsync(currentDp, cancellationToken).ConfigureAwait(false);
                if (loc.Failed)
                {
                    return LvalChainResult.Failure(loc.Error!);
                }

                if (loc.Size < 4)
                {
                    return LvalChainResult.Failure($"rowSize {loc.Size} < 4");
                }

                currentDp = Ru32(loc.Page, loc.Start);
                int availableData = loc.Size - 4;
                int wantData = Math.Min(availableData, maxLen - totalLen);

                if (wantData > 0 && loc.Start + 4 + wantData <= reader._pgSz)
                {
                    buffer ??= new byte[maxLen];
                    Buffer.BlockCopy(loc.Page, loc.Start + 4, buffer, totalLen, wantData);
                    totalLen += wantData;
                }
            }

            if (totalLen == 0)
            {
                return LvalChainResult.Failure("no chunks read");
            }

            if (totalLen == buffer!.Length)
            {
                return LvalChainResult.Success(buffer);
            }

            var result = new byte[totalLen];
            Buffer.BlockCopy(buffer, 0, result, 0, totalLen);
            return LvalChainResult.Success(result);
        }
        catch (IOException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
        catch (OverflowException ex)
        {
            return LvalChainResult.Failure(ex.Message);
        }
    }

    internal async ValueTask<string> ReadLongValueAsync(byte[] row, int start, int len, bool isOle, CancellationToken cancellationToken)
    {
        if (len < 12)
        {
            return isOle ? "(OLE)" : "(memo)";
        }

        byte bitmask = row[start + 3];
        int memoLen = JetTypeInfo.ReadUInt24LittleEndian(row.AsSpan(start, 3));

        switch (bitmask & 0xC0)
        {
            case 0x80:
                int memoStart = start + 12;
                int inlineLen = Math.Min(memoLen, row.Length - memoStart);
                return inlineLen <= 0 ? string.Empty : DecodeLongValue(row, memoStart, inlineLen, isOle);

            case 0x40:
                LvalRowLocation memoLoc = await LocateLvalRowAsync(Ru32(row, start + 4), cancellationToken).ConfigureAwait(false);
                int memoSize = Math.Min(memoLoc.Size, memoLen);
                return !memoLoc.Failed && memoSize > 0
                    ? DecodeLongValue(memoLoc.Page, memoLoc.Start, memoSize, isOle)
                    : (isOle ? "(OLE)" : "(memo on LVAL page)");

            default:
                LvalChainResult chain = await ReadLvalChainAsync(Ru32(row, start + 4), memoLen, cancellationToken).ConfigureAwait(false);
                return chain.Data != null
                    ? DecodeLongValue(chain.Data, 0, chain.Data.Length, isOle)
                    : (isOle ? $"(OLE chain error: {chain.Error})" : $"(memo chain error: {chain.Error})");
        }
    }

    internal async ValueTask<byte[]> ReadLongValueRawBytesAsync(byte[] row, int start, int len, CancellationToken cancellationToken)
    {
        if (len < 12)
        {
            return [];
        }

        byte bitmask = row[start + 3];
        int memoLen = JetTypeInfo.ReadUInt24LittleEndian(row.AsSpan(start, 3));

        switch (bitmask & 0xC0)
        {
            case 0x80:
                int memoStart = start + 12;
                int inlineLen = Math.Min(memoLen, row.Length - memoStart);
                if (inlineLen <= 0)
                {
                    return [];
                }

                return row.AsSpan(memoStart, inlineLen).ToArray();

            case 0x40:
                LvalRowLocation memoLoc = await LocateLvalRowAsync(Ru32(row, start + 4), cancellationToken).ConfigureAwait(false);
                int memoSize = Math.Min(memoLoc.Size, memoLen);
                if (memoLoc.Failed || memoSize <= 0)
                {
                    return [];
                }

                return memoLoc.Page.AsSpan(memoLoc.Start, memoSize).ToArray();

            default:
                LvalChainResult chain = await ReadLvalChainAsync(Ru32(row, start + 4), memoLen, cancellationToken).ConfigureAwait(false);
                return chain.Data ?? [];
        }
    }

    internal async ValueTask<byte[]> ReadOleValueBytesAsync(byte[] row, int start, int len, CancellationToken cancellationToken)
    {
        if (len < 12)
        {
            return [];
        }

        byte bitmask = row[start + 3];
        int memoLen = JetTypeInfo.ReadUInt24LittleEndian(row.AsSpan(start, 3));

        switch (bitmask & 0xC0)
        {
            case 0x80:
                int memoStart = start + 12;
                int inlineLen = Math.Min(memoLen, row.Length - memoStart);
                return inlineLen <= 0 ? [] : AccessReader.DecodeOleValueBytes(row, memoStart, inlineLen);

            case 0x40:
                LvalRowLocation oleLoc = await LocateLvalRowAsync(Ru32(row, start + 4), cancellationToken).ConfigureAwait(false);
                int oleSize = Math.Min(oleLoc.Size, memoLen);
                return !oleLoc.Failed && oleSize > 0
                    ? AccessReader.DecodeOleValueBytes(oleLoc.Page, oleLoc.Start, oleSize)
                    : [];

            default:
                LvalChainResult chain = await ReadLvalChainAsync(Ru32(row, start + 4), memoLen, cancellationToken).ConfigureAwait(false);
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

        return reader._format != Enums.DatabaseFormat.Jet3Mdb
            ? DecodeJet4Text(buffer, offset, length)
            : reader.AnsiEncoding.GetString(buffer, offset, length);
    }

    /// <summary>
    /// Result of locating a single LVAL row within its data page.
    /// </summary>
    internal readonly record struct LvalRowLocation(byte[] Page, int Start, int Size, string? Error)
    {
        public bool Failed => Error is not null;
    }

    private struct SmallLvalDpSet
    {
        private const int InlineCapacity = 8;

        private uint value0;
        private uint value1;
        private uint value2;
        private uint value3;
        private uint value4;
        private uint value5;
        private uint value6;
        private uint value7;
        private int count;
        private HashSet<uint>? overflow;

        public bool Add(uint value)
        {
            if (overflow is not null)
            {
                return overflow.Add(value);
            }

            for (int index = 0; index < count; index++)
            {
                if (GetValue(index) == value)
                {
                    return false;
                }
            }

            if (count < InlineCapacity)
            {
                SetValue(count, value);
                count++;
                return true;
            }

            overflow = new HashSet<uint>(InlineCapacity + 1)
            {
                value0,
                value1,
                value2,
                value3,
                value4,
                value5,
                value6,
                value7,
            };

            return overflow.Add(value);
        }

        private readonly uint GetValue(int index) => index switch
        {
            0 => value0,
            1 => value1,
            2 => value2,
            3 => value3,
            4 => value4,
            5 => value5,
            6 => value6,
            7 => value7,
            _ => throw new ArgumentOutOfRangeException(nameof(index)),
        };

        private void SetValue(int index, uint value)
        {
            switch (index)
            {
                case 0:
                    value0 = value;
                    break;

                case 1:
                    value1 = value;
                    break;

                case 2:
                    value2 = value;
                    break;

                case 3:
                    value3 = value;
                    break;

                case 4:
                    value4 = value;
                    break;

                case 5:
                    value5 = value;
                    break;

                case 6:
                    value6 = value;
                    break;

                case 7:
                    value7 = value;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(index));
            }
        }
    }
}
