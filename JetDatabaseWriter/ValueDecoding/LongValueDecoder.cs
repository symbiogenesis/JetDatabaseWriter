namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Buffers;
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
    internal async ValueTask<LvalRowLocation> LocateLvalRowAsync(uint lvalDp, CancellationToken cancellationToken)
    {
        if (TryLocateLvalRowSync(lvalDp, out LvalRowLocation cached))
        {
            return cached;
        }

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

        int rawOff = Ru16(page, reader._dataPage.RowsStart + (lvalRow * 2));
        if ((rawOff & 0xC000) != 0)
        {
            return new(page, 0, 0, "deleted/overflow row");
        }

        int rowStart = rawOff & 0x1FFF;
        if (rowStart == 0 || rowStart >= reader._pgSz)
        {
            return new(page, 0, 0, $"invalid rowStart {rowStart}");
        }

        int rowEnd = reader._pgSz - 1;
        for (int r = 0; r < numRows; r++)
        {
            int ofs = Ru16(page, reader._dataPage.RowsStart + (r * 2)) & 0x1FFF;
            if (ofs > rowStart && ofs < rowEnd)
            {
                rowEnd = ofs - 1;
            }
        }

        return new(page, rowStart, rowEnd - rowStart + 1, null);
    }

    internal async ValueTask<LvalChainResult> ReadLvalChainAsync(uint firstLvalDp, int maxLen, CancellationToken cancellationToken)
    {
        if (maxLen <= 0)
        {
            return LvalChainResult.Failure("no chunks read");
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(maxLen);
        int totalLen = 0;
        uint currentDp = firstLvalDp;
        var seen = new HashSet<uint>();

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
                    Buffer.BlockCopy(loc.Page, loc.Start + 4, buffer, totalLen, wantData);
                    totalLen += wantData;
                }
            }

            if (totalLen == 0)
            {
                return LvalChainResult.Failure("no chunks read");
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
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
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
                    ? AccessReader.DecodeOleValueBytes(chain.Data, 0, chain.Data.Length)
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
}
