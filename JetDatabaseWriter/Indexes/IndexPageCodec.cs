namespace JetDatabaseWriter.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Layout-aware read codec for JET index pages. Owns the common header,
/// bitmask, prefix-compression, and entry-decoding rules shared by leaf,
/// intermediate, seeker, and mutation code.
/// </summary>
internal static class IndexPageCodec
{
    private const int LeafTrailerSize = 4;
    private const int IntermediateTrailerSize = 8;

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="page"/> is an
    /// index leaf page (<c>page_type = 0x04</c>).
    /// </summary>
    /// <param name="page">The page bytes.</param>
    public static bool IsLeaf(byte[] page)
        => page?.Length > 0 && page[0] == Constants.IndexLeafPage.PageTypeLeaf;

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="page"/> is an
    /// index intermediate page (<c>page_type = 0x03</c>).
    /// </summary>
    /// <param name="page">The page bytes.</param>
    public static bool IsIntermediate(byte[] page)
        => page?.Length > 0 && page[0] == Constants.IndexLeafPage.PageTypeIntermediate;

    /// <summary>
    /// Returns the page number recorded in the <c>next_page</c> sibling field.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    public static long ReadNextPage(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < layout.NextPageOffset + 4)
        {
            return 0;
        }

        return (uint)Ri32(page, layout.NextPageOffset);
    }

    /// <summary>
    /// Returns the page number recorded in the <c>tail_page</c> header field.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    public static long ReadTailPage(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < layout.TailPageOffset + 4)
        {
            return 0;
        }

        return (uint)Ri32(page, layout.TailPageOffset);
    }

    /// <summary>
    /// Returns the page number recorded in the <c>prev_page</c> sibling field.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    public static long ReadPrevPage(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < layout.PrevPageOffset + 4)
        {
            return 0;
        }

        return (uint)Ri32(page, layout.PrevPageOffset);
    }

    /// <summary>
    /// Reads the three sibling pointer fields from an index page header.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    public static (long Prev, long Next, long Tail) ReadSiblingPointers(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page)
    {
        if (page == null || page.Length < layout.TailPageOffset + 4)
        {
            return (0, 0, 0);
        }

        long previousPage = (uint)Ri32(page, layout.PrevPageOffset);
        long nextPage = (uint)Ri32(page, layout.NextPageOffset);
        long tailPage = (uint)Ri32(page, layout.TailPageOffset);
        return (previousPage, nextPage, tailPage);
    }

    /// <summary>
    /// Returns <see langword="true"/> when a page is a leaf root with no
    /// sibling or tail pointers.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    public static bool IsSingleRootLeaf(IndexLeafPageBuilder.LeafPageLayout layout, byte[] page)
    {
        if (!IsLeaf(page) || page.Length < layout.TailPageOffset + 4)
        {
            return false;
        }

        (long previousPage, long nextPage, long tailPage) = ReadSiblingPointers(layout, page);
        return previousPage == 0 && nextPage == 0 && tailPage == 0;
    }

    /// <summary>
    /// Reads the first child pointer from an intermediate page, or zero when
    /// the page is malformed or not an intermediate page.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="intermediatePage">The intermediate page.</param>
    /// <param name="pageSize">The page size.</param>
    public static long ReadFirstChildPointer(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] intermediatePage,
        int pageSize)
    {
        if (!IsIntermediate(intermediatePage)
            || !TryGetPayloadEnd(layout, intermediatePage, pageSize, out int payloadEnd)
            || payloadEnd <= layout.FirstEntryOffset)
        {
            return 0;
        }

        int entryStart = layout.FirstEntryOffset;
        int nextEntryStart = NextEntryStart(layout, intermediatePage, payloadEnd, entryStart);
        int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
        int entryLength = entryEnd - entryStart;
        if (entryLength < IntermediateTrailerSize)
        {
            return 0;
        }

        return DecodeIntermediateChildPointer(intermediatePage, entryEnd - 4);
    }

    /// <summary>
    /// Reads a big-endian 4-byte child-page pointer from an intermediate entry.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="offset">The offset.</param>
    public static long DecodeIntermediateChildPointer(byte[] page, int offset)
    {
        if (page == null || offset < 0 || offset + 4 > page.Length)
        {
            return 0;
        }

        return BinaryPrimitives.ReadUInt32BigEndian(page.AsSpan(offset, 4));
    }

    /// <summary>
    /// Decodes leaf entries into canonical key plus data-row pointers.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    public static List<IndexEntry> DecodeLeafEntries(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize)
    {
        var result = new List<IndexEntry>();
        if (!TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || payloadEnd <= layout.FirstEntryOffset)
        {
            return result;
        }

        int prefixLength = Ru16(page, layout.PrefLenOffset);
        byte[]? sharedPrefix = null;
        int entryStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int suffixLength = entryEnd - entryStart - LeafTrailerSize;
            if (suffixLength < 0 || entryStart + suffixLength + LeafTrailerSize > page.Length)
            {
                break;
            }

            byte[] canonicalKey = DecodeCanonicalKey(page, entryStart, suffixLength, prefixLength, sharedPrefix, isFirstEntry);
            if (isFirstEntry && prefixLength > 0 && suffixLength >= prefixLength)
            {
                sharedPrefix = new byte[prefixLength];
                Buffer.BlockCopy(canonicalKey, 0, sharedPrefix, 0, prefixLength);
            }

            int trailerOffset = entryStart + suffixLength;
            long dataPage = ReadUInt24BigEndian(page.AsSpan(trailerOffset, 3));
            byte dataRow = page[trailerOffset + 3];
            result.Add(new IndexEntry(canonicalKey, dataPage, dataRow));

            isFirstEntry = false;
            if (nextEntryStart < 0)
            {
                break;
            }

            entryStart = nextEntryStart;
        }

        return result;
    }

    /// <summary>
    /// Decodes intermediate entries into canonical summary key, row pointer,
    /// and child-page pointer tuples.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    public static List<DecodedIntermediateEntry> DecodeIntermediateEntries(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize)
    {
        var result = new List<DecodedIntermediateEntry>();
        if (!IsIntermediate(page)
            || !TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || payloadEnd <= layout.FirstEntryOffset)
        {
            return result;
        }

        int prefixLength = Ru16(page, layout.PrefLenOffset);
        byte[]? sharedPrefix = null;
        int entryStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int suffixLength = entryEnd - entryStart - IntermediateTrailerSize;
            if (suffixLength < 0 || entryStart + suffixLength + IntermediateTrailerSize > page.Length)
            {
                break;
            }

            byte[] canonicalKey = DecodeCanonicalKey(page, entryStart, suffixLength, prefixLength, sharedPrefix, isFirstEntry);
            if (isFirstEntry && prefixLength > 0 && suffixLength >= prefixLength)
            {
                sharedPrefix = new byte[prefixLength];
                Buffer.BlockCopy(canonicalKey, 0, sharedPrefix, 0, prefixLength);
            }

            int trailerOffset = entryStart + suffixLength;
            long dataPage = ReadUInt24BigEndian(page.AsSpan(trailerOffset, 3));
            byte dataRow = page[trailerOffset + 3];
            long childPage = DecodeIntermediateChildPointer(page, trailerOffset + 4);
            result.Add(new DecodedIntermediateEntry(new IndexEntry(canonicalKey, dataPage, dataRow), childPage));

            isFirstEntry = false;
            if (nextEntryStart < 0)
            {
                break;
            }

            entryStart = nextEntryStart;
        }

        return result;
    }

    /// <summary>
    /// Returns the child page whose summary key may contain
    /// <paramref name="searchKey"/>, or <see langword="null"/> when every
    /// summary sorts before the search key.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="searchKey">The search key.</param>
    public static long? SelectChildPage(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize,
        byte[] searchKey)
    {
        if (!IsIntermediate(page)
            || !TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || payloadEnd <= layout.FirstEntryOffset)
        {
            return null;
        }

        int prefixLength = Ru16(page, layout.PrefLenOffset);
        int entryStart = layout.FirstEntryOffset;
        int prefixStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int suffixLength = entryEnd - entryStart - IntermediateTrailerSize;
            if (!IsEntryReadable(page, entryStart, suffixLength, IntermediateTrailerSize))
            {
                return null;
            }

            if (isFirstEntry && prefixLength > suffixLength)
            {
                return null;
            }

            int comparison = CompareSearchKeyToEntry(
                searchKey,
                page,
                prefixStart,
                entryStart,
                suffixLength,
                prefixLength,
                isFirstEntry);
            if (comparison <= 0)
            {
                return DecodeIntermediateChildPointer(page, entryStart + suffixLength + IntermediateTrailerSize - 4);
            }

            if (nextEntryStart < 0)
            {
                break;
            }

            isFirstEntry = false;
            entryStart = nextEntryStart;
        }

        return null;
    }

    /// <summary>
    /// Scans one leaf page for an exact key match without materializing the
    /// page's decoded entries.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="searchKey">The search key.</param>
    public static (bool Found, bool ContinueToNext) ContainsKeyInLeafPage(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize,
        byte[] searchKey)
    {
        if (!TryScanLeafPage(layout, page, pageSize, searchKey, matches: null, out int lastComparison))
        {
            return (false, false);
        }

        return lastComparison == 0
            ? (true, false)
            : (false, lastComparison > 0);
    }

    /// <summary>
    /// Appends exact-key row-location matches from one leaf page without
    /// materializing the page's decoded entries.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="searchKey">The search key.</param>
    /// <param name="matches">The matches.</param>
    public static bool CollectMatchingLeafEntries(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize,
        byte[] searchKey,
        List<(long DataPage, int RowIndex)> matches)
    {
        if (!TryScanLeafPage(layout, page, pageSize, searchKey, matches, out int lastComparison))
        {
            return false;
        }

        return lastComparison >= 0;
    }

    /// <summary>
    /// Returns the start offset of the next entry on a page, or <c>-1</c>
    /// when the bitmask has no later entry start before the payload end.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="payloadEnd">The payload end.</param>
    /// <param name="currentStart">The current start.</param>
    public static int NextEntryStart(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int payloadEnd,
        int currentStart)
    {
        if (page == null || currentStart < layout.FirstEntryOffset || payloadEnd <= layout.FirstEntryOffset)
        {
            return -1;
        }

        int searchStart = currentStart - layout.FirstEntryOffset + 1;
        int searchEnd = payloadEnd - layout.FirstEntryOffset;
        for (int bitIndex = searchStart; bitIndex < searchEnd; bitIndex++)
        {
            int byteOffset = layout.BitmaskOffset + (bitIndex / 8);
            if (byteOffset >= layout.FirstEntryOffset || byteOffset >= page.Length)
            {
                return -1;
            }

            if ((page[byteOffset] & (1 << (bitIndex % 8))) != 0)
            {
                int candidate = layout.FirstEntryOffset + bitIndex;
                return candidate < payloadEnd ? candidate : -1;
            }
        }

        return -1;
    }

    /// <summary>
    /// Lexicographically compares encoded index keys using unsigned byte order.
    /// </summary>
    /// <param name="left">The left value.</param>
    /// <param name="right">The right.</param>
    public static int CompareKeyBytes(byte[] left, byte[] right)
    {
        int length = Math.Min(left.Length, right.Length);
        for (int offset = 0; offset < length; offset++)
        {
            int difference = left[offset] - right[offset];
            if (difference != 0)
            {
                return difference;
            }
        }

        return left.Length - right.Length;
    }

    private static bool TryGetPayloadEnd(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize,
        out int payloadEnd)
    {
        payloadEnd = 0;
        if (page == null || page.Length < pageSize || pageSize <= layout.FirstEntryOffset)
        {
            return false;
        }

        int freeSpace = Ru16(page, 2);
        payloadEnd = pageSize - freeSpace;
        return payloadEnd >= layout.FirstEntryOffset && payloadEnd <= pageSize;
    }

    private static bool TryScanLeafPage(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize,
        byte[] searchKey,
        List<(long DataPage, int RowIndex)>? matches,
        out int lastComparison)
    {
        lastComparison = -1;
        if (!IsLeaf(page)
            || !TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || payloadEnd <= layout.FirstEntryOffset)
        {
            return false;
        }

        int prefixLength = Ru16(page, layout.PrefLenOffset);
        int entryStart = layout.FirstEntryOffset;
        int prefixStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        bool hasEntries = false;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int suffixLength = entryEnd - entryStart - LeafTrailerSize;
            if (!IsEntryReadable(page, entryStart, suffixLength, LeafTrailerSize))
            {
                return false;
            }

            if (isFirstEntry && prefixLength > suffixLength)
            {
                return false;
            }

            int comparison = CompareSearchKeyToEntry(
                searchKey,
                page,
                prefixStart,
                entryStart,
                suffixLength,
                prefixLength,
                isFirstEntry);
            if (comparison == 0)
            {
                if (matches == null)
                {
                    lastComparison = 0;
                    return true;
                }

                int pointerOffset = entryStart + suffixLength;
                long dataPage = ReadUInt24BigEndian(page.AsSpan(pointerOffset, 3));
                matches.Add((dataPage, page[pointerOffset + 3]));
            }

            hasEntries = true;
            lastComparison = comparison;
            if (nextEntryStart < 0)
            {
                break;
            }

            isFirstEntry = false;
            entryStart = nextEntryStart;
        }

        return hasEntries;
    }

    private static bool IsEntryReadable(byte[] page, int entryStart, int suffixLength, int trailerLength)
        => suffixLength >= 0 && entryStart + suffixLength + trailerLength <= page.Length;

    private static int CompareSearchKeyToEntry(
        byte[] searchKey,
        byte[] page,
        int prefixStart,
        int entryStart,
        int suffixLength,
        int prefixLength,
        bool isFirstEntry)
    {
        int canonicalLength = isFirstEntry || prefixLength == 0 ? suffixLength : prefixLength + suffixLength;
        int length = Math.Min(searchKey.Length, canonicalLength);
        for (int offset = 0; offset < length; offset++)
        {
            byte entryByte;
            if (isFirstEntry || prefixLength == 0)
            {
                entryByte = page[entryStart + offset];
            }
            else if (offset < prefixLength)
            {
                entryByte = page[prefixStart + offset];
            }
            else
            {
                entryByte = page[entryStart + offset - prefixLength];
            }

            int difference = searchKey[offset] - entryByte;
            if (difference != 0)
            {
                return difference;
            }
        }

        return searchKey.Length - canonicalLength;
    }

    private static byte[] DecodeCanonicalKey(
        byte[] page,
        int entryStart,
        int suffixLength,
        int prefixLength,
        byte[]? sharedPrefix,
        bool isFirstEntry)
    {
        if (isFirstEntry)
        {
            byte[] canonical = new byte[suffixLength];
            Buffer.BlockCopy(page, entryStart, canonical, 0, suffixLength);
            return canonical;
        }

        byte[] key = new byte[prefixLength + suffixLength];
        if (prefixLength > 0 && sharedPrefix != null)
        {
            Buffer.BlockCopy(sharedPrefix, 0, key, 0, prefixLength);
        }

        Buffer.BlockCopy(page, entryStart, key, prefixLength, suffixLength);
        return key;
    }
}
