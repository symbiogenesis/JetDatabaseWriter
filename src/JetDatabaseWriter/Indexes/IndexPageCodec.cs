namespace JetDatabaseWriter.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Verification;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Layout-aware codec for JET index pages. Owns the common header, bitmask,
/// prefix-compression, page-build, and entry-decoding rules shared by leaf,
/// intermediate, cursor, and mutation code.
/// </summary>
internal static class IndexPageCodec
{
    private const int LeafTrailerSize = 4;
    private const int IntermediateTrailerSize = 8;

    /// <summary>Reads the shared entry prefix byte count, excluding the Jet3 level byte.</summary>
    /// <param name="layout">The page layout.</param>
    /// <param name="page">The page bytes.</param>
    public static int ReadPrefixLength(IndexPageLayout layout, byte[] page)
        => layout.PrefLenOffset == Constants.IndexLeafPage.Jet3.PrefLenOffset
            ? page[layout.PrefLenOffset]
            : Ru16(page, layout.PrefLenOffset);

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
    /// Builds a leaf index page using the supplied per-format layout.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The previous sibling page.</param>
    /// <param name="nextPage">The next sibling page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="enablePrefixCompression">Whether prefix compression is enabled.</param>
    /// <param name="maxPrefixLength">The maximum prefix length.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the page size, entry payload, or page-number fields exceed format limits.</exception>
    /// <exception cref="JetLimitationException">Valid entries exceed the available index page capacity.</exception>
    public static byte[] BuildLeafPage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression,
        int? maxPrefixLength = null)
        => TryBuildLeafPage(layout, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression, maxPrefixLength)
            ?? throw new JetLimitationException(JetErrorCode.IndexEntryTooLarge, "Leaf page capacity exceeded.", new JetErrorInfo { PageNumber = parentTdefPage });

    /// <summary>
    /// Builds a single-page leaf with zeroed sibling/tail pointers — the
    /// shape produced by callers that emit a fresh root leaf rather than
    /// rewriting an existing entry in a sibling chain.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="enablePrefixCompression">Whether prefix compression is enabled.</param>
    /// <param name="maxPrefixLength">The maximum prefix length.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the page size, entry payload, or page-number fields exceed format limits.</exception>
    public static byte[] BuildLeafPage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        bool enablePrefixCompression,
        int? maxPrefixLength = null)
        => BuildLeafPage(
            layout,
            pageSize,
            parentTdefPage,
            entries,
            prevPage: 0,
            nextPage: 0,
            tailPage: 0,
            enablePrefixCompression,
            maxPrefixLength);

    /// <summary>
    /// Attempts to build an index leaf page, returning <see langword="null"/>
    /// when the supplied entries do not fit in one page.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    public static byte[]? TryBuildLeafPage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries)
        => TryBuildLeafPage(layout, pageSize, parentTdefPage, entries, prevPage: 0, nextPage: 0, tailPage: 0, enablePrefixCompression: true);

    /// <summary>
    /// Attempts to build an index leaf page while preserving sibling pointers,
    /// returning <see langword="null"/> when the supplied entries do not fit in one page.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
    public static byte[]? TryBuildLeafPage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage)
        => TryBuildLeafPage(layout, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression: true);

    /// <summary>
    /// Attempts to build an index leaf page, returning <see langword="null"/>
    /// when the supplied entries do not fit in one page.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The prev page.</param>
    /// <param name="nextPage">The next page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="enablePrefixCompression">Whether to emit shared-prefix compression metadata.</param>
    /// <param name="maxPrefixLength">Maximum number of leading bytes that may be shared through prefix compression.</param>
    public static byte[]? TryBuildLeafPage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<IndexEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression,
        int? maxPrefixLength = null)
        => TryBuildPage(layout, pageSize, parentTdefPage, entries, static entry => entry, null, prevPage, nextPage, tailPage, enablePrefixCompression, maxPrefixLength);

    /// <summary>
    /// Builds an intermediate index page using the supplied per-format layout.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The decoded intermediate entries to emit.</param>
    /// <param name="prevPage">The previous sibling page.</param>
    /// <param name="nextPage">The next sibling page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="maxPrefixLength">The maximum prefix length.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the page size, entry payload, or page-number fields exceed format limits.</exception>
    /// <exception cref="JetLimitationException">Valid entries exceed the available index page capacity.</exception>
    public static byte[] BuildIntermediatePage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<DecodedIntermediateEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        int? maxPrefixLength = null)
        => TryBuildIntermediatePage(layout, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, maxPrefixLength)
            ?? throw new JetLimitationException(JetErrorCode.IndexEntryTooLarge, "Intermediate page capacity exceeded.", new JetErrorInfo { PageNumber = parentTdefPage });

    /// <summary>
    /// Attempts to build an intermediate index page, returning <see langword="null"/> on overflow.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The parent TDEF page.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="prevPage">The previous sibling page.</param>
    /// <param name="nextPage">The next sibling page.</param>
    /// <param name="tailPage">The tail page.</param>
    /// <param name="maxPrefixLength">The maximum prefix length.</param>
    public static byte[]? TryBuildIntermediatePage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<DecodedIntermediateEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        int? maxPrefixLength = null)
        => TryBuildPage(layout, pageSize, parentTdefPage, entries, static entry => entry.Entry, static entry => entry.ChildPage, prevPage, nextPage, tailPage, true, maxPrefixLength);

    /// <summary>
    /// Writes all sibling pointer fields in an index page header.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="prevPage">The previous sibling page.</param>
    /// <param name="nextPage">The next sibling page.</param>
    /// <param name="tailPage">The tail page.</param>
    public static void WriteSiblingPointers(
        IndexPageLayout layout,
        byte[] page,
        long prevPage,
        long nextPage,
        long tailPage)
    {
        WritePrevPage(layout, page, prevPage);
        WriteNextPage(layout, page, nextPage);
        WriteTailPage(layout, page, tailPage);
    }

    /// <summary>
    /// Writes the previous-page sibling pointer in an index page header.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="prevPage">The previous sibling page.</param>
    public static void WritePrevPage(IndexPageLayout layout, byte[] page, long prevPage)
        => WritePageNumber32(page, layout.PrevPageOffset, prevPage, nameof(prevPage));

    /// <summary>
    /// Writes the next-page sibling pointer in an index page header.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="nextPage">The next sibling page.</param>
    public static void WriteNextPage(IndexPageLayout layout, byte[] page, long nextPage)
        => WritePageNumber32(page, layout.NextPageOffset, nextPage, nameof(nextPage));

    /// <summary>
    /// Writes the tail-page pointer in an index page header.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="tailPage">The tail page.</param>
    public static void WriteTailPage(IndexPageLayout layout, byte[] page, long tailPage)
        => WritePageNumber32(page, layout.TailPageOffset, tailPage, nameof(tailPage));

    /// <summary>
    /// Returns the page number recorded in the <c>next_page</c> sibling field.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    public static long ReadNextPage(IndexPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < (long)layout.NextPageOffset + 4)
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
    public static long ReadTailPage(IndexPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < (long)layout.TailPageOffset + 4)
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
    public static long ReadPrevPage(IndexPageLayout layout, byte[] page)
    {
        if (page == null || page.Length < (long)layout.PrevPageOffset + 4)
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
        IndexPageLayout layout,
        byte[] page)
    {
        if (page == null || page.Length < (long)layout.TailPageOffset + 4)
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
    public static bool IsSingleRootLeaf(IndexPageLayout layout, byte[] page)
    {
        if (!IsLeaf(page) || page.Length < (long)layout.TailPageOffset + 4)
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
        IndexPageLayout layout,
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

    /// <summary>Reads the final child pointer after reconstructing shared pointer bytes.</summary>
    /// <param name="layout">The page layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    public static long ReadLastChildPointer(IndexPageLayout layout, byte[] page, int pageSize)
    {
        if (!IsIntermediate(page)
            || !TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || payloadEnd <= layout.FirstEntryOffset)
        {
            return 0;
        }

        int prefixLength = ReadPrefixLength(layout, page);
        int entryStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int fullLength = entryEnd - entryStart + (isFirstEntry ? 0 : prefixLength);
            if (fullLength < IntermediateTrailerSize || (isFirstEntry && prefixLength > fullLength))
            {
                return 0;
            }

            if (entryEnd == payloadEnd)
            {
                long childPage = 0;
                for (int pointerByte = 0; pointerByte < 4; pointerByte++)
                {
                    childPage = (childPage << 8) | ReadCanonicalKeyByte(page, layout.FirstEntryOffset, entryStart, prefixLength, isFirstEntry, fullLength - 4 + pointerByte);
                }

                return childPage;
            }

            isFirstEntry = false;
            entryStart = entryEnd;
        }

        return 0;
    }

    /// <summary>
    /// Reads a big-endian 4-byte child-page pointer from an intermediate entry.
    /// </summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="offset">The offset.</param>
    public static long DecodeIntermediateChildPointer(byte[] page, int offset)
    {
        if (page == null || !BinaryBuffer.IsRange(page.Length, offset, 4))
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
        IndexPageLayout layout,
        byte[] page,
        int pageSize)
    {
        _ = TryDecodeLeafEntriesCore(layout, page, pageSize, strict: false, out List<IndexEntry> result);
        return result;
    }

    /// <summary>Decodes leaf entries only when the entire payload is valid.</summary>
    /// <param name="layout">The page layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="result">The decoded entries, possibly partial on failure.</param>
    /// <returns>Whether every entry in the payload was decoded.</returns>
    public static bool TryDecodeLeafEntries(
        IndexPageLayout layout,
        byte[] page,
        int pageSize,
        out List<IndexEntry> result)
        => TryDecodeLeafEntriesCore(layout, page, pageSize, strict: true, out result);

    /// <summary>
    /// Decodes intermediate entries into canonical summary key, row pointer,
    /// and child-page pointer tuples.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    public static List<DecodedIntermediateEntry> DecodeIntermediateEntries(
        IndexPageLayout layout,
        byte[] page,
        int pageSize)
    {
        _ = TryDecodeIntermediateEntries(layout, page, pageSize, out List<DecodedIntermediateEntry> result);
        return result;
    }

    /// <summary>Decodes intermediate entries only when the entire payload is valid.</summary>
    /// <param name="layout">The page layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="result">The decoded entries, possibly partial on failure.</param>
    /// <returns>Whether every entry in the payload was decoded.</returns>
    public static bool TryDecodeIntermediateEntries(
        IndexPageLayout layout,
        byte[] page,
        int pageSize,
        out List<DecodedIntermediateEntry> result)
    {
        result = [];
        if (!IsIntermediate(page)
            || !TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || payloadEnd < layout.FirstEntryOffset)
        {
            return false;
        }

        int prefixLength = ReadPrefixLength(layout, page);
        byte[]? sharedPrefix = null;
        int entryStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int storedLength = entryEnd - entryStart;
            byte[] fullEntry = DecodeCanonicalKey(page, entryStart, storedLength, prefixLength, sharedPrefix, isFirstEntry);
            if (fullEntry.Length < IntermediateTrailerSize || (isFirstEntry && prefixLength > fullEntry.Length))
            {
                return false;
            }

            if (isFirstEntry && prefixLength > 0)
            {
                sharedPrefix = new byte[prefixLength];
                Buffer.BlockCopy(fullEntry, 0, sharedPrefix, 0, prefixLength);
            }

            int keyLength = fullEntry.Length - IntermediateTrailerSize;
            byte[] canonicalKey = new byte[keyLength];
            Buffer.BlockCopy(fullEntry, 0, canonicalKey, 0, keyLength);
            long dataPage = ReadUInt24BigEndian(fullEntry.AsSpan(keyLength, 3));
            byte dataRow = fullEntry[keyLength + 3];
            long childPage = DecodeIntermediateChildPointer(fullEntry, keyLength + 4);
            result.Add(new DecodedIntermediateEntry(new IndexEntry(canonicalKey, dataPage, dataRow), childPage));

            isFirstEntry = false;
            if (nextEntryStart < 0)
            {
                break;
            }

            entryStart = nextEntryStart;
        }

        return true;
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
        IndexPageLayout layout,
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

        int prefixLength = ReadPrefixLength(layout, page);
        int entryStart = layout.FirstEntryOffset;
        int prefixStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int suffixLength = entryEnd - entryStart - IntermediateTrailerSize;
            if (suffixLength + (isFirstEntry ? 0 : prefixLength) < 0 || entryStart + suffixLength + IntermediateTrailerSize > page.Length)
            {
                return null;
            }

            if (isFirstEntry && prefixLength > suffixLength + IntermediateTrailerSize)
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
                long childPage = 0;
                int pointerOffset = suffixLength + (isFirstEntry ? 0 : prefixLength) + 4;
                for (int pointerByte = 0; pointerByte < 4; pointerByte++)
                {
                    childPage = (childPage << 8) | ReadCanonicalKeyByte(page, prefixStart, entryStart, prefixLength, isFirstEntry, pointerOffset + pointerByte);
                }

                return childPage;
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
        IndexPageLayout layout,
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
        IndexPageLayout layout,
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
    /// Appends row-location matches from one leaf page when their canonical
    /// keys fall within the supplied encoded bounds.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="range">The encoded range.</param>
    /// <param name="matches">The matches.</param>
    public static bool CollectRangeLeafEntries(
        IndexPageLayout layout,
        byte[] page,
        int pageSize,
        in EncodedIndexRange range,
        List<(long DataPage, int RowIndex)> matches)
    {
        if (!IsLeaf(page)
            || !TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || payloadEnd <= layout.FirstEntryOffset)
        {
            return false;
        }

        int prefixLength = ReadPrefixLength(layout, page);
        int entryStart = layout.FirstEntryOffset;
        int prefixStart = layout.FirstEntryOffset;
        int suffixLength;
        bool isFirstEntry = true;
        bool hasEntries = false;

        bool CurrentEntryMatchesRange(in EncodedIndexRange currentRange, out bool continueScanning)
        {
            continueScanning = true;

            EncodedIndexBound lowerBound = currentRange.Lower;
            if (!lowerBound.IsUnbounded)
            {
                byte[] lowerKey = lowerBound.Key!;
                int lowerComparison = CompareCurrentEntry(lowerKey);
                if (lowerComparison > 0
                    || (!lowerBound.Inclusive && lowerComparison == 0)
                    || (!lowerBound.Inclusive && lowerBound.IsPrefix && CurrentEntryStartsWith(lowerKey)))
                {
                    return false;
                }
            }

            byte[]? requiredPrefix = currentRange.RequiredPrefix;
            if (requiredPrefix != null)
            {
                int prefixComparison = CompareCurrentEntry(requiredPrefix);
                if (prefixComparison > 0)
                {
                    return false;
                }

                if (!CurrentEntryStartsWith(requiredPrefix))
                {
                    continueScanning = false;
                    return false;
                }
            }

            EncodedIndexBound upperBound = currentRange.Upper;
            if (!upperBound.IsUnbounded)
            {
                byte[] upperKey = upperBound.Key!;
                int upperComparison = CompareCurrentEntry(upperKey);
                bool startsWithUpper = upperBound.IsPrefix && CurrentEntryStartsWith(upperKey);
                if (startsWithUpper && upperBound.Inclusive)
                {
                    return true;
                }

                if (upperComparison < 0 || (!upperBound.Inclusive && upperComparison == 0))
                {
                    continueScanning = false;
                    return false;
                }
            }

            return true;
        }

        int CompareCurrentEntry(byte[] key)
            => CompareSearchKeyToEntry(
                key,
                page,
                prefixStart,
                entryStart,
                suffixLength,
                prefixLength,
                isFirstEntry);

        bool CurrentEntryStartsWith(byte[] key)
            => EntryStartsWithKey(
                page,
                prefixStart,
                entryStart,
                suffixLength,
                prefixLength,
                isFirstEntry,
                key);

        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            suffixLength = entryEnd - entryStart - LeafTrailerSize;
            if (suffixLength + (isFirstEntry ? 0 : prefixLength) < 0 || entryStart + suffixLength + LeafTrailerSize > page.Length)
            {
                return false;
            }

            if (isFirstEntry && prefixLength > suffixLength + LeafTrailerSize)
            {
                return false;
            }

            if (CurrentEntryMatchesRange(in range, out bool continueScanning))
            {
                int keyLength = suffixLength + (isFirstEntry ? 0 : prefixLength);
                long dataPage = 0;
                for (int pointerByte = 0; pointerByte < 3; pointerByte++)
                {
                    dataPage = (dataPage << 8) | ReadCanonicalKeyByte(page, prefixStart, entryStart, prefixLength, isFirstEntry, keyLength + pointerByte);
                }

                byte rowIndex = ReadCanonicalKeyByte(page, prefixStart, entryStart, prefixLength, isFirstEntry, keyLength + 3);
                matches.Add((dataPage, rowIndex));
            }

            if (!continueScanning)
            {
                return false;
            }

            hasEntries = true;
            if (nextEntryStart < 0)
            {
                break;
            }

            isFirstEntry = false;
            entryStart = nextEntryStart;
        }

        return hasEntries;
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
        IndexPageLayout layout,
        byte[] page,
        int payloadEnd,
        int currentStart)
    {
        if (page == null || layout.BitmaskOffset < 0 || layout.FirstEntryOffset < 0
            || payloadEnd < 0 || currentStart < 0)
        {
            return -1;
        }

        uint next = RecoveryKernel.FindIndexEntryStart(page, (uint)layout.BitmaskOffset, (uint)layout.FirstEntryOffset, (uint)payloadEnd, (uint)currentStart);
        return next == uint.MaxValue ? -1 : checked((int)next);
    }

    /// <summary>
    /// Lexicographically compares encoded index keys using unsigned byte order.
    /// </summary>
    /// <param name="left">The left value.</param>
    /// <param name="right">The right.</param>
    public static int CompareKeyBytes(byte[] left, byte[] right)
        => left.AsSpan().SequenceCompareTo(right);

    /// <summary>Measures a valid leaf without allocating or emitting its page bytes.</summary>
    /// <param name="layout">The page layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="entries">The leaf entries.</param>
    /// <param name="maxPrefixLength">The prefix ceiling to preserve.</param>
    /// <param name="freeSpace">The remaining payload bytes when the entries fit.</param>
    internal static bool TryMeasureLeafPage(IndexPageLayout layout, int pageSize, IReadOnlyList<IndexEntry> entries, int maxPrefixLength, out int freeSpace)
        => CanBuildPage(layout, pageSize, 0, entries, static entry => entry, null, 0, 0, 0, true, maxPrefixLength, out _, out freeSpace);

    /// <summary>Packs entries greedily using exact payload sizes, without building candidate pages.</summary>
    /// <typeparam name="T">The leaf or intermediate entry representation.</typeparam>
    /// <param name="layout">The page layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="parentTdefPage">The owner table definition.</param>
    /// <param name="entries">The entries in input order.</param>
    /// <param name="getEntry">Reads the key and row pointer.</param>
    /// <param name="getChild">Reads an intermediate child pointer, or null for leaves.</param>
    /// <param name="enablePrefixCompression">Whether partitioning accounts for shared prefixes.</param>
    internal static List<List<T>>? PartitionPages<T>(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<T> entries,
        Func<T, IndexEntry> getEntry,
        Func<T, long>? getChild,
        bool enablePrefixCompression)
    {
        // Validate every pointer before a capacity refusal, including later entries.
        _ = CanBuildPage(layout, pageSize, parentTdefPage, entries, getEntry, getChild, 0, 0, 0, enablePrefixCompression, null, out _, out _);
        var groups = new List<List<T>>();
        var current = new List<T>();
        long rawSize = 0;
        int sharedPrefix = 0;
        int prefixLimit = layout.PrefLenOffset == Constants.IndexLeafPage.Jet3.PrefLenOffset ? byte.MaxValue : ushort.MaxValue;
        int trailerSize = getChild is null ? LeafTrailerSize : IntermediateTrailerSize;
        int payloadArea = pageSize - layout.FirstEntryOffset;
        foreach (T item in entries)
        {
            byte[] key = getEntry(item).Key;
            long entrySize = (long)key.Length + trailerSize;
            if (entrySize > payloadArea)
            {
                return null;
            }

            int prefix = 0;
            if (enablePrefixCompression && current.Count > 0)
            {
                byte[] first = getEntry(current[0]).Key;
                int limit = Math.Min(sharedPrefix, key.Length);
                while (prefix < limit && first[prefix] == key[prefix])
                {
                    prefix++;
                }
            }

            long candidateSize = rawSize + entrySize - ((long)current.Count * prefix);
            long entryStart = rawSize - ((long)(current.Count - 1) * prefix);
            if (candidateSize > payloadArea || (current.Count > 0 && layout.BitmaskOffset + (entryStart / 8) >= layout.FirstEntryOffset))
            {
                groups.Add(current);
                current = [];
                rawSize = 0;
            }

            sharedPrefix = current.Count == 0 ? Math.Min(key.Length, prefixLimit) : prefix;
            current.Add(item);
            rawSize += entrySize;
        }

        if (current.Count > 0 || entries.Count == 0)
        {
            groups.Add(current);
        }

        return groups;
    }

    private static byte[]? TryBuildPage<T>(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<T> entries,
        Func<T, IndexEntry> getEntry,
        Func<T, long>? getChild,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression,
        int? maxPrefixLength)
    {
        if (!CanBuildPage(layout, pageSize, parentTdefPage, entries, getEntry, getChild, prevPage, nextPage, tailPage, enablePrefixCompression, maxPrefixLength, out int prefix, out _))
        {
            return null;
        }

        byte[] page = new byte[pageSize];
        WriteIndexPageHeader(page, getChild is null ? Constants.IndexLeafPage.PageTypeLeaf : Constants.IndexLeafPage.PageTypeIntermediate, layout, parentTdefPage, prevPage, nextPage, tailPage);
        if (layout.PrefLenOffset == Constants.IndexLeafPage.Jet3.PrefLenOffset)
        {
            page[layout.PrefLenOffset] = (byte)prefix;
        }
        else
        {
            Wu16(page, layout.PrefLenOffset, prefix);
        }

        int cursor = layout.FirstEntryOffset;
        for (int i = 0; i < entries.Count; i++)
        {
            T item = entries[i];
            IndexEntry entry = getEntry(item);
            int keyOffset = i == 0 ? 0 : prefix;
            int keyLength = entry.Key.Length - keyOffset;
            Buffer.BlockCopy(entry.Key, keyOffset, page, cursor, keyLength);
            int pointerOffset = cursor + keyLength;
            WriteUInt24BigEndian(page, pointerOffset, (int)entry.DataPage);
            page[pointerOffset + 3] = entry.DataRow;
            if (getChild is not null)
            {
                BinaryPrimitives.WriteUInt32BigEndian(page.AsSpan(pointerOffset + LeafTrailerSize, 4), (uint)getChild(item));
            }

            SetEntryStartBit(layout, page, cursor, isFirstEntry: i == 0, parameterName: nameof(entries), overflowMessage: "Bitmask overflow on index page.");
            cursor = pointerOffset + (getChild is null ? LeafTrailerSize : IntermediateTrailerSize);
        }

        SetSentinelBit(layout, page, cursor, entries.Count > 0);
        Wu16(page, 2, pageSize - cursor);
        return page;
    }

    private static bool CanBuildPage<T>(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<T> entries,
        Func<T, IndexEntry> getEntry,
        Func<T, long>? getChild,
        long prevPage,
        long nextPage,
        long tailPage,
        bool enablePrefixCompression,
        int? maxPrefixLength,
        out int prefix,
        out int freeSpace)
    {
        freeSpace = 0;
        Guard.NotNull(entries, nameof(entries));
        if (pageSize <= layout.FirstEntryOffset || pageSize - layout.FirstEntryOffset > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize));
        }

        if (maxPrefixLength < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPrefixLength));
        }

        // Validate all metadata before capacity: even an entry after an overfull
        // payload must never turn an invalid pointer into a split request.
        ValidatePageNumber(parentTdefPage, nameof(parentTdefPage));
        ValidatePageNumber(prevPage, nameof(prevPage));
        ValidatePageNumber(nextPage, nameof(nextPage));
        ValidatePageNumber(tailPage, nameof(tailPage));
        if (layout.PrevPageOffset < 0 || (long)layout.PrevPageOffset + 4 > layout.FirstEntryOffset
            || layout.NextPageOffset < 0 || (long)layout.NextPageOffset + 4 > layout.FirstEntryOffset
            || layout.TailPageOffset < 0 || (long)layout.TailPageOffset + 4 > layout.FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(layout));
        }

        if (layout.BitmaskOffset < 0 || layout.BitmaskOffset >= layout.FirstEntryOffset
            || layout.PrefLenOffset < 0 || (long)layout.PrefLenOffset + 2 > layout.FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(layout));
        }

        if (entries.Count > 0)
        {
            Guard.NotNull(getEntry(entries[0]).Key, nameof(entries));
        }

        int sharedPrefix = entries.Count < 2 ? 0 : getEntry(entries[0]).Key.Length;
        foreach (T item in entries)
        {
            IndexEntry entry = getEntry(item);
            Guard.NotNull(entry.Key, nameof(entries));
            if (entry.DataPage is < 0 or > 0xFFFFFF
                || (getChild is not null && getChild(item) is < 0 or > 0xFFFFFFFFL))
            {
                throw new ArgumentOutOfRangeException(nameof(entries), "Index entry pointer exceeds its format range.");
            }

            int matched = 0;
            byte[] firstKey = getEntry(entries[0]).Key;
            while (matched < sharedPrefix && matched < entry.Key.Length && firstKey[matched] == entry.Key[matched])
            {
                matched++;
            }

            sharedPrefix = matched;
        }

        prefix = enablePrefixCompression ? Math.Min(sharedPrefix, ushort.MaxValue) : 0;
        prefix = Math.Min(prefix, maxPrefixLength ?? prefix);
        if (layout.PrefLenOffset == Constants.IndexLeafPage.Jet3.PrefLenOffset)
        {
            prefix = Math.Min(prefix, byte.MaxValue);
        }

        long cursor = layout.FirstEntryOffset;
        for (int i = 0; i < entries.Count; i++)
        {
            if (i > 0 && layout.BitmaskOffset + ((cursor - layout.FirstEntryOffset) / 8) >= layout.FirstEntryOffset)
            {
                return false;
            }

            cursor += (long)getEntry(entries[i]).Key.Length - (i == 0 ? 0 : prefix) + (getChild is null ? LeafTrailerSize : IntermediateTrailerSize);
            if (cursor > pageSize)
            {
                return false;
            }
        }

        freeSpace = pageSize - checked((int)cursor);
        return getChild is null || entries.Count > 0;
    }

    private static void ValidatePageNumber(long pageNumber, string parameterName)
    {
        if (pageNumber is < 0 or > 0xFFFFFFFFL)
        {
            throw new ArgumentOutOfRangeException(parameterName, "Index page number exceeds the 32-bit range.");
        }
    }

    private static void WriteIndexPageHeader(
        byte[] page,
        byte pageType,
        IndexPageLayout layout,
        long parentTdefPage,
        long prevPage,
        long nextPage,
        long tailPage)
    {
        page[0] = pageType;
        page[1] = 0x01;
        WritePageNumber32(page, 4, parentTdefPage, nameof(parentTdefPage));
        WriteSiblingPointers(layout, page, prevPage, nextPage, tailPage);
    }

    private static void WritePageNumber32(byte[] page, int offset, long pageNumber, string parameterName)
    {
        Guard.NotNull(page, nameof(page));
        if (!BinaryBuffer.IsRange(page.Length, offset, 4))
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "Page number offset is outside the page buffer.");
        }

        if (pageNumber is < 0 or > 0xFFFFFFFFL)
        {
            throw new ArgumentOutOfRangeException(parameterName, "Index page number exceeds the 32-bit range.");
        }

        Wu32(page, offset, (uint)pageNumber);
    }

    private static void SetEntryStartBit(
        IndexPageLayout layout,
        byte[] page,
        int entryStart,
        bool isFirstEntry,
        string parameterName,
        string overflowMessage)
    {
        if (isFirstEntry)
        {
            return;
        }

        int bitIndex = entryStart - layout.FirstEntryOffset;
        int byteOffset = layout.BitmaskOffset + (bitIndex / 8);
        if (byteOffset >= layout.FirstEntryOffset)
        {
            throw new JetLimitationException(JetErrorCode.IndexEntryTooLarge, overflowMessage, new JetErrorInfo { Reason = parameterName });
        }

        page[byteOffset] |= (byte)(1 << (bitIndex % 8));
    }

    private static void SetSentinelBit(
        IndexPageLayout layout,
        byte[] page,
        int payloadCursor,
        bool hasEntries)
    {
        if (!hasEntries)
        {
            return;
        }

        int sentinelBitIndex = payloadCursor - layout.FirstEntryOffset;
        int sentinelByteOffset = layout.BitmaskOffset + (sentinelBitIndex / 8);
        if (sentinelByteOffset < layout.FirstEntryOffset)
        {
            page[sentinelByteOffset] |= (byte)(1 << (sentinelBitIndex % 8));
        }
    }

    private static bool TryDecodeLeafEntriesCore(
        IndexPageLayout layout,
        byte[] page,
        int pageSize,
        bool strict,
        out List<IndexEntry> result)
    {
        result = [];
        if ((strict && !IsLeaf(page))
            || !TryGetPayloadEnd(layout, page, pageSize, out int payloadEnd)
            || (strict && !HasExactLeafPayloadBoundary(layout, page, payloadEnd)))
        {
            return false;
        }

        int prefixLength = ReadPrefixLength(layout, page);
        if (payloadEnd == layout.FirstEntryOffset)
        {
            return !strict || prefixLength == 0;
        }

        byte[]? sharedPrefix = null;
        int entryStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int storedLength = entryEnd - entryStart;
            byte[] fullEntry = DecodeCanonicalKey(page, entryStart, storedLength, prefixLength, sharedPrefix, isFirstEntry);
            if (fullEntry.Length < LeafTrailerSize || (isFirstEntry && prefixLength > fullEntry.Length))
            {
                return false;
            }

            if (isFirstEntry && prefixLength > 0)
            {
                sharedPrefix = new byte[prefixLength];
                Buffer.BlockCopy(fullEntry, 0, sharedPrefix, 0, prefixLength);
            }

            int keyLength = fullEntry.Length - LeafTrailerSize;
            byte[] canonicalKey = new byte[keyLength];
            Buffer.BlockCopy(fullEntry, 0, canonicalKey, 0, keyLength);
            long dataPage = ReadUInt24BigEndian(fullEntry.AsSpan(keyLength, 3));
            byte dataRow = fullEntry[keyLength + 3];
            result.Add(new IndexEntry(canonicalKey, dataPage, dataRow));

            isFirstEntry = false;
            if (nextEntryStart < 0)
            {
                break;
            }

            entryStart = nextEntryStart;
        }

        return true;
    }

    private static bool HasExactLeafPayloadBoundary(IndexPageLayout layout, byte[] page, int payloadEnd)
        => layout.BitmaskOffset >= 0 && layout.FirstEntryOffset >= 0 && payloadEnd >= 0
            && RecoveryKernel.CheckIndexLeafBitmap(page, (uint)layout.BitmaskOffset, (uint)layout.FirstEntryOffset, (uint)payloadEnd);

    private static bool TryGetPayloadEnd(
        IndexPageLayout layout,
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
        IndexPageLayout layout,
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

        int prefixLength = ReadPrefixLength(layout, page);
        int entryStart = layout.FirstEntryOffset;
        int prefixStart = layout.FirstEntryOffset;
        bool isFirstEntry = true;
        bool hasEntries = false;
        while (entryStart < payloadEnd)
        {
            int nextEntryStart = NextEntryStart(layout, page, payloadEnd, entryStart);
            int entryEnd = nextEntryStart < 0 ? payloadEnd : nextEntryStart;
            int suffixLength = entryEnd - entryStart - LeafTrailerSize;
            if (suffixLength + (isFirstEntry ? 0 : prefixLength) < 0 || entryStart + suffixLength + LeafTrailerSize > page.Length)
            {
                return false;
            }

            if (isFirstEntry && prefixLength > suffixLength + LeafTrailerSize)
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

                int keyLength = suffixLength + (isFirstEntry ? 0 : prefixLength);
                long dataPage = 0;
                for (int pointerByte = 0; pointerByte < 3; pointerByte++)
                {
                    dataPage = (dataPage << 8) | ReadCanonicalKeyByte(page, prefixStart, entryStart, prefixLength, isFirstEntry, keyLength + pointerByte);
                }

                byte rowIndex = ReadCanonicalKeyByte(page, prefixStart, entryStart, prefixLength, isFirstEntry, keyLength + 3);
                matches.Add((dataPage, rowIndex));
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

    private static int CompareSearchKeyToEntry(
        byte[] searchKey,
        byte[] page,
        int prefixStart,
        int entryStart,
        int suffixLength,
        int prefixLength,
        bool isFirstEntry)
    {
        int searchLength = searchKey.Length;
        if (isFirstEntry || prefixLength == 0)
        {
            return searchKey.AsSpan().SequenceCompareTo(page.AsSpan(entryStart, suffixLength));
        }

        // Native compression can include bookmark bytes, so only the key's part
        // of the shared prefix participates in comparison.
        int canonicalLength = prefixLength + suffixLength;
        int keyPrefixLength = Math.Min(prefixLength, canonicalLength);
        int comparison = searchKey.AsSpan(0, Math.Min(searchLength, keyPrefixLength))
            .SequenceCompareTo(page.AsSpan(prefixStart, keyPrefixLength));
        return comparison != 0
            ? comparison
            : searchKey.AsSpan(keyPrefixLength).SequenceCompareTo(page.AsSpan(entryStart, canonicalLength - keyPrefixLength));
    }

    private static bool EntryStartsWithKey(
        byte[] page,
        int prefixStart,
        int entryStart,
        int suffixLength,
        int prefixLength,
        bool isFirstEntry,
        byte[] requiredPrefix)
    {
        int canonicalLength = isFirstEntry || prefixLength == 0 ? suffixLength : prefixLength + suffixLength;
        if (requiredPrefix.Length > canonicalLength)
        {
            return false;
        }

        if (isFirstEntry || prefixLength == 0)
        {
            return requiredPrefix.AsSpan().SequenceEqual(page.AsSpan(entryStart, requiredPrefix.Length));
        }

        int comparedPrefixLength = Math.Min(prefixLength, requiredPrefix.Length);
        return requiredPrefix.AsSpan(0, comparedPrefixLength).SequenceEqual(page.AsSpan(prefixStart, comparedPrefixLength))
            && requiredPrefix.AsSpan(comparedPrefixLength).SequenceEqual(page.AsSpan(entryStart, requiredPrefix.Length - comparedPrefixLength));
    }

    private static byte ReadCanonicalKeyByte(
        byte[] page,
        int prefixStart,
        int entryStart,
        int prefixLength,
        bool isFirstEntry,
        int offset)
    {
        if (isFirstEntry || prefixLength == 0)
        {
            return page[entryStart + offset];
        }

        return offset < prefixLength
            ? page[prefixStart + offset]
            : page[entryStart + offset - prefixLength];
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
