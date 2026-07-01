namespace JetDatabaseWriter.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
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
    {
        if (pageSize <= layout.FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be greater than {layout.FirstEntryOffset}.");
        }

        Guard.NotNull(entries, nameof(entries));

        byte[] page = new byte[pageSize];
        WriteIndexPageHeader(
            page,
            Constants.IndexLeafPage.PageTypeLeaf,
            layout,
            parentTdefPage,
            prevPage,
            nextPage,
            tailPage);

        int prefLen = enablePrefixCompression ? ComputeSharedPrefixLength(entries) : 0;
        if (maxPrefixLength.HasValue && prefLen > maxPrefixLength.Value)
        {
            prefLen = maxPrefixLength.Value;
        }

        Wu16(page, layout.PrefLenOffset, prefLen);

        int payloadCursor = layout.FirstEntryOffset;
        int payloadLimit = pageSize;

        for (int i = 0; i < entries.Count; i++)
        {
            IndexEntry entry = entries[i];
            int keyOffset = i == 0 ? 0 : prefLen;
            int keyLen = entry.Key.Length - keyOffset;
            int entryLen = keyLen + LeafTrailerSize;
            int entryStart = payloadCursor;

            if (entryStart + entryLen > payloadLimit)
            {
                string message = $"Index entries do not fit on a single leaf page (need {entryStart + entryLen} bytes, have {payloadLimit}). B-tree splitting is required for tables this large.";
                throw new ArgumentOutOfRangeException(nameof(entries), message);
            }

            Buffer.BlockCopy(entry.Key, keyOffset, page, entryStart, keyLen);

            long dataPage = entry.DataPage;
            if (dataPage is < 0 or > 0xFFFFFF)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), $"Index entry data page {dataPage} exceeds the 24-bit range.");
            }

            int rowPointerOffset = entryStart + keyLen;
            WriteUInt24BigEndian(page, rowPointerOffset, (int)dataPage);
            page[rowPointerOffset + 3] = entry.DataRow;

            SetEntryStartBit(
                layout,
                page,
                entryStart,
                isFirstEntry: i == 0,
                parameterName: nameof(entries),
                overflowMessage: "Bitmask overflow: too many entries for a single leaf page.");
            payloadCursor += entryLen;
        }

        SetSentinelBit(layout, page, payloadCursor, entries.Count > 0);
        Wu16(page, 2, payloadLimit - payloadCursor);
        return page;
    }

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
    {
        try
        {
            return BuildLeafPage(layout, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, enablePrefixCompression, maxPrefixLength);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

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
    public static byte[] BuildIntermediatePage(
        IndexPageLayout layout,
        int pageSize,
        long parentTdefPage,
        IReadOnlyList<DecodedIntermediateEntry> entries,
        long prevPage,
        long nextPage,
        long tailPage,
        int? maxPrefixLength = null)
    {
        if (pageSize <= layout.FirstEntryOffset)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), $"pageSize must be greater than {layout.FirstEntryOffset}.");
        }

        Guard.NotNull(entries, nameof(entries));
        if (entries.Count == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(entries), "Intermediate pages require at least one entry.");
        }

        byte[] page = new byte[pageSize];
        WriteIndexPageHeader(
            page,
            Constants.IndexLeafPage.PageTypeIntermediate,
            layout,
            parentTdefPage,
            prevPage,
            nextPage,
            tailPage);

        int prefLen = ComputeIntermediatePrefixLength(entries);
        if (maxPrefixLength.HasValue && prefLen > maxPrefixLength.Value)
        {
            prefLen = maxPrefixLength.Value;
        }

        Wu16(page, layout.PrefLenOffset, prefLen);

        int payloadCursor = layout.FirstEntryOffset;
        int payloadLimit = pageSize;

        for (int i = 0; i < entries.Count; i++)
        {
            DecodedIntermediateEntry decoded = entries[i];
            IndexEntry summary = decoded.Entry;
            byte[] key = summary.Key;
            int keyOffset = i == 0 ? 0 : prefLen;
            int keyLen = key.Length - keyOffset;
            int entryLen = keyLen + IntermediateTrailerSize;
            int entryStart = payloadCursor;

            if (entryStart + entryLen > payloadLimit)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), "Intermediate page overflow (internal error).");
            }

            Buffer.BlockCopy(key, keyOffset, page, entryStart, keyLen);

            long dataPage = summary.DataPage;
            if (dataPage is < 0 or > 0xFFFFFF)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), $"Index entry data page {dataPage} exceeds the 24-bit range.");
            }

            int rowPointerOffset = entryStart + keyLen;
            WriteUInt24BigEndian(page, rowPointerOffset, (int)dataPage);
            page[rowPointerOffset + 3] = summary.DataRow;

            long childPage = decoded.ChildPage;
            if (childPage is < 0 or > 0xFFFFFFFFL)
            {
                throw new ArgumentOutOfRangeException(nameof(entries), "Child page exceeds 32-bit range.");
            }

            BinaryPrimitives.WriteUInt32BigEndian(page.AsSpan(rowPointerOffset + LeafTrailerSize, 4), (uint)childPage);

            SetEntryStartBit(
                layout,
                page,
                entryStart,
                isFirstEntry: i == 0,
                parameterName: nameof(entries),
                overflowMessage: "Bitmask overflow on intermediate page.");
            payloadCursor += entryLen;
        }

        SetSentinelBit(layout, page, payloadCursor, hasEntries: true);
        Wu16(page, 2, payloadLimit - payloadCursor);
        return page;
    }

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
    {
        Guard.NotNull(entries, nameof(entries));
        if (entries.Count == 0)
        {
            return null;
        }

        try
        {
            return BuildIntermediatePage(layout, pageSize, parentTdefPage, entries, prevPage, nextPage, tailPage, maxPrefixLength);
        }
        catch (ArgumentOutOfRangeException)
        {
            return null;
        }
    }

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
    public static long ReadTailPage(IndexPageLayout layout, byte[] page)
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
    public static long ReadPrevPage(IndexPageLayout layout, byte[] page)
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
        IndexPageLayout layout,
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
    public static bool IsSingleRootLeaf(IndexPageLayout layout, byte[] page)
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
        IndexPageLayout layout,
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
        IndexPageLayout layout,
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

        int prefixLength = Ru16(page, layout.PrefLenOffset);
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
            if (!IsEntryReadable(page, entryStart, suffixLength, LeafTrailerSize))
            {
                return false;
            }

            if (isFirstEntry && prefixLength > suffixLength)
            {
                return false;
            }

            if (CurrentEntryMatchesRange(in range, out bool continueScanning))
            {
                int pointerOffset = entryStart + suffixLength;
                long dataPage = ReadUInt24BigEndian(page.AsSpan(pointerOffset, 3));
                matches.Add((dataPage, page[pointerOffset + 3]));
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
        if (offset < 0 || offset + 4 > page.Length)
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
            throw new ArgumentOutOfRangeException(parameterName, overflowMessage);
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

    private static int ComputeSharedPrefixLength(IReadOnlyList<IndexEntry> entries)
    {
        if (entries.Count < 2)
        {
            return 0;
        }

        byte[] first = entries[0].Key;
        int prefixLength = first.Length;
        for (int i = 1; i < entries.Count && prefixLength > 0; i++)
        {
            byte[] other = entries[i].Key;
            int max = Math.Min(prefixLength, other.Length);
            int j = 0;
            while (j < max && first[j] == other[j])
            {
                j++;
            }

            prefixLength = j;
        }

        return Math.Min(prefixLength, 0xFFFF);
    }

    private static int ComputeIntermediatePrefixLength(IReadOnlyList<DecodedIntermediateEntry> entries)
    {
        if (entries.Count < 2)
        {
            return 0;
        }

        byte[] first = entries[0].Entry.Key;
        int prefixLength = first.Length;
        for (int i = 1; i < entries.Count && prefixLength > 0; i++)
        {
            byte[] other = entries[i].Entry.Key;
            int max = Math.Min(prefixLength, other.Length);
            int j = 0;
            while (j < max && first[j] == other[j])
            {
                j++;
            }

            prefixLength = j;
        }

        return Math.Min(prefixLength, 0xFFFF);
    }

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
            byte entryByte = ReadCanonicalKeyByte(page, prefixStart, entryStart, prefixLength, isFirstEntry, offset);
            int difference = searchKey[offset] - entryByte;
            if (difference != 0)
            {
                return difference;
            }
        }

        return searchKey.Length - canonicalLength;
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

        for (int offset = 0; offset < requiredPrefix.Length; offset++)
        {
            byte entryByte = ReadCanonicalKeyByte(page, prefixStart, entryStart, prefixLength, isFirstEntry, offset);
            if (entryByte != requiredPrefix[offset])
            {
                return false;
            }
        }

        return true;
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
