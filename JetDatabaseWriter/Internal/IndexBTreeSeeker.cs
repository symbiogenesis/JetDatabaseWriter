namespace JetDatabaseWriter.Internal;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Helpers;

/// <summary>
/// Read-only B-tree seeker over JET index pages emitted by
/// <see cref="IndexBTreeBuilder"/> / <see cref="IndexLeafPageBuilder"/>. Walks
/// from a real-idx <c>first_dp</c> root through any number of intermediate
/// (<c>0x03</c>) pages down to a leaf (<c>0x04</c>) and reports whether a
/// supplied encoded composite key matches any entry. Used by the writer's
/// referential-integrity enforcement path so an INSERT into a child table can
/// validate parent-side existence in O(log N) page reads instead of an O(N)
/// linear parent scan (see <c>docs/design/index-and-relationship-format-notes.md</c>
/// §4 and §7 R3).
/// <para>
/// Jet4 / ACE only — bitmask at <c>0x1B</c>, first entry at <c>0x1E0</c>,
/// matching <see cref="IndexLeafPageBuilder.Jet4BitmaskOffset"/> and
/// <see cref="IndexLeafPageBuilder.Jet4FirstEntryOffset"/>.
/// </para>
/// <para>
/// Honours §4.4 prefix compression (re-prepends the page <c>pref_len</c>
/// bytes from the first entry on every subsequent entry on the same page).
/// Honours intermediate-page descent by selecting the first child whose
/// summary key is &gt;= the search key; non-unique indexes that span sibling
/// leaves are handled by following <c>next_page</c> while the leaf's first
/// entry still equals the search key.
/// </para>
/// <para>
/// <b>tail-page append:</b> honours the §4.5 <c>tail_page</c> append
/// optimisation. When intermediate descent at any level finds that the
/// search key sorts strictly greater than every summary on the page (i.e.
/// would otherwise return "not present"), the seeker follows that page's
/// <c>tail_page</c> header field if non-zero — the writer stamps it with
/// the absolute page number of the rightmost leaf so reads land on the
/// tail leaf without descending the tree. The append-only incremental
/// fast path can leave the rightmost intermediate summary stale, so this
/// fall-through is required for correctness, not just performance.
/// </para>
/// <para>
/// <b>Out of scope:</b> Jet3 page layouts (Jet3 index emission is supported
/// by the writer but the FK-side seeker is currently Jet4 / ACE only —
/// Jet3 RI enforcement falls back to the O(N) HashSet snapshot path).
/// </para>
/// </summary>
internal static class IndexBTreeSeeker
{
    private const byte PageTypeIntermediate = IndexBTreeBuilder.PageTypeIntermediate;
    private const byte PageTypeLeaf = IndexLeafPageBuilder.PageTypeLeaf;
    private const int BitmaskOffset = IndexLeafPageBuilder.Jet4BitmaskOffset;
    private const int FirstEntryOffset = IndexLeafPageBuilder.Jet4FirstEntryOffset;

    /// <summary>
    /// Returns <see langword="true"/> when at least one entry in the B-tree
    /// rooted at <paramref name="rootPageNumber"/> has an encoded key equal to
    /// <paramref name="searchKey"/>. The supplied key MUST be the byte
    /// sequence produced by <see cref="IndexKeyEncoder.EncodeEntry(byte, object?, bool)"/>
    /// concatenated across the index's key columns in col-map order — i.e.
    /// the same composite-key bytes the leaf entries store.
    /// </summary>
    /// <param name="readPage">Async page-read callback. Returns a buffer the
    /// seeker treats as borrowed; ownership stays with the caller (the seeker
    /// reads and discards without invoking <c>ReturnPage</c>, since the page
    /// pool is private to the writer).</param>
    /// <param name="pageSize">Database page size (4096 for Jet4 / ACE).</param>
    /// <param name="rootPageNumber">Page number from the real-idx
    /// <c>first_dp</c> field. May reference either a leaf (<c>0x04</c>) or an
    /// intermediate (<c>0x03</c>) page.</param>
    /// <param name="searchKey">Composite encoded key bytes to match.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public static async ValueTask<bool> ContainsKeyAsync(
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        int pageSize,
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(readPage, nameof(readPage));
        Guard.NotNull(searchKey, nameof(searchKey));

        if (rootPageNumber <= 0 || pageSize <= FirstEntryOffset)
        {
            return false;
        }

        // Descend through intermediate pages until we land on a leaf.
        long currentPage = rootPageNumber;
        for (int depth = 0; depth < 32; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await readPage(currentPage, cancellationToken).ConfigureAwait(false);
            byte pageType = page[0];

            if (pageType == PageTypeLeaf)
            {
                return await SeekLeafChain(readPage, pageSize, currentPage, page, searchKey, cancellationToken).ConfigureAwait(false);
            }

            if (pageType != PageTypeIntermediate)
            {
                // Tiny tables can store rows on the same page the real-idx
                // points at (per HACKING.md §4 — "for very small tables,
                // first_dp can point directly at a 0x01 data page"). The
                // writer never emits that shape, but be defensive.
                return false;
            }

            // Intermediate descent: pick the first entry whose summary key is
            // >= searchKey and follow its child pointer. When every summary
            // is < searchKey, fall through to the page's tail_page if
            // non-zero — the rightmost leaf can hold append-only entries
            // whose key has not yet been promoted into the intermediate's
            // summary record.
            long? next = SelectChildPage(pageSize, page, searchKey);
            if (next == null)
            {
                long tail = (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(16, 4));
                if (tail <= 0)
                {
                    return false;
                }

                next = tail;
            }

            currentPage = next.Value;
        }

        // Defensive: an excessively deep tree implies a malformed file. The
        // 32-level cap leaves plenty of headroom (a 4 KB page holds dozens of
        // intermediate entries; 32 levels would index 10^40+ rows).
        return false;
    }

    /// <summary>
    /// Like <see cref="ContainsKeyAsync"/> but accumulates the
    /// <c>(dataPage, rowIndex)</c> row pointer of every leaf entry whose
    /// canonical key equals <paramref name="searchKey"/>. Walks sibling
    /// leaves while the leaf's last canonical key still equals
    /// <paramref name="searchKey"/> so non-unique keys spanning multiple
    /// leaves are fully enumerated. Returns an empty list when the key
    /// is absent. Used by the writer's cascade-update / cascade-delete
    /// paths to locate dependent child rows in O(log N + K) page reads
    /// instead of an O(N) child-table snapshot scan.
    /// </summary>
    public static async ValueTask<List<(long DataPage, int RowIndex)>> FindRowLocationsAsync(
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        int pageSize,
        long rootPageNumber,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(readPage, nameof(readPage));
        Guard.NotNull(searchKey, nameof(searchKey));

        var matches = new List<(long DataPage, int RowIndex)>();
        if (rootPageNumber <= 0 || pageSize <= FirstEntryOffset)
        {
            return matches;
        }

        long currentPage = rootPageNumber;
        for (int depth = 0; depth < 32; depth++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await readPage(currentPage, cancellationToken).ConfigureAwait(false);
            byte pageType = page[0];

            if (pageType == PageTypeLeaf)
            {
                await CollectLeafChain(readPage, pageSize, page, searchKey, matches, cancellationToken).ConfigureAwait(false);
                return matches;
            }

            if (pageType != PageTypeIntermediate)
            {
                return matches;
            }

            long? next = SelectChildPage(pageSize, page, searchKey);
            if (next == null)
            {
                long tail = (uint)BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(16, 4));
                if (tail <= 0)
                {
                    return matches;
                }

                next = tail;
            }

            currentPage = next.Value;
        }

        return matches;
    }

    /// <summary>
    /// Scans <paramref name="leafPage"/> for an entry equal to
    /// <paramref name="searchKey"/>; if not found and the search key matches
    /// the leaf's last entry, follows <c>next_page</c> and continues — handles
    /// the rare case where a non-unique key spans sibling leaves.
    /// </summary>
    private static async ValueTask<bool> SeekLeafChain(
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        int pageSize,
        long currentPage,
        byte[] leafPage,
        byte[] searchKey,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        while (page != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(20, 2));
            int payloadEnd = pageSize - BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
            byte[]? sharedPrefix = null;

            // Verify the search key's prefix matches this page's shared
            // prefix; if it doesn't, no entry on this page can match (every
            // canonical key on the page begins with that prefix).
            if (pref > 0)
            {
                if (searchKey.Length < pref)
                {
                    return false;
                }
            }

            int entryStart = FirstEntryOffset;
            bool prefixChecked = false;
            int lastEntryStart = -1;
            while (entryStart < payloadEnd)
            {
                int next = NextEntryStart(page, payloadEnd, entryStart);
                int entryEnd = next < 0 ? payloadEnd : next;
                int totalLen = entryEnd - entryStart;

                // Leaf entry layout: [shared-prefix-stripped key bytes] +
                // 3-byte BE data page + 1-byte data row → key suffix length
                // = totalLen - 4. The first entry on the page is whole; later
                // entries had their leading `pref` bytes stripped.
                int suffixLen = totalLen - 4;
                if (suffixLen < 0)
                {
                    break;
                }

                // Sanity-check the shared prefix against the first entry
                // (whose first `pref` bytes ARE the canonical prefix). Once
                // confirmed against the search key, we never re-check.
                if (!prefixChecked)
                {
                    if (pref > 0)
                    {
                        sharedPrefix = new byte[pref];
                        Buffer.BlockCopy(page, entryStart, sharedPrefix, 0, pref);
                        for (int i = 0; i < pref; i++)
                        {
                            if (searchKey[i] != sharedPrefix[i])
                            {
                                return false;
                            }
                        }
                    }

                    prefixChecked = true;
                }

                // Build the canonical key: prefix + suffix (first entry's
                // canonical key already lives at [entryStart, entryStart+suffixLen);
                // for later entries, the canonical key is sharedPrefix + the
                // suffix bytes at [entryStart, entryStart+suffixLen)).
                int canonicalLen = entryStart == FirstEntryOffset ? suffixLen : pref + suffixLen;
                if (canonicalLen == searchKey.Length
                    && KeyEquals(searchKey, page, entryStart, suffixLen, sharedPrefix, pref, isFirstEntry: entryStart == FirstEntryOffset))
                {
                    return true;
                }

                lastEntryStart = entryStart;
                if (next < 0)
                {
                    break;
                }

                entryStart = next;
            }

            // No match on this page. Check whether the LAST entry on the page
            // equals the search key (canonically) — if not, sibling leaves
            // cannot contain it (entries are sorted globally). Only then walk
            // next_page.
            long nextPageNumber = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(12, 4));
            if (nextPageNumber <= 0 || lastEntryStart < 0)
            {
                return false;
            }

            // Compare last entry's canonical key against searchKey.
            int lastNext = NextEntryStart(page, payloadEnd, lastEntryStart);
            int lastEnd = lastNext < 0 ? payloadEnd : lastNext;
            int lastSuffixLen = lastEnd - lastEntryStart - 4;
            int lastCanonicalLen = lastEntryStart == FirstEntryOffset ? lastSuffixLen : pref + lastSuffixLen;
            if (lastCanonicalLen != searchKey.Length
                || !KeyEquals(searchKey, page, lastEntryStart, lastSuffixLen, sharedPrefix, pref, isFirstEntry: lastEntryStart == FirstEntryOffset))
            {
                return false;
            }

            page = await readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
            currentPage = nextPageNumber;
            if (page[0] != PageTypeLeaf)
            {
                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns the page number of the child whose subtree may contain
    /// <paramref name="searchKey"/>. Each intermediate entry summarises the
    /// LAST entry of its child page, so we pick the first entry whose
    /// canonical key is &gt;= searchKey. If every entry's summary is &lt;
    /// searchKey the key cannot exist anywhere in the subtree.
    /// </summary>
    private static long? SelectChildPage(int pageSize, byte[] page, byte[] searchKey)
    {
        int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(20, 2));
        int payloadEnd = pageSize - BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
        byte[]? sharedPrefix = null;

        int entryStart = FirstEntryOffset;
        bool prefixCaptured = false;
        while (entryStart < payloadEnd)
        {
            int next = NextEntryStart(page, payloadEnd, entryStart);
            int entryEnd = next < 0 ? payloadEnd : next;
            int totalLen = entryEnd - entryStart;

            // Intermediate entry layout: [stripped key bytes] +
            // 3-byte BE data page + 1-byte data row + 4-byte LE child page
            // → suffixLen = totalLen - 8.
            int suffixLen = totalLen - 8;
            if (suffixLen < 0)
            {
                return null;
            }

            if (!prefixCaptured && pref > 0)
            {
                sharedPrefix = new byte[pref];
                Buffer.BlockCopy(page, entryStart, sharedPrefix, 0, pref);
                prefixCaptured = true;
            }

            int cmp = CompareCanonicalKey(
                searchKey,
                page,
                entryStart,
                suffixLen,
                sharedPrefix,
                pref,
                isFirstEntry: entryStart == FirstEntryOffset);

            // searchKey <= summary → this child's subtree may hold a match.
            if (cmp <= 0)
            {
                int childPtrOffset = entryStart + suffixLen + 4;
                return BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(childPtrOffset, 4));
            }

            if (next < 0)
            {
                break;
            }

            entryStart = next;
        }

        // searchKey is greater than every summary on this page → not present.
        return null;
    }

    /// <summary>
    /// Returns the start offset of the next entry on this page (per the
    /// §4.2 entry-start bitmask), or <c>-1</c> when none exists. The bitmask
    /// indexes payload bytes starting at <see cref="FirstEntryOffset"/>; bit
    /// position N (LSB-first within each byte) corresponds to offset
    /// <c>FirstEntryOffset + N</c>.
    /// </summary>
    private static int NextEntryStart(byte[] page, int payloadEnd, int currentStart)
    {
        // Search the bitmask for any bit set strictly after currentStart.
        int searchStart = currentStart - FirstEntryOffset + 1;
        for (int bit = searchStart; bit < payloadEnd - FirstEntryOffset; bit++)
        {
            int byteOff = BitmaskOffset + (bit / 8);
            if (byteOff >= FirstEntryOffset)
            {
                return -1;
            }

            if ((page[byteOff] & (1 << (bit % 8))) != 0)
            {
                int candidate = FirstEntryOffset + bit;
                return candidate < payloadEnd ? candidate : -1;
            }
        }

        return -1;
    }

    /// <summary>
    /// Tests whether the canonical key at <paramref name="entryStart"/> on
    /// <paramref name="page"/> equals <paramref name="searchKey"/>. The
    /// caller has already confirmed the canonical lengths agree.
    /// </summary>
    private static bool KeyEquals(
        byte[] searchKey,
        byte[] page,
        int entryStart,
        int suffixLen,
        byte[]? sharedPrefix,
        int pref,
        bool isFirstEntry)
    {
        if (isFirstEntry || pref == 0)
        {
            // No prefix stripping: searchKey == [entryStart, entryStart+suffixLen).
            for (int i = 0; i < suffixLen; i++)
            {
                if (searchKey[i] != page[entryStart + i])
                {
                    return false;
                }
            }

            return true;
        }

        // searchKey == sharedPrefix ++ [entryStart, entryStart+suffixLen).
        for (int i = 0; i < pref; i++)
        {
            if (searchKey[i] != sharedPrefix![i])
            {
                return false;
            }
        }

        for (int i = 0; i < suffixLen; i++)
        {
            if (searchKey[pref + i] != page[entryStart + i])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Three-way compare between <paramref name="searchKey"/> and the
    /// canonical key at <paramref name="entryStart"/>. Returns negative when
    /// searchKey &lt; canonical, 0 on equality, positive when greater. Used
    /// during intermediate-page descent to pick the first child whose
    /// summary is &gt;= searchKey.
    /// </summary>
    private static int CompareCanonicalKey(
        byte[] searchKey,
        byte[] page,
        int entryStart,
        int suffixLen,
        byte[]? sharedPrefix,
        int pref,
        bool isFirstEntry)
    {
        int canonicalLen = isFirstEntry || pref == 0 ? suffixLen : pref + suffixLen;
        int min = Math.Min(searchKey.Length, canonicalLen);
        for (int i = 0; i < min; i++)
        {
            byte sb = searchKey[i];
            byte cb = isFirstEntry || pref == 0
                ? page[entryStart + i]
                : (i < pref ? sharedPrefix![i] : page[entryStart + (i - pref)]);
            int diff = sb - cb;
            if (diff != 0)
            {
                return diff;
            }
        }

        return searchKey.Length - canonicalLen;
    }

    /// <summary>
    /// Walks the leaf-sibling chain starting at <paramref name="leafPage"/>,
    /// appending the row pointer <c>(dataPage, rowIndex)</c> of every entry
    /// whose canonical key equals <paramref name="searchKey"/>. Stops once
    /// the per-page last canonical key sorts strictly greater than the
    /// search key (further siblings cannot hold a match) or when
    /// <c>next_page</c> is zero.
    /// </summary>
    private static async ValueTask CollectLeafChain(
        Func<long, CancellationToken, ValueTask<byte[]>> readPage,
        int pageSize,
        byte[] leafPage,
        byte[] searchKey,
        List<(long DataPage, int RowIndex)> matches,
        CancellationToken cancellationToken)
    {
        byte[]? page = leafPage;
        while (page != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(20, 2));
            int payloadEnd = pageSize - BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
            byte[]? sharedPrefix = null;

            if (pref > 0 && searchKey.Length < pref)
            {
                return;
            }

            int entryStart = FirstEntryOffset;
            bool prefixChecked = false;
            int lastEntryStart = -1;
            int lastSuffixLen = 0;
            while (entryStart < payloadEnd)
            {
                int next = NextEntryStart(page, payloadEnd, entryStart);
                int entryEnd = next < 0 ? payloadEnd : next;
                int totalLen = entryEnd - entryStart;
                int suffixLen = totalLen - 4;
                if (suffixLen < 0)
                {
                    break;
                }

                if (!prefixChecked)
                {
                    if (pref > 0)
                    {
                        sharedPrefix = new byte[pref];
                        Buffer.BlockCopy(page, entryStart, sharedPrefix, 0, pref);
                        for (int i = 0; i < pref; i++)
                        {
                            if (searchKey[i] != sharedPrefix[i])
                            {
                                return;
                            }
                        }
                    }

                    prefixChecked = true;
                }

                int canonicalLen = entryStart == FirstEntryOffset ? suffixLen : pref + suffixLen;
                if (canonicalLen == searchKey.Length
                    && KeyEquals(searchKey, page, entryStart, suffixLen, sharedPrefix, pref, isFirstEntry: entryStart == FirstEntryOffset))
                {
                    int ptrOffset = entryStart + suffixLen;
                    long dataPage = ((long)page[ptrOffset] << 16) | ((long)page[ptrOffset + 1] << 8) | page[ptrOffset + 2];
                    int rowIndex = page[ptrOffset + 3];
                    matches.Add((dataPage, rowIndex));
                }

                lastEntryStart = entryStart;
                lastSuffixLen = suffixLen;
                if (next < 0)
                {
                    break;
                }

                entryStart = next;
            }

            // If the page's last canonical key sorts strictly greater than
            // searchKey, no sibling leaf can hold further matches (entries
            // are sorted globally). Otherwise, when last == searchKey OR
            // last < searchKey but tail-page may overshoot, walk next_page.
            long nextPageNumber = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(12, 4));
            if (nextPageNumber <= 0 || lastEntryStart < 0)
            {
                return;
            }

            int cmp = CompareCanonicalKey(
                searchKey,
                page,
                lastEntryStart,
                lastSuffixLen,
                sharedPrefix,
                pref,
                isFirstEntry: lastEntryStart == FirstEntryOffset);

            // cmp = searchKey - lastCanonical. If searchKey < lastCanonical
            // (cmp < 0) then sibling leaves only hold larger keys → stop.
            if (cmp < 0)
            {
                return;
            }

            page = await readPage(nextPageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] != PageTypeLeaf)
            {
                return;
            }
        }
    }
}
