namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Independently checks reopened private index storage against immutable original stored-key observations.</summary>
/// <remarks>
/// The caller exclusively owns the reopened decrypted destination, original observations and separately reopened row map,
/// keeping their distinct backing storage immutable throughout validation, including through wrappers. Original entries
/// must come from an independently completed source observation in strict stored-key/bookmark order, never the builder's
/// output or fence scratch. The validator does not regenerate semantic or collation keys. It verifies the builder's
/// private contiguous level layout, every stored key/bookmark and exact child maximum, root, owner and allocation coverage.
/// General native trees may use different layouts. This proves preservation of supplied observations and the private
/// allocation only; missing source entries, stored-key/row semantics, global incoming ownership, database validity,
/// map/TDEF publication, encryption and safe source replacement require separate proofs.
/// </remarks>
internal static class CompactRawIndexValidator
{
    /// <summary>Audits a reopened private tree, then compares every leaf and intermediate entry independently.</summary>
    /// <param name="format">The preserved source and destination format.</param>
    /// <param name="destinationOwner">The existing private table-definition identity.</param>
    /// <param name="destination">The reopened readable seekable decrypted private destination.</param>
    /// <param name="originalEntries">The immutable original stored keys and original live-row bookmarks, borrowed until enumeration advances.</param>
    /// <param name="relocations">The separately reopened, immutable, independently validated complete row ledger.</param>
    /// <param name="allocation">The private builder's declared allocation, treated as untrusted evidence.</param>
    /// <param name="budget">The graph, comparison, read and simultaneous working-storage allowances.</param>
    /// <param name="cancellationToken">Cancels all traversal, comparisons, input enumeration and stream reads.</param>
    /// <returns>Private coverage evidence, without a complete database or source-replacement verdict.</returns>
    /// <exception cref="ArgumentException">A required stream capability is absent.</exception>
    /// <exception cref="JetCorruptDataException">Private geometry, entries, topology or original observations disagree.</exception>
    /// <exception cref="JetLimitationException">A configured work, read or storage allowance is exhausted.</exception>
    internal static async ValueTask<CompactRawIndexValidatorResult> ValidateAsync(
        JetFormat format,
        long destinationOwner,
        Stream destination,
        IAsyncEnumerable<IndexEntry> originalEntries,
        CompactRowRelocationMap relocations,
        CompactRawIndexBuilderResult allocation,
        CompactRawIndexValidatorBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(originalEntries, nameof(originalEntries));
        Guard.NotNull(relocations, nameof(relocations));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.IndexAudit, nameof(budget));
        Guard.Positive(budget.MaxTreePages, nameof(budget));
        Guard.InRange(budget.MaxEntries, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxCanonicalBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 1L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxLevels, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        if (!destination.CanRead || !destination.CanSeek)
        {
            throw new ArgumentException("The reopened private destination must be readable and seekable.", nameof(destination));
        }

        long length = destination.Length;
        if (length % format.PageSize != 0 || allocation.FirstPageNumber is < 3 or > uint.MaxValue
            || destinationOwner <= 0 || destinationOwner >= allocation.FirstPageNumber
            || allocation.PageCount <= 0 || allocation.PageCount > (long)uint.MaxValue + 1 - allocation.FirstPageNumber
            || allocation.FirstPageNumber + allocation.PageCount > length / format.PageSize
            || allocation.LastLeafPageNumber < allocation.FirstPageNumber
            || allocation.LastLeafPageNumber >= allocation.FirstPageNumber + allocation.PageCount
            || allocation.RootPageNumber != allocation.FirstPageNumber + allocation.PageCount - 1
            || allocation.EntryCount < 0 || allocation.Levels <= 0)
        {
            throw Corrupt("The declared private index allocation has invalid geometry or root.");
        }

        if (allocation.PageCount > budget.MaxTreePages || allocation.EntryCount > budget.MaxEntries || allocation.Levels > budget.MaxLevels)
        {
            throw Limit("The declared private index allocation exceeds its page, entry or level allowance.");
        }

        // Two independent page cursors, reconstructed prefix/entry buffers and previous
        // source/destination keys, including array overhead and temporary comparisons.
        long scratch = (64L * format.PageSize) + 65_536;
        if (scratch >= budget.MaxWorkingBytes)
        {
            throw Limit("The fixed index comparison storage exceeds MaxWorkingBytes.");
        }

        CompactIndexBudget audit = AuditBudget(budget, allocation, scratch);
        using IDisposable? admission = audit.Accounting?.ReserveWorkingBytes(scratch, allocation.RootPageNumber);
        var pages = new PrivatePages(format, destination, allocation, length, budget.MaxReadBytes);
        return await new Validator(format, destinationOwner, pages, originalEntries, relocations, allocation, budget)
            .RunAsync(audit, cancellationToken).ConfigureAwait(false);
    }

    private static CompactIndexBudget AuditBudget(CompactRawIndexValidatorBudget budget, CompactRawIndexBuilderResult allocation, long scratch)
    {
        CompactIndexBudget supplied = budget.IndexAudit;
        return new()
        {
            Accounting = supplied.Accounting,
            MaxTreePages = checked((int)Math.Min(allocation.PageCount, supplied.MaxTreePages)),
            MaxEntries = supplied.MaxEntries,
            MaxCanonicalBytes = supplied.MaxCanonicalBytes,
            MaxRetainedKeyBytes = supplied.MaxRetainedKeyBytes,
            MaxReferences = supplied.MaxReferences,
            MaxGraphSteps = supplied.MaxGraphSteps,
            MaxRecords = supplied.MaxRecords,
            MaxWorkingBytes = Math.Min(supplied.MaxWorkingBytes, budget.MaxWorkingBytes - scratch),
        };
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptIndex, reason, new JetErrorInfo { Reason = reason });

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private static bool Same(IndexEntry left, IndexEntry right)
        => left.DataPage == right.DataPage && left.DataRow == right.DataRow && left.Key.AsSpan().SequenceEqual(right.Key);

    private static int Compare(IndexEntry left, IndexEntry right)
    {
        int key = left.Key.AsSpan().SequenceCompareTo(right.Key);
        if (key != 0)
        {
            return key;
        }

        int page = left.DataPage.CompareTo(right.DataPage);
        return page != 0 ? page : left.DataRow.CompareTo(right.DataRow);
    }

    private sealed class Validator(
        JetFormat format,
        long owner,
        PrivatePages pages,
        IAsyncEnumerable<IndexEntry> originals,
        CompactRowRelocationMap relocations,
        CompactRawIndexBuilderResult allocation,
        CompactRawIndexValidatorBudget budget)
    {
        private long canonicalBytes;
        private long leaves;
        private long auditedPages;
        private bool terminal;

        internal async ValueTask<CompactRawIndexValidatorResult> RunAsync(CompactIndexBudget auditBudget, CancellationToken token)
        {
            await CompactIndexAudit.ScanAsync(pages, format, owner, allocation.RootPageNumber, auditBudget, this.ObserveAsync, token).ConfigureAwait(false);
            if (!this.terminal || this.auditedPages != allocation.PageCount)
            {
                throw Corrupt("The reopened root does not cover every declared private index allocation page.");
            }

            await this.CompareLeavesAsync(token).ConfigureAwait(false);
            int levels = await this.CompareLevelsAsync(token).ConfigureAwait(false);
            if (levels != allocation.Levels || this.leaves != allocation.EntryCount)
            {
                throw Corrupt("Independent private tree height or complete entry count disagrees with its declaration.");
            }

            pages.ThrowIfDisposedOrCancelled(token);
            return new(this.leaves, allocation.PageCount, levels, pages.ReadBytes);
        }

        private ValueTask ObserveAsync(CompactIndexRecord record, CancellationToken token)
        {
            pages.ThrowIfDisposedOrCancelled(token);
            if (record.Kind == "index-page")
            {
                this.auditedPages++;
            }
            else if (record.Kind == "index-terminal")
            {
                this.terminal = record.NavigableGraphAcyclic && record.EntryEdgeFencesValidated
                    && record.ObservedLeafChainOrdered && record.BookmarkInclusiveOrderValidated && record.ObservedKeyRoutingValidated;
            }

            return default;
        }

        private async ValueTask CompareLeavesAsync(CancellationToken token)
        {
            await using IAsyncEnumerator<IndexEntry> source = originals.GetAsyncEnumerator(token);
            IndexEntry? previous = null;
            for (long number = allocation.FirstPageNumber; number <= allocation.LastLeafPageNumber; number++)
            {
                byte[] bytes = await pages.ReadPageAsync(number, token).ConfigureAwait(false);
                try
                {
                    this.CheckHeader(bytes, number, allocation.FirstPageNumber, allocation.LastLeafPageNumber, false);
                    var cursor = new EntryCursor(format, bytes, false);
                    bool any = false;
                    while (cursor.MoveNext(token))
                    {
                        IndexEntry actual = cursor.Entry;
                        this.Charge(actual.Key.Length + 4L);
                        if (this.leaves >= allocation.EntryCount || !await source.MoveNextAsync().ConfigureAwait(false))
                        {
                            throw Corrupt("A staged leaf entry has no matching immutable original observation.");
                        }

                        pages.ThrowIfDisposedOrCancelled(token);
                        IndexEntry original = source.Current;
                        Guard.NotNull(original.Key, nameof(originals));
                        this.Charge(original.Key.Length + 4L);
                        if (original.Key.Length > format.PageSize || (previous.HasValue && Compare(previous.Value, original) >= 0))
                        {
                            throw Corrupt("Immutable original observations are not bounded, strictly ordered stored entries.");
                        }

                        IndexEntry expected = await relocations.RelocateAsync(original, token).ConfigureAwait(false);
                        if (!Same(expected, actual))
                        {
                            throw Corrupt("A staged stored key or relocated bookmark differs from its immutable original observation.");
                        }

                        // The enumerator may reuse its key after the next MoveNextAsync.
                        previous = original with { Key = original.Key.AsSpan().ToArray() };
                        this.leaves++;
                        any = true;
                    }

                    if (!any && (allocation.EntryCount != 0 || allocation.PageCount != 1))
                    {
                        throw Corrupt("An empty leaf is present outside the single empty private tree.");
                    }
                }
                finally
                {
                    PageBuffers.Return(bytes);
                }
            }

            if (await source.MoveNextAsync().ConfigureAwait(false))
            {
                throw Corrupt("An immutable original observation is absent from the complete staged leaf allocation.");
            }

            pages.ThrowIfDisposedOrCancelled(token);
        }

        private async ValueTask<int> CompareLevelsAsync(CancellationToken token)
        {
            long lowerFirst = allocation.FirstPageNumber;
            long lowerLast = allocation.LastLeafPageNumber;
            long levelFirst = lowerLast + 1;
            long end = allocation.FirstPageNumber + allocation.PageCount;
            int levels = 1;
            while (levelFirst < end)
            {
                token.ThrowIfCancellationRequested();
                if (levels >= budget.MaxLevels)
                {
                    throw Limit("Independent private index traversal exceeds MaxLevels.");
                }

                long child = lowerFirst;
                long levelLast = levelFirst - 1;
                do
                {
                    levelLast++;
                    if (levelLast >= end)
                    {
                        throw Corrupt("An intermediate level omits part of the preceding private allocation level.");
                    }

                    byte[] bytes = await pages.ReadPageAsync(levelLast, token).ConfigureAwait(false);
                    try
                    {
                        var cursor = new EntryCursor(format, bytes, true);
                        bool any = false;
                        while (cursor.MoveNext(token))
                        {
                            this.Charge(cursor.Entry.Key.Length + 8L);
                            if (child > lowerLast || cursor.Child != child)
                            {
                                throw Corrupt("Intermediate entries do not exclusively cover the preceding private level in allocation order.");
                            }

                            IndexEntry maximum = await this.ReadMaximumAsync(child, levels != 1, token).ConfigureAwait(false);
                            if (!Same(cursor.Entry, maximum))
                            {
                                throw Corrupt("An intermediate stored key or bookmark differs from its independently read child maximum.");
                            }

                            child++;
                            any = true;
                        }

                        if (!any)
                        {
                            throw Corrupt("A private intermediate allocation page has no entry children.");
                        }

                        this.CheckHeader(bytes, levelLast, levelFirst, child > lowerLast ? levelLast : -1, true);
                    }
                    finally
                    {
                        PageBuffers.Return(bytes);
                    }
                }
                while (child <= lowerLast);

                levels++;
                if (levelLast - levelFirst >= lowerLast - lowerFirst)
                {
                    throw Corrupt("The private intermediate allocation level does not reduce its child level.");
                }

                lowerFirst = levelFirst;
                lowerLast = levelLast;
                levelFirst = levelLast + 1;
            }

            if (lowerFirst != lowerLast || lowerFirst != allocation.RootPageNumber)
            {
                throw Corrupt("The private allocation does not terminate in its single declared root.");
            }

            return levels;
        }

        private async ValueTask<IndexEntry> ReadMaximumAsync(long child, bool intermediate, CancellationToken token)
        {
            byte[] bytes = await pages.ReadPageAsync(child, token).ConfigureAwait(false);
            try
            {
                var cursor = new EntryCursor(format, bytes, intermediate);
                IndexEntry? maximum = null;
                while (cursor.MoveNext(token))
                {
                    this.Charge(cursor.Entry.Key.Length + (intermediate ? 8L : 4L));
                    maximum = cursor.Entry;
                }

                return maximum ?? throw Corrupt("An intermediate separator names an empty private child.");
            }
            finally
            {
                PageBuffers.Return(bytes);
            }
        }

        private void CheckHeader(byte[] bytes, long page, long first, long last, bool intermediate)
        {
            if ((intermediate ? !IndexPageCodec.IsIntermediate(bytes) : !IndexPageCodec.IsLeaf(bytes))
                || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(4, 4)) != owner
                || IndexPageCodec.ReadPrevPage(format.IndexPage, bytes) != (page == first ? 0 : page - 1)
                || IndexPageCodec.ReadNextPage(format.IndexPage, bytes) != (page == last ? 0 : page + 1)
                || IndexPageCodec.ReadTailPage(format.IndexPage, bytes) != (intermediate ? allocation.LastLeafPageNumber : 0))
            {
                throw Corrupt("A private allocation page disagrees with its expected type, owner, level siblings or tail.");
            }
        }

        private void Charge(long bytes)
        {
            if (bytes > budget.MaxCanonicalBytes - this.canonicalBytes)
            {
                throw Limit("Independent stored-entry comparison exceeds MaxCanonicalBytes.");
            }

            this.canonicalBytes += bytes;
        }
    }

    /// <summary>Decodes independently of the builder's entry helpers after the raw geometry audit.</summary>
    /// <remarks>Each bounded cursor rechecks prefix and trailer bounds before reconstructing opaque stored bytes.</remarks>
    /// <param name="format">The preserved index layout.</param>
    /// <param name="page">One independently audited private page.</param>
    /// <param name="intermediate">Whether child pointer trailers are expected.</param>
    private sealed class EntryCursor(JetFormat format, byte[] page, bool intermediate)
    {
        private readonly int end = format.PageSize - BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
        private readonly int prefixLength = IndexPageCodec.ReadPrefixLength(format.IndexPage, page);
        private int start = format.IndexPage.FirstEntryOffset;
        private byte[]? prefix;

        internal IndexEntry Entry { get; private set; }

        internal long Child { get; private set; }

        internal bool MoveNext(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (this.start == this.end)
            {
                return false;
            }

            int trailer = intermediate ? 8 : 4;
            int next = this.start + 1;
            while (next < this.end && (page[format.IndexPage.BitmaskOffset + ((next - format.IndexPage.FirstEntryOffset) / 8)] & (1 << ((next - format.IndexPage.FirstEntryOffset) % 8))) == 0)
            {
                token.ThrowIfCancellationRequested();
                next++;
            }

            int shared = this.prefix is null ? 0 : this.prefixLength;
            int length = next - this.start + shared;
            if (this.start > this.end || this.end > format.PageSize || length < trailer || length > format.PageSize * 2
                || this.prefixLength < 0 || (this.prefix is null && this.prefixLength > length))
            {
                throw Corrupt("A reopened private entry has an invalid independent prefix or trailer boundary.");
            }

            byte[] canonical = new byte[length];
            this.prefix?.CopyTo(canonical, 0);
            page.AsSpan(this.start, next - this.start).CopyTo(canonical.AsSpan(shared));
            this.prefix ??= canonical.AsSpan(0, this.prefixLength).ToArray();
            int keyLength = length - trailer;
            long bookmark = (canonical[keyLength] << 16) | (canonical[keyLength + 1] << 8) | canonical[keyLength + 2];
            this.Entry = new(canonical.AsSpan(0, keyLength).ToArray(), bookmark, canonical[keyLength + 3]);
            this.Child = intermediate ? BinaryPrimitives.ReadUInt32BigEndian(canonical.AsSpan(keyLength + 4)) : 0;
            this.start = next;
            return true;
        }
    }

    private sealed class PrivatePages(JetFormat format, Stream stream, CompactRawIndexBuilderResult allocation, long length, long maxReadBytes) : IPageSource
    {
        public int PageSize => format.PageSize;

        public long PageCount => length / format.PageSize;

        public bool IsDisposed => false;

        internal long ReadBytes { get; private set; }

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            this.ThrowIfDisposedOrCancelled(cancellationToken);
            if (pageNumber < allocation.FirstPageNumber || pageNumber >= allocation.FirstPageNumber + allocation.PageCount)
            {
                throw Corrupt("A reopened index edge escapes its declared private page allocation.");
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(format.PageSize);
            try
            {
                stream.Position = pageNumber * format.PageSize;
                int offset = 0;
                while (offset < format.PageSize)
                {
                    this.ThrowIfDisposedOrCancelled(cancellationToken);
                    int requested = format.PageSize - offset;
                    if (requested > maxReadBytes - this.ReadBytes)
                    {
                        throw Limit("Independent private index validation exceeds MaxReadBytes.");
                    }

                    this.ReadBytes += requested;
                    int read = await stream.ReadAsync(buffer.AsMemory(offset, requested), cancellationToken).ConfigureAwait(false);
                    if (read <= 0 || read > requested)
                    {
                        throw Corrupt("The reopened private index ended before a complete allocation page.");
                    }

                    offset += read;
                }

                this.ThrowIfDisposedOrCancelled(cancellationToken);
                return buffer;
            }
            catch
            {
                PageBuffers.Return(buffer);
                throw;
            }
        }

        public void ThrowIfDisposed()
        {
            if (stream.Length != length)
            {
                throw Corrupt("The reopened private destination changed length during index validation.");
            }
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.ThrowIfDisposed();
        }
    }
}
