namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Observes bounded raw index-tree references and canonical keys without trusting allocation hints.</summary>
/// <remarks>
/// Completion covers one root's intermediate-entry children, intermediate tails and in-page key order only.
/// Header sibling/tail references are retained but do not establish ownership. This does not
/// validate bookmark target rows, separator fences, leaf-chain agreement, global key order,
/// shared roots, allocation maps or opaque incoming references. Every observation remains pinned.
/// Duplicate child reachability and first-discovery ancestor cycles are rejected; arbitrary
/// mixed cycles through tail aliases remain unverified, although each page is read at most once.
/// An unrecognized or ambiguous payload refuses the scan instead of authorizing reclamation.
/// </remarks>
internal static class CompactIndexAudit
{
    /// <summary>Streams one tree's raw observations, retaining real I/O failures and cancellation.</summary>
    /// <param name="pages">The stable caller-owned decrypted page source; caller caches are not populated.</param>
    /// <param name="format">The source's verified format.</param>
    /// <param name="ownerTDefPage">The expected table-definition owner.</param>
    /// <param name="rootPage">The native index root.</param>
    /// <param name="budget">Explicit traversal and storage limits.</param>
    /// <param name="visitor">Consumes observations; retained records are caller-owned memory.</param>
    /// <param name="cancellationToken">Cancels reads, bitmap inspection and visitor delivery.</param>
    /// <returns>A task completing after this root's bounded observation succeeds.</returns>
    /// <exception cref="JetCorruptDataException">A native reference, page or payload is malformed or ambiguous.</exception>
    /// <exception cref="JetLimitationException">A traversal or working-storage limit is exceeded.</exception>
    internal static ValueTask ScanAsync(IPageSource pages, JetFormat format, long ownerTDefPage, long rootPage, CompactIndexBudget budget, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(visitor, nameof(visitor));
        Guard.Positive(budget.MaxTreePages, nameof(budget));
        Guard.InRange(budget.MaxEntries, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxCanonicalBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReferences, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxLineageSteps, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRecords, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        long pageCount = pages.PageCount;
        if (pages.PageSize != format.PageSize || ownerTDefPage <= 0 || ownerTDefPage >= pageCount || rootPage <= 0 || rootPage >= pageCount)
        {
            throw Corrupt(rootPage, "The index root, owner or source geometry is invalid.");
        }

        // Include collection capacity growth, pending pages, one pooled page,
        // prefix/current/previous entry copies and per-record scratch. No page
        // decoder constructs a list whose retained keys multiply with prefixes.
        long pageAllowance = Math.Min(budget.MaxTreePages, pageCount - 1);
        long storage = (256L * pageAllowance) + (16L * format.PageSize) + 65_536;
        if (storage > budget.MaxWorkingBytes)
        {
            throw Limit(rootPage, "The conservative index storage reservation exceeds MaxWorkingBytes.");
        }

        return new Scanner(pages, format, pageCount, ownerTDefPage, rootPage, budget, visitor).RunAsync(cancellationToken);
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptIndex, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private sealed class Scanner(IPageSource pages, JetFormat format, long pageCount, long owner, long root, CompactIndexBudget budget, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor)
    {
        private readonly Dictionary<long, long> discovered = [];
        private readonly HashSet<long> children = [];
        private readonly Queue<long> pending = new();
        private readonly long pageCount = pageCount;
        private long entries;
        private long canonicalBytes;
        private long references;
        private long lineageSteps;
        private long records;

        private static bool IsBitSet(IndexPageLayout layout, byte[] page, int bit)
            => (page[layout.BitmaskOffset + (bit / 8)] & (1 << (bit % 8))) != 0;

        internal async ValueTask RunAsync(CancellationToken cancellationToken)
        {
            this.Schedule(root, root, isChild: true);
            while (this.pending.Count > 0)
            {
                long current = this.pending.Dequeue();
                this.CheckSource(current, cancellationToken);
                byte[] page = await pages.ReadUncachedPageAsync(current, cancellationToken).ConfigureAwait(false);
                try
                {
                    this.CheckSource(current, cancellationToken);
                    await this.ScanPageAsync(current, page, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }

            await this.EmitAsync(
                this.Record("index-terminal", root) with
                {
                    Coverage = "One root's intermediate-entry children and intermediate tail references; in-page raw key order. Siblings, bookmark targets, fences, global order, arbitrary mixed tail-alias cycles, allocation and incoming-reference ownership remain unverified.",
                },
                cancellationToken).ConfigureAwait(false);
        }

        private async ValueTask ScanPageAsync(long current, byte[] page, CancellationToken cancellationToken)
        {
            if (page.Length < format.PageSize || (!IndexPageCodec.IsLeaf(page) && !IndexPageCodec.IsIntermediate(page)))
            {
                throw Corrupt(current, "The index page is truncated or has an unexpected page type.");
            }

            if (BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(4, 4)) != owner)
            {
                throw Corrupt(current, "The index page owner does not match its table definition.");
            }

            IndexPageLayout layout = format.IndexPage;
            int end = this.ValidateBoundary(current, page, cancellationToken);
            bool intermediate = IndexPageCodec.IsIntermediate(page);
            int prefixLength = IndexPageCodec.ReadPrefixLength(layout, page);
            long tail = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(layout.TailPageOffset, 4));
            if (end == layout.FirstEntryOffset && (prefixLength != 0 || (intermediate && tail == 0)))
            {
                throw Corrupt(current, "An empty index page has a nonzero prefix or an intermediate page has no child or tail.");
            }

            await this.EmitAsync(this.Record("index-page", current) with { PageType = page[0] }, cancellationToken).ConfigureAwait(false);
            await this.HeaderAsync(current, page, layout.PrevPageOffset, "previous", cancellationToken).ConfigureAwait(false);
            await this.HeaderAsync(current, page, layout.NextPageOffset, "next", cancellationToken).ConfigureAwait(false);
            await this.HeaderAsync(current, page, layout.TailPageOffset, "tail", cancellationToken).ConfigureAwait(false);
            byte[] prefix = [];
            byte[]? previous = null;
            int trailer = intermediate ? 8 : 4;
            int start = layout.FirstEntryOffset;
            int ordinal = 0;
            while (start < end)
            {
                this.CheckSource(current, cancellationToken);
                int next = start + 1;
                while (next < end && !IsBitSet(layout, page, next - layout.FirstEntryOffset))
                {
                    next++;
                }

                int length = next - start + (ordinal == 0 ? 0 : prefixLength);
                if (length < trailer || (ordinal == 0 && prefixLength > length))
                {
                    throw Corrupt(current, "An index entry has an ambiguous prefix or truncated pointer trailer.");
                }

                this.ReserveEntry(current, length);
                byte[] canonical = new byte[length];
                int prefixBytes = ordinal == 0 ? 0 : prefixLength;
                if (prefixBytes > 0)
                {
                    Buffer.BlockCopy(prefix, 0, canonical, 0, prefixBytes);
                }

                Buffer.BlockCopy(page, start, canonical, prefixBytes, next - start);
                if (ordinal == 0)
                {
                    prefix = canonical.AsSpan(0, prefixLength).ToArray();
                }

                if (previous is { } previousEntry && previousEntry.AsSpan(0, previousEntry.Length - trailer).SequenceCompareTo(canonical.AsSpan(0, length - trailer)) > 0)
                {
                    throw Corrupt(current, "Stored canonical index keys decrease within a page.");
                }

                await this.EntryAsync(current, ordinal, canonical, intermediate, cancellationToken).ConfigureAwait(false);
                previous = canonical;
                ordinal++;
                start = next;
            }

            if (intermediate && tail != 0)
            {
                if (tail == current || tail == root)
                {
                    throw Corrupt(current, "An intermediate tail reference cycles to itself or the observed root.");
                }

                this.Schedule(current, tail, isChild: false);
            }
        }

        private int ValidateBoundary(long current, byte[] page, CancellationToken cancellationToken)
        {
            IndexPageLayout layout = format.IndexPage;
            int end = format.PageSize - BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
            int length = end - layout.FirstEntryOffset;
            int bits = (layout.FirstEntryOffset - layout.BitmaskOffset) * 8;
            if (length < 0 || end > format.PageSize || length >= bits)
            {
                throw Corrupt(current, "The index payload bounds exceed the page or entry bitmap.");
            }

            for (int bit = 0; bit < bits; bit++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                bool set = IsBitSet(layout, page, bit);
                if ((bit == 0 && set) || (bit > length && set) || (bit == length && set != (length > 0)))
                {
                    throw Corrupt(current, "The index entry bitmap does not identify an exact payload boundary.");
                }
            }

            return end;
        }

        private async ValueTask EntryAsync(long current, int ordinal, byte[] canonical, bool intermediate, CancellationToken cancellationToken)
        {
            int keyLength = canonical.Length - (intermediate ? 8 : 4);
            long bookmark = (canonical[keyLength] << 16) | (canonical[keyLength + 1] << 8) | canonical[keyLength + 2];
            this.Reference(current, bookmark);
            long? child = intermediate ? BinaryPrimitives.ReadUInt32BigEndian(canonical.AsSpan(keyLength + 4, 4)) : null;
            if (child.HasValue)
            {
                this.Reference(current, child.Value);
            }

            await this.EmitAsync(
                this.Record(intermediate ? "intermediate-entry" : "leaf-entry", current) with
                {
                    Role = "bookmark",
                    EntryIndex = ordinal,
                    CanonicalKey = canonical.AsSpan(0, keyLength).ToArray(),
                    TargetPageNumber = bookmark,
                    TargetRowIndex = canonical[keyLength + 3],
                    ChildPageNumber = child,
                },
                cancellationToken).ConfigureAwait(false);
            if (child.HasValue)
            {
                await this.EmitAsync(this.Record("index-reference", current) with { Role = "child", EntryIndex = ordinal, TargetPageNumber = child }, cancellationToken).ConfigureAwait(false);
                this.Schedule(current, child.Value, isChild: true);
            }
        }

        private async ValueTask HeaderAsync(long current, byte[] page, int offset, string role, CancellationToken cancellationToken)
        {
            long target = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(offset, 4));
            if (target != 0)
            {
                this.Reference(current, target);
                await this.EmitAsync(this.Record("index-reference", current) with { Role = role, TargetPageNumber = target }, cancellationToken).ConfigureAwait(false);
            }
        }

        private void Reference(long current, long target)
        {
            if (target <= 0 || target >= this.pageCount)
            {
                throw Corrupt(current, "An index reference is outside the source page range.");
            }

            if (this.references >= budget.MaxReferences)
            {
                throw Limit(current, "The index references exceed MaxReferences.");
            }

            this.references++;
        }

        private void Schedule(long current, long target, bool isChild)
        {
            if (isChild && this.children.Contains(target))
            {
                throw Corrupt(current, "Repeated index child reachability is cyclic or shared; ownership is ambiguous.");
            }

            if (this.discovered.ContainsKey(target))
            {
                long ancestor = current;
                while (true)
                {
                    if (this.lineageSteps >= budget.MaxLineageSteps)
                    {
                        throw Limit(current, "The index ancestor inspection exceeds MaxLineageSteps.");
                    }

                    this.lineageSteps++;
                    if (ancestor == target)
                    {
                        throw Corrupt(current, "An index reference cycles to a first-discovery ancestor.");
                    }

                    long parent = this.discovered[ancestor];
                    if (parent == ancestor)
                    {
                        break;
                    }

                    ancestor = parent;
                }

                if (isChild)
                {
                    this.children.Add(target);
                }

                return;
            }

            if (this.discovered.Count >= budget.MaxTreePages)
            {
                throw Limit(current, "The discovered index pages exceed MaxTreePages.");
            }

            this.discovered.Add(target, current);
            if (isChild)
            {
                this.children.Add(target);
            }

            this.pending.Enqueue(target);
        }

        private void ReserveEntry(long current, int length)
        {
            if (this.entries >= budget.MaxEntries || length > budget.MaxCanonicalBytes - this.canonicalBytes)
            {
                throw Limit(current, "The index entries or reconstructed bytes exceed MaxEntries or MaxCanonicalBytes.");
            }

            this.entries++;
            this.canonicalBytes += length;
        }

        private void CheckSource(long current, CancellationToken cancellationToken)
        {
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            if (pages.PageCount != this.pageCount || pages.PageSize != format.PageSize)
            {
                throw Corrupt(current, "The source page geometry changed during index observation.");
            }
        }

        private CompactIndexRecord Record(string kind, long page)
            => new() { Kind = kind, PageNumber = page, OwnerTDefPage = owner, RootPageNumber = root };

        private async ValueTask EmitAsync(CompactIndexRecord record, CancellationToken cancellationToken)
        {
            this.CheckSource(record.PageNumber, cancellationToken);
            if (this.records >= budget.MaxRecords)
            {
                throw Limit(record.PageNumber, "The index observations exceed MaxRecords.");
            }

            this.records++;
            await visitor(record, cancellationToken).ConfigureAwait(false);
            this.CheckSource(record.PageNumber, cancellationToken);
        }
    }
}
