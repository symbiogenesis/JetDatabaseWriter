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
/// Completion covers one root's intermediate-entry children, intermediate tails, in-page key order
/// and inclusive entry-edge-only separator fences. Separate flags describe complete observed-edge fences,
/// bookmark-inclusive ordering and key routing.
/// Observed leaf links are checked for decreasing keys;
/// a separate terminal flag identifies one closed reciprocal chain containing every observed leaf.
/// Header references and ordering proofs do not establish ownership. This does not validate bookmark target rows,
/// shared roots, allocation maps or opaque incoming references. Every observation remains pinned.
/// Repeated entry-child reachability and every cycle in the observed navigable graph are rejected.
/// Acyclic entry/tail aliases remain observable; every distinct page is read at most once.
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
    internal static async ValueTask ScanAsync(IPageSource pages, JetFormat format, long ownerTDefPage, long rootPage, CompactIndexBudget budget, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(visitor, nameof(visitor));
        Guard.Positive(budget.MaxTreePages, nameof(budget));
        Guard.InRange(budget.MaxEntries, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxCanonicalBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRetainedKeyBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReferences, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxGraphSteps, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRecords, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        long pageCount = pages.PageCount;
        if (pages.PageSize != format.PageSize || ownerTDefPage <= 0 || ownerTDefPage >= pageCount || rootPage <= 0 || rootPage >= pageCount)
        {
            throw Corrupt(rootPage, "The index root, owner or source geometry is invalid.");
        }

        // Include dictionary/list capacity growth, per-page graph state, pending
        // DFS/topological stacks, routing and complete-descendant envelopes,
        // every navigable edge, one pooled page, and key scratch.
        // Each stored entry consumes at least one payload byte even when its
        // prefix includes the complete pointer trailer; each page adds one tail.
        long pageAllowance = Math.Min(budget.MaxTreePages, pageCount - 1);
        long entryAllowance = Math.Min(budget.MaxEntries, pageAllowance * format.PageSize);
        long edgeAllowance = Math.Min(budget.MaxReferences, entryAllowance + pageAllowance);

        // At most one separator buffer per entry child and two boundary buffers
        // per leaf survive a page read. A canonical buffer can span two pages
        // when the shared prefix is reconstructed. Array overhead is included
        // in the per-node allowance; visitor copies remain caller-owned.
        long retainedAllowance = Math.Min(budget.MaxRetainedKeyBytes, 6L * pageAllowance * format.PageSize);
        long storage = (576L * pageAllowance) + (16L * edgeAllowance) + retainedAllowance + (16L * format.PageSize) + 65_536;
        if (storage > budget.MaxWorkingBytes)
        {
            throw Limit(rootPage, "The conservative index storage reservation exceeds MaxWorkingBytes.");
        }

        IDisposable? reservation = budget.Accounting?.ReserveWorkingBytes(storage, rootPage);
        try
        {
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            if (pages.PageCount != pageCount || pages.PageSize != format.PageSize)
            {
                throw Corrupt(rootPage, "The source page geometry changed during index admission.");
            }

            await new Scanner(pages, format, pageCount, ownerTDefPage, rootPage, budget, visitor).RunAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            reservation?.Dispose();
        }
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptIndex, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private sealed class Scanner(IPageSource pages, JetFormat format, long pageCount, long owner, long root, CompactIndexBudget budget, Func<CompactIndexRecord, CancellationToken, ValueTask> visitor)
    {
        private readonly Dictionary<long, Node> discovered = [];
        private readonly Queue<long> pending = new();
        private readonly long pageCount = pageCount;
        private long entries;
        private long canonicalBytes;
        private long references;
        private long graphSteps;
        private long records;
        private long retainedKeyBytes;
        private ReadOnlyMemory<byte>? leafMinimum;
        private ReadOnlyMemory<byte>? leafMaximum;
        private bool emptyLeafObserved;
        private bool bookmarkOrderValidated = true;
        private bool allObservedEdgeFencesValidated = true;
        private bool childMaximumSeparatorsValidated = true;

        private static int CompareKeys(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right)
            => left.Span[..^4].SequenceCompareTo(right.Span[..^4]);

        private static int CompareEntries(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right)
        {
            int key = CompareKeys(left, right);
            return key != 0 ? key : left.Span[^4..].SequenceCompareTo(right.Span[^4..]);
        }

        private static bool IsEmptyRange(ReadOnlyMemory<byte>? lower, ReadOnlyMemory<byte>? upper)
            => lower.HasValue && upper.HasValue && CompareKeys(lower.Value, upper.Value) >= 0;

        private static ReadOnlyMemory<byte>? MaximumLower(ReadOnlyMemory<byte>? left, ReadOnlyMemory<byte>? right)
            => !left.HasValue || (right.HasValue && CompareKeys(left.Value, right.Value) < 0) ? right : left;

        private static ReadOnlyMemory<byte>? MinimumUpper(ReadOnlyMemory<byte>? left, ReadOnlyMemory<byte>? right)
            => !left.HasValue || (right.HasValue && CompareKeys(left.Value, right.Value) > 0) ? right : left;

        private static ReadOnlyMemory<byte>? MaximumEntry(ReadOnlyMemory<byte>? left, ReadOnlyMemory<byte>? right)
            => !left.HasValue || (right.HasValue && CompareEntries(left.Value, right.Value) < 0) ? right : left;

        private static bool IsBitSet(IndexPageLayout layout, byte[] page, int bit)
            => (page[layout.BitmaskOffset + (bit / 8)] & (1 << (bit % 8))) != 0;

        internal async ValueTask RunAsync(CancellationToken cancellationToken)
        {
            this.discovered.Add(root, new(root) { EntryParent = root });
            this.pending.Enqueue(root);
            while (this.pending.Count > 0)
            {
                long current = this.pending.Dequeue();
                this.CheckSource(current, cancellationToken);
                budget.Accounting?.Charge(CompactDiscoveryCharge.IndexPages, 1, current);
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

            List<Node> postorder = this.ValidateGraph(cancellationToken);
            bool observedLeafChainOrdered = this.ValidateLeafLinks(cancellationToken);
            bool observedKeyRoutingValidated = observedLeafChainOrdered && this.ValidateRouting(postorder, cancellationToken);

            await this.EmitAsync(
                this.Record("index-terminal", root) with
                {
                    NavigableGraphAcyclic = true,
                    EntryEdgeFencesValidated = true,
                    AllObservedEdgeFencesValidated = this.allObservedEdgeFencesValidated,
                    ObservedLeafChainOrdered = observedLeafChainOrdered,
                    ObservedKeyRoutingValidated = observedKeyRoutingValidated,
                    BookmarkInclusiveOrderValidated = observedLeafChainOrdered && this.bookmarkOrderValidated,
                    ObservedChildMaximumSeparatorsValidated = this.childMaximumSeparatorsValidated,
                    Coverage = "One root's entry-child/intermediate-tail graph is acyclic with no repeated entry children. Stored keys satisfy in-page order and inclusive separator fences along entry-edge-only paths; all tail edges are excluded from these fences, including paths not proven to be aliases. AllObservedEdgeFencesValidated separately certifies that complete observed leaf-key envelopes satisfy inclusive local fences on every entry-child and intermediate-tail edge, including aliases; false withholds this proof without rejecting uncertain native layouts. ObservedChildMaximumSeparatorsValidated separately compares each separator's exact stored key and bookmark with its child's complete observed leaf maximum, including all child/tail aliases and bookmark tie breakers; empty descendants or any mismatch withhold this proof. ObservedLeafChainOrdered identifies one closed reciprocal chain containing every observed leaf in key order. BookmarkInclusiveOrderValidated additionally certifies bookmark tie breakers for in-page order, entry-edge-only fences and that chain; false withholds that proof without rejecting uncertain native duplicate ordering. ObservedKeyRoutingValidated separately certifies that separator/tail descent for each encoded key present in the observed leaves can start at or before its first matching leaf and continue along that chain; false means this conservative routing proof is incomplete. Bookmark target coverage is identified separately by BookmarkTargetsValidated. Stored-key/row correspondence, missing leaves, shared roots, allocation and incoming-reference ownership remain unverified.",
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
            Node node = this.discovered[current];
            node.IsIntermediate = intermediate;
            node.PreviousPage = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(layout.PrevPageOffset, 4));
            node.NextPage = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(layout.NextPageOffset, 4));
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

                int childBytes = intermediate ? 4 : 0;
                if (previous is { } previousEntry)
                {
                    ReadOnlyMemory<byte> previousStored = previousEntry.AsMemory(0, previousEntry.Length - childBytes);
                    ReadOnlyMemory<byte> currentStored = canonical.AsMemory(0, length - childBytes);
                    if (CompareKeys(previousStored, currentStored) > 0)
                    {
                        throw Corrupt(current, "Stored canonical index keys decrease within a page.");
                    }

                    this.bookmarkOrderValidated &= CompareEntries(previousStored, currentStored) <= 0;
                }

                if (!intermediate)
                {
                    if (!node.Minimum.HasValue)
                    {
                        this.RetainKey(current, canonical.Length, 0);
                        node.Minimum = canonical.AsMemory();
                        node.Maximum = canonical.AsMemory();
                    }
                    else if (CompareEntries(canonical, node.Maximum!.Value) > 0)
                    {
                        // Native equal-key bookmark order is an optional proof.
                        // Retain its actual maximum even when the last entry is smaller.
                        long released = node.Maximum.Value.Equals(node.Minimum.Value) ? 0 : node.Maximum.Value.Length;
                        this.RetainKey(current, canonical.Length, released);
                        node.Maximum = canonical.AsMemory();
                    }
                }

                await this.EntryAsync(current, ordinal, canonical, intermediate, cancellationToken).ConfigureAwait(false);
                previous = canonical;
                ordinal++;
                start = next;
            }

            if (intermediate && tail != 0)
            {
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
                this.RetainKey(current, canonical.Length, 0);
                this.Schedule(current, child.Value, isChild: true, canonical.AsMemory(0, keyLength + 4));
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

            budget.Accounting?.Charge(CompactDiscoveryCharge.References, 1, current);
            this.references++;
        }

        private void Schedule(long current, long target, bool isChild, ReadOnlyMemory<byte>? separator = null)
        {
            if (target == current || target == root)
            {
                throw Corrupt(current, $"A navigable index reference to page {target} creates a cycle.");
            }

            if (!this.discovered.TryGetValue(target, out Node? node))
            {
                if (this.discovered.Count >= budget.MaxTreePages)
                {
                    throw Limit(current, "The discovered index pages exceed MaxTreePages.");
                }

                node = new(target);
                this.discovered.Add(target, node);
                this.pending.Enqueue(target);
            }

            if (isChild)
            {
                if (node.EntryParent.HasValue)
                {
                    throw Corrupt(current, $"Index page {target} is already an entry child of page {node.EntryParent.Value}; repeated entry-child ownership is ambiguous.");
                }

                node.EntryParent = current;
                node.LowerBound = this.discovered[current].LastSeparator;
                node.UpperBound = separator;
                this.discovered[current].LastSeparator = separator;
                this.discovered[current].EntryChildCount++;
            }

            this.discovered[current].Targets.Add(node);
        }

        private List<Node> ValidateGraph(CancellationToken cancellationToken)
        {
            // A first-discovery parent chain cannot prove this graph acyclic:
            // a tail may alias a node discovered in a different branch. Retain
            // every navigable edge and use iterative gray/black DFS instead.
            var stack = new List<Node> { this.discovered[root] };
            List<Node> postorder = [];
            while (stack.Count > 0)
            {
                Node current = stack[^1];
                this.CheckSource(current.Page, cancellationToken);
                if (current.State == 0)
                {
                    this.GraphStep(current.Page);
                    current.State = 1;
                }

                if (current.NextTarget == current.Targets.Count)
                {
                    this.ValidateFences(current, cancellationToken);
                    current.State = 2;
                    postorder.Add(current);
                    stack.RemoveAt(stack.Count - 1);
                    continue;
                }

                this.GraphStep(current.Page);
                Node target = current.Targets[current.NextTarget++];
                if (target.State == 1)
                {
                    throw Corrupt(current.Page, $"A navigable index reference to active page {target.Page} creates a cycle.");
                }

                if (target.State == 0)
                {
                    stack.Add(target);
                }
            }

            return postorder;
        }

        private void ValidateFences(Node node, CancellationToken cancellationToken)
        {
            this.GraphStep(node.Page);
            node.AllMinimum = node.Minimum;
            node.AllMaximum = node.Maximum;
            for (int i = 0; i < node.Targets.Count; i++)
            {
                this.CheckSource(node.Page, cancellationToken);
                this.GraphStep(node.Page);
                Node child = node.Targets[i];
                bool tail = i == node.EntryChildCount;
                if (!tail)
                {
                    node.Minimum ??= child.Minimum;
                    if (child.Maximum.HasValue)
                    {
                        node.Maximum = child.Maximum;
                    }
                }

                // All descendants are included even when another branch or a
                // tail reaches the same node. Reuse existing canonical buffers;
                // a wider alias envelope may withhold this optional proof.
                node.AllMinimum = MinimumUpper(node.AllMinimum, child.AllMinimum);
                node.AllMaximum = MaximumEntry(node.AllMaximum, child.AllMaximum);
                ReadOnlyMemory<byte>? lower = tail ? node.LastSeparator : child.LowerBound;
                ReadOnlyMemory<byte>? upper = tail ? null : child.UpperBound;
                this.allObservedEdgeFencesValidated &= (!child.AllMinimum.HasValue || !lower.HasValue || CompareKeys(child.AllMinimum.Value, lower.Value) >= 0)
                    && (!child.AllMaximum.HasValue || !upper.HasValue || CompareKeys(child.AllMaximum.Value, upper.Value) <= 0);
            }

            if (node.UpperBound.HasValue)
            {
                this.childMaximumSeparatorsValidated &= node.AllMaximum.HasValue
                    && node.AllMaximum.Value.Span.SequenceEqual(node.UpperBound.Value.Span);
            }

            if ((node.Minimum.HasValue && node.LowerBound.HasValue && CompareKeys(node.Minimum.Value, node.LowerBound.Value) < 0)
                || (node.Maximum.HasValue && node.UpperBound.HasValue && CompareKeys(node.Maximum.Value, node.UpperBound.Value) > 0))
            {
                throw Corrupt(node.Page, "Stored entry-edge-only subtree keys exceed their inclusive separator fences.");
            }

            this.bookmarkOrderValidated &= (!node.Minimum.HasValue || !node.LowerBound.HasValue || CompareEntries(node.Minimum.Value, node.LowerBound.Value) >= 0)
                && (!node.Maximum.HasValue || !node.UpperBound.HasValue || CompareEntries(node.Maximum.Value, node.UpperBound.Value) <= 0);
        }

        private bool ValidateLeafLinks(CancellationToken cancellationToken)
        {
            Node? first = null;
            int leaves = 0;
            bool complete = true;
            foreach (Node node in this.discovered.Values)
            {
                this.CheckSource(node.Page, cancellationToken);
                this.GraphStep(node.Page);
                if (node.IsIntermediate)
                {
                    continue;
                }

                leaves++;
                if (node.PreviousPage == 0)
                {
                    complete &= first is null;
                    first = node;
                }

                if (node.NextPage != 0 && this.discovered.TryGetValue(node.NextPage, out Node? next) && !next.IsIntermediate
                    && node.Maximum.HasValue && next.Minimum.HasValue && CompareKeys(node.Maximum.Value, next.Minimum.Value) > 0)
                {
                    throw Corrupt(node.Page, "Stored canonical keys decrease across an observed leaf sibling link.");
                }
            }

            int visited = 0;
            ReadOnlyMemory<byte>? previousMaximum = null;
            Node? current = first;
            while (current is not null && visited < leaves)
            {
                this.CheckSource(current.Page, cancellationToken);
                this.GraphStep(current.Page);

                // Empty leaves cannot hide a decrease between their nonempty neighbors.
                if (previousMaximum.HasValue && current.Minimum.HasValue && CompareKeys(previousMaximum.Value, current.Minimum.Value) > 0)
                {
                    throw Corrupt(current.Page, "Stored canonical keys decrease along the observed leaf sibling chain.");
                }

                this.bookmarkOrderValidated &= !previousMaximum.HasValue || !current.Minimum.HasValue || CompareEntries(previousMaximum.Value, current.Minimum.Value) <= 0;

                current.PreviousLeafMaximum = previousMaximum;
                this.leafMinimum ??= current.Minimum;
                this.emptyLeafObserved |= !current.Minimum.HasValue;
                previousMaximum = current.Maximum ?? previousMaximum;
                visited++;
                if (current.NextPage == 0)
                {
                    current = null;
                }
                else if (this.discovered.TryGetValue(current.NextPage, out Node? next) && !next.IsIntermediate && next.PreviousPage == current.Page)
                {
                    current = next;
                }
                else
                {
                    return false;
                }
            }

            this.leafMaximum = previousMaximum;
            return complete && current is null && visited == leaves && leaves > 0;
        }

        private bool ValidateRouting(List<Node> postorder, CancellationToken cancellationToken)
        {
            // Search ranges are (lower, upper], comparing encoded keys only:
            // native exact-key descent does not include a bookmark. The hull of
            // incoming ranges over-approximates aliases, so it can withhold a
            // proof but cannot certify a gap by discarding an incoming path.
            this.discovered[root].HasRoutingRange = true;
            bool proven = !this.emptyLeafObserved || !this.leafMaximum.HasValue;
            for (int i = postorder.Count - 1; i >= 0; i--)
            {
                Node node = postorder[i];
                this.CheckSource(node.Page, cancellationToken);
                this.GraphStep(node.Page);
                if (!node.HasRoutingRange)
                {
                    continue;
                }

                if (!node.IsIntermediate)
                {
                    // Landing after any earlier matching key would skip it.
                    // No assertion of corruption follows from a failed proof:
                    // the admitted hull may contain gaps with no stored keys.
                    if (node.PreviousLeafMaximum.HasValue && !this.ContainsNoObservedKeys(node.RoutingLower, node.RoutingUpper)
                        && (!node.RoutingLower.HasValue || CompareKeys(node.PreviousLeafMaximum.Value, node.RoutingLower.Value) > 0))
                    {
                        proven = false;
                    }

                    continue;
                }

                for (int childIndex = 0; childIndex < node.Targets.Count; childIndex++)
                {
                    this.CheckSource(node.Page, cancellationToken);
                    this.GraphStep(node.Page);
                    Node target = node.Targets[childIndex];
                    bool tail = childIndex == node.EntryChildCount;
                    ReadOnlyMemory<byte>? lower = MaximumLower(node.RoutingLower, tail ? node.LastSeparator : target.LowerBound);
                    ReadOnlyMemory<byte>? upper = MinimumUpper(node.RoutingUpper, tail ? null : target.UpperBound);
                    if (!IsEmptyRange(lower, upper))
                    {
                        target.MergeRoutingRange(lower, upper);
                    }
                }

                if (node.Targets.Count == node.EntryChildCount)
                {
                    ReadOnlyMemory<byte>? lower = MaximumLower(node.RoutingLower, node.LastSeparator);
                    proven &= IsEmptyRange(lower, node.RoutingUpper) || this.ContainsNoObservedKeys(lower, node.RoutingUpper);
                }
            }

            return proven;
        }

        private bool ContainsNoObservedKeys(ReadOnlyMemory<byte>? lower, ReadOnlyMemory<byte>? upper)
            => !this.leafMaximum.HasValue
                || (lower.HasValue && CompareKeys(this.leafMaximum.Value, lower.Value) <= 0)
                || (upper.HasValue && this.leafMinimum.HasValue && CompareKeys(this.leafMinimum.Value, upper.Value) > 0);

        private void RetainKey(long current, int length, long released)
        {
            if (length > budget.MaxRetainedKeyBytes - (this.retainedKeyBytes - released))
            {
                throw Limit(current, "The retained separator and boundary buffers exceed MaxRetainedKeyBytes.");
            }

            this.retainedKeyBytes += length - released;
        }

        private void GraphStep(long current)
        {
            if (this.graphSteps >= budget.MaxGraphSteps)
            {
                throw Limit(current, "The index graph inspection exceeds MaxGraphSteps.");
            }

            budget.Accounting?.Charge(CompactDiscoveryCharge.GraphSteps, 1, current);
            this.graphSteps++;
        }

        private void ReserveEntry(long current, int length)
        {
            if (this.entries >= budget.MaxEntries || length > budget.MaxCanonicalBytes - this.canonicalBytes)
            {
                throw Limit(current, "The index entries or reconstructed bytes exceed MaxEntries or MaxCanonicalBytes.");
            }

            budget.Accounting?.Charge(CompactDiscoveryCharge.IndexEntries, 1, current);
            budget.Accounting?.Charge(CompactDiscoveryCharge.IndexCanonicalBytes, length, current);
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

            budget.Accounting?.Charge(CompactDiscoveryCharge.Records, 1, record.PageNumber);
            this.records++;
            await visitor(record, cancellationToken).ConfigureAwait(false);
            this.CheckSource(record.PageNumber, cancellationToken);
        }

        private sealed class Node(long page)
        {
            internal long Page { get; } = page;

            internal long? EntryParent { get; set; }

            internal List<Node> Targets { get; } = [];

            internal byte State { get; set; }

            internal int NextTarget { get; set; }

            internal bool IsIntermediate { get; set; }

            internal long PreviousPage { get; set; }

            internal long NextPage { get; set; }

            internal int EntryChildCount { get; set; }

            internal ReadOnlyMemory<byte>? LowerBound { get; set; }

            internal ReadOnlyMemory<byte>? UpperBound { get; set; }

            internal ReadOnlyMemory<byte>? LastSeparator { get; set; }

            internal ReadOnlyMemory<byte>? Minimum { get; set; }

            internal ReadOnlyMemory<byte>? Maximum { get; set; }

            internal ReadOnlyMemory<byte>? AllMinimum { get; set; }

            internal ReadOnlyMemory<byte>? AllMaximum { get; set; }

            internal ReadOnlyMemory<byte>? PreviousLeafMaximum { get; set; }

            internal bool HasRoutingRange { get; set; }

            internal ReadOnlyMemory<byte>? RoutingLower { get; set; }

            internal ReadOnlyMemory<byte>? RoutingUpper { get; set; }

            internal void MergeRoutingRange(ReadOnlyMemory<byte>? lower, ReadOnlyMemory<byte>? upper)
            {
                if (!this.HasRoutingRange)
                {
                    this.RoutingLower = lower;
                    this.RoutingUpper = upper;
                    this.HasRoutingRange = true;
                    return;
                }

                this.RoutingLower = !this.RoutingLower.HasValue || !lower.HasValue ? null : MinimumUpper(this.RoutingLower, lower);
                this.RoutingUpper = !this.RoutingUpper.HasValue || !upper.HasValue ? null : MaximumLower(this.RoutingUpper, upper);
            }
        }
    }
}
