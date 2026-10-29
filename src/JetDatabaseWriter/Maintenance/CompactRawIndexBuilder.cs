namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;

/// <summary>Streams sorted stored keys into a separate private, unvalidated index tree.</summary>
/// <remarks>
/// This owns only a contiguous append allocation at the destination EOF, never existing pages,
/// TDEF roots, usage maps or source storage. The caller exclusively owns a private decrypted
/// staging destination and two distinct empty seekable fence streams, preferably disk-backed.
/// All three streams remain caller-owned and must not alias through wrappers or backing files.
/// Emitted pages are decrypted images; the caller must protect staging and separately encode
/// destination page encryption before producing a database file.
/// Only one page group and its copied keys are retained; level fences spill to the scratch streams.
/// Packing uses uncompressed sizes, so relocated bookmarks never rely on old compression lengths.
/// Stored canonical keys are copied verbatim; no collation or semantic keys are regenerated.
/// Any failure can leave partial private output. Neither completion nor flushing validates a
/// database or authorizes source replacement, incoming-reference retargeting or reclamation.
/// </remarks>
internal static class CompactRawIndexBuilder
{
    /// <summary>Appends a bounded tree and flushes the private destination before returning its location.</summary>
    /// <param name="format">The verified destination format, matching the source's preserved format.</param>
    /// <param name="ownerTDefPage">The existing private destination TDEF page.</param>
    /// <param name="entries">Strictly increasing canonical key then relocated bookmark order; keys may be borrowed until the enumerator advances.</param>
    /// <param name="privateDestination">The exclusive private writable seekable decrypted destination, with a page-aligned existing prefix.</param>
    /// <param name="firstFenceStream">The first exclusive empty readable, writable and seekable fence scratch stream.</param>
    /// <param name="secondFenceStream">The second exclusive empty readable, writable and seekable fence scratch stream.</param>
    /// <param name="budget">Limits covering all entries, pages, output, repeated fence I/O and working storage.</param>
    /// <param name="cancellationToken">Cancels input enumeration, packing and every stream operation.</param>
    /// <returns>The completed private allocation, explicitly requiring independent database validation.</returns>
    /// <exception cref="ArgumentException">Stream identity, capabilities or page alignment are invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The owner or append allocation is outside the native page range.</exception>
    /// <exception cref="JetLimitationException">A configured construction limit is exceeded.</exception>
    /// <exception cref="JetCorruptDataException">Raw keys or relocated bookmarks are invalid or out of order.</exception>
    internal static async ValueTask<CompactRawIndexBuilderResult> BuildAsync(JetFormat format, long ownerTDefPage, IAsyncEnumerable<IndexEntry> entries, Stream privateDestination, Stream firstFenceStream, Stream secondFenceStream, CompactRawIndexBuilderBudget budget, CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(entries, nameof(entries));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(firstFenceStream, nameof(firstFenceStream));
        Guard.NotNull(secondFenceStream, nameof(secondFenceStream));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxEntries, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxCanonicalBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxTreePages, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxFenceBytes, 1L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxLevels, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (Aliases(privateDestination, firstFenceStream) || Aliases(privateDestination, secondFenceStream) || Aliases(firstFenceStream, secondFenceStream))
        {
            throw new ArgumentException("The private destination and fence streams must be distinct.", nameof(firstFenceStream));
        }

        if (!privateDestination.CanSeek || !privateDestination.CanWrite || privateDestination.Length % format.PageSize != 0)
        {
            throw new ArgumentException("The private destination must be writable, seekable and page aligned.", nameof(privateDestination));
        }

        ValidateScratch(firstFenceStream, nameof(firstFenceStream));
        ValidateScratch(secondFenceStream, nameof(secondFenceStream));
        long firstPage = privateDestination.Length / format.PageSize;
        if (firstPage is <= 0 or > uint.MaxValue || ownerTDefPage <= 0 || ownerTDefPage >= firstPage)
        {
            throw new ArgumentOutOfRangeException(nameof(ownerTDefPage), "The owner must already exist before the private append allocation.");
        }

        // At most pageSize/4 entries (including empty keys), list capacity growth,
        // copied key bytes, the prior key, one pending entry, codec pages and I/O headers.
        // Input iterator and stream implementations are caller-owned storage.
        if ((128L * format.PageSize) + 65_536 > budget.MaxWorkingBytes)
        {
            throw Limit("The conservative builder storage reservation exceeds MaxWorkingBytes.");
        }

        return await new Builder(format, ownerTDefPage, firstPage, privateDestination, budget).RunAsync(entries, firstFenceStream, secondFenceStream, cancellationToken).ConfigureAwait(false);
    }

    private static bool Aliases(Stream left, Stream right)
        => ReferenceEquals(left, right)
            || (left is FileStream leftFile && right is FileStream rightFile && string.Equals(leftFile.Name, rightFile.Name, StringComparison.OrdinalIgnoreCase));

    private static void ValidateScratch(Stream stream, string name)
    {
        if (!stream.CanSeek || !stream.CanRead || !stream.CanWrite || stream.Length != 0)
        {
            throw new ArgumentException("A fence stream must be empty, readable, writable and seekable.", name);
        }
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private static void Charge(ref long consumed, long amount, long maximum, string name)
    {
        if (amount > maximum - consumed)
        {
            throw Limit($"The raw index build exceeds {name}.");
        }

        consumed += amount;
    }

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

    private static async ValueTask ReadExactlyAsync(Stream stream, byte[] bytes, CancellationToken token)
    {
        int read = await StreamRead.ReadAtLeastAsync(stream, bytes, bytes.Length, throwOnEndOfStream: false, token).ConfigureAwait(false);
        if (read != bytes.Length)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The private fence stream ended before a complete record.");
        }
    }

    private sealed class Builder(JetFormat format, long owner, long firstPage, Stream destination, CompactRawIndexBuilderBudget budget)
    {
        private readonly int capacity = format.PageSize - format.IndexPage.FirstEntryOffset;
        private readonly byte[] fenceHeader = new byte[12];
        private long pageCount;
        private long entryCount;
        private long canonicalBytes;
        private long outputBytes;
        private long fenceBytes;

        internal async ValueTask<CompactRawIndexBuilderResult> RunAsync(IAsyncEnumerable<IndexEntry> entries, Stream inputFences, Stream outputFences, CancellationToken token)
        {
            inputFences.Position = 0;
            outputFences.Position = 0;
            var leaves = new List<IndexEntry>();
            IndexEntry? previous = null;
            int size = 0;
            await foreach (IndexEntry borrowed in entries.WithCancellation(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                Guard.NotNull(borrowed.Key, nameof(entries));
                if (borrowed.DataPage is <= 0 or > 0xFFFFFF)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "A relocated bookmark exceeds the nonzero native 24-bit page range.");
                }

                if (borrowed.Key.Length > this.capacity - 8)
                {
                    throw Limit("A stored key cannot fit both a leaf and an intermediate fence entry.");
                }

                Charge(ref this.entryCount, 1, budget.MaxEntries, nameof(budget.MaxEntries));
                Charge(ref this.canonicalBytes, borrowed.Key.Length + 4L, budget.MaxCanonicalBytes, nameof(budget.MaxCanonicalBytes));
                if (previous.HasValue && Compare(previous.Value, borrowed) >= 0)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "Raw keys and relocated bookmarks must be strictly increasing.");
                }

                // Snapshot before advancing an enumerator that may reuse its source buffer.
                IndexEntry entry = borrowed with { Key = borrowed.Key.AsSpan().ToArray() };
                if (size + entry.Key.Length + 4 > this.capacity)
                {
                    await this.WriteLeafAsync(leaves, more: true, inputFences, token).ConfigureAwait(false);
                    leaves.Clear();
                    size = 0;
                }

                leaves.Add(entry);
                size += entry.Key.Length + 4;
                previous = entry;
            }

            await this.WriteLeafAsync(leaves, more: false, inputFences, token).ConfigureAwait(false);
            leaves.Clear();
            previous = null;
            long leafCount = this.pageCount;
            long lastLeaf = firstPage + leafCount - 1;
            long childCount = leafCount;
            long root = firstPage;
            int levels = 1;
            while (childCount > 1)
            {
                token.ThrowIfCancellationRequested();
                if (levels >= budget.MaxLevels)
                {
                    throw Limit("The raw index build exceeds MaxLevels.");
                }

                inputFences.Position = 0;
                outputFences.SetLength(0);
                outputFences.Position = 0;
                long levelBase = firstPage + this.pageCount;
                var group = new List<DecodedIntermediateEntry>();
                size = 0;
                for (long child = 0; child < childCount; child++)
                {
                    DecodedIntermediateEntry entry = await this.ReadFenceAsync(inputFences, token).ConfigureAwait(false);
                    if (size + entry.Entry.Key.Length + 8 > this.capacity)
                    {
                        await this.WriteIntermediateAsync(group, levelBase, lastLeaf, more: true, outputFences, token).ConfigureAwait(false);
                        group.Clear();
                        size = 0;
                    }

                    group.Add(entry);
                    size += entry.Entry.Key.Length + 8;
                }

                if (inputFences.Position != inputFences.Length)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The private fence stream changed during construction.");
                }

                await this.WriteIntermediateAsync(group, levelBase, lastLeaf, more: false, outputFences, token).ConfigureAwait(false);
                long nextCount = firstPage + this.pageCount - levelBase;
                if (nextCount >= childCount)
                {
                    throw Limit("Stored fence sizes prevent the intermediate level from reducing the tree.");
                }

                root = levelBase;
                childCount = nextCount;
                levels++;
                (inputFences, outputFences) = (outputFences, inputFences);
            }

            token.ThrowIfCancellationRequested();
            await destination.FlushAsync(token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            if (destination.Length != (firstPage + this.pageCount) * format.PageSize)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The private destination allocation changed during its final flush.");
            }

            return new(root, firstPage, lastLeaf, this.pageCount, this.entryCount, levels, this.fenceBytes);
        }

        private async ValueTask WriteLeafAsync(List<IndexEntry> entries, bool more, Stream fences, CancellationToken token)
        {
            long page = this.NextPage(more);
            byte[] bytes = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, owner, entries, page == firstPage ? 0 : page - 1, more ? page + 1 : 0, 0, enablePrefixCompression: true);
            await this.WritePageAsync(page, bytes, token).ConfigureAwait(false);
            if (entries.Count != 0)
            {
                await this.WriteFenceAsync(fences, new(entries[^1], page), token).ConfigureAwait(false);
            }
        }

        private async ValueTask WriteIntermediateAsync(List<DecodedIntermediateEntry> entries, long levelBase, long tail, bool more, Stream fences, CancellationToken token)
        {
            long page = this.NextPage(more);
            byte[] bytes = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, owner, entries, page == levelBase ? 0 : page - 1, more ? page + 1 : 0, tail);
            await this.WritePageAsync(page, bytes, token).ConfigureAwait(false);
            await this.WriteFenceAsync(fences, new(entries[^1].Entry, page), token).ConfigureAwait(false);
        }

        private long NextPage(bool more)
        {
            long page = firstPage + this.pageCount;
            if (page > uint.MaxValue || (more && page == uint.MaxValue))
            {
                throw Limit("The private tree allocation exceeds the native 32-bit page range.");
            }

            return page;
        }

        private async ValueTask WritePageAsync(long page, byte[] bytes, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Charge(ref this.pageCount, 1, budget.MaxTreePages, nameof(budget.MaxTreePages));
            Charge(ref this.outputBytes, format.PageSize, budget.MaxOutputBytes, nameof(budget.MaxOutputBytes));
            long offset = page * format.PageSize;
            if (destination.Length != offset)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The private destination append allocation changed during construction.");
            }

            destination.Position = offset;
            await destination.WriteAsync(bytes.AsMemory(), token).ConfigureAwait(false);
            if (destination.Length != offset + format.PageSize)
            {
                throw new JetOperationException(JetErrorCode.InvalidOperationState, "The private destination did not append one complete index page.");
            }
        }

        private async ValueTask WriteFenceAsync(Stream stream, DecodedIntermediateEntry entry, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Charge(ref this.fenceBytes, 12L + entry.Entry.Key.Length, budget.MaxFenceBytes, nameof(budget.MaxFenceBytes));
            BinaryPrimitives.WriteInt32LittleEndian(this.fenceHeader.AsSpan(0, 4), entry.Entry.Key.Length);
            BinaryPrimitives.WriteUInt32LittleEndian(this.fenceHeader.AsSpan(4, 4), checked((uint)((entry.Entry.DataPage << 8) | entry.Entry.DataRow)));
            BinaryPrimitives.WriteUInt32LittleEndian(this.fenceHeader.AsSpan(8, 4), checked((uint)entry.ChildPage));
            await stream.WriteAsync(this.fenceHeader.AsMemory(), token).ConfigureAwait(false);
            await stream.WriteAsync(entry.Entry.Key.AsMemory(), token).ConfigureAwait(false);
        }

        private async ValueTask<DecodedIntermediateEntry> ReadFenceAsync(Stream stream, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Charge(ref this.fenceBytes, 12, budget.MaxFenceBytes, nameof(budget.MaxFenceBytes));
            await ReadExactlyAsync(stream, this.fenceHeader, token).ConfigureAwait(false);
            int length = BinaryPrimitives.ReadInt32LittleEndian(this.fenceHeader.AsSpan(0, 4));
            uint bookmark = BinaryPrimitives.ReadUInt32LittleEndian(this.fenceHeader.AsSpan(4, 4));
            long child = BinaryPrimitives.ReadUInt32LittleEndian(this.fenceHeader.AsSpan(8, 4));
            if (length < 0 || length > this.capacity - 8 || child < firstPage || child >= firstPage + this.pageCount || (bookmark >> 8) == 0)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The private fence stream contains an invalid bounded entry.");
            }

            Charge(ref this.fenceBytes, length, budget.MaxFenceBytes, nameof(budget.MaxFenceBytes));
            byte[] key = new byte[length];
            await ReadExactlyAsync(stream, key, token).ConfigureAwait(false);
            return new(new(key, bookmark >> 8, (byte)(bookmark & 0xFF)), child);
        }
    }
}
