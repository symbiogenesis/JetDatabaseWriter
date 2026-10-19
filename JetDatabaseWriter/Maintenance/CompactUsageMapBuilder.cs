namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;

/// <summary>Builds one native usage-map row from existing private allocation runs with bounded bitmap storage.</summary>
/// <remarks>
/// The caller exclusively owns a separate decrypted destination and immutable, increasing allocation runs.
/// The map lists exactly those runs; its own newly appended row and bitmap storage is excluded.
/// An INLINE row is retained when one native window fits, otherwise REFERENCE bitmaps are streamed in order.
/// This works for known table, index and column allocations, but does not infer ownership, free-space eligibility,
/// global allocation state or incoming references. The caller must separately assemble those memberships,
/// patch verified TDEF references, preserve pinned storage and validate the complete destination database.
/// No source storage is modified or reclaimed. Failures leave partial private output and never return a result.
/// </remarks>
internal static class CompactUsageMapBuilder
{
    /// <summary>Appends and flushes a private map for strictly increasing, nonoverlapping existing page runs.</summary>
    /// <param name="format">The verified preserved native format.</param>
    /// <param name="runs">Positive runs naming existing pages before the append allocation, in increasing order.</param>
    /// <param name="privateDestination">The exclusive writable, seekable, page-aligned decrypted destination.</param>
    /// <param name="budget">Limits for runs, membership, output and working storage.</param>
    /// <param name="cancellationToken">Cancels enumeration, bitmap construction, writes and flushing.</param>
    /// <returns>A completed native pointer and private allocation, requiring database validation.</returns>
    /// <exception cref="ArgumentException">The destination is not writable, seekable or page aligned.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The destination allocation or a budget is invalid.</exception>
    /// <exception cref="JetCorruptDataException">An input allocation run is invalid, overlapping or out of order.</exception>
    /// <exception cref="JetLimitationException">A configured allowance or native map capacity is exceeded.</exception>
    internal static async ValueTask<CompactUsageMapBuilderResult> BuildAsync(JetFormat format, IAsyncEnumerable<CompactPageRun> runs, Stream privateDestination, CompactUsageMapBuilderBudget budget, CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(runs, nameof(runs));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxRuns, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxListedPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (!privateDestination.CanWrite || !privateDestination.CanSeek || privateDestination.Length % format.PageSize != 0)
        {
            throw new ArgumentException("The private destination must be writable, seekable and page aligned.", nameof(privateDestination));
        }

        long firstPage = privateDestination.Length / format.PageSize;
        if (firstPage is < 3 or > 0xFFFFFF)
        {
            throw new ArgumentOutOfRangeException(nameof(privateDestination), "The map row must follow reserved pages within the native pointer range.");
        }

        long reservation = (2L * format.PageSize) + format.UsageMapRowSize + 4096;
        if (reservation > budget.MaxWorkingBytes)
        {
            throw Limit("The usage-map builder storage reservation exceeds MaxWorkingBytes.");
        }

        return await new Builder(format, firstPage, privateDestination, budget).RunAsync(runs, cancellationToken).ConfigureAwait(false);
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private sealed class Builder(JetFormat format, long firstPage, Stream destination, CompactUsageMapBuilderBudget budget)
    {
        private readonly byte[] rowPage = new byte[format.PageSize];
        private readonly byte[] bitmap = new byte[format.PageSize];
        private readonly byte[] originalInline = new byte[format.UsageMapRowSize];
        private readonly int rowStart = format.PageSize - format.UsageMapRowSize;
        private long pageCount;
        private long listedPages;
        private long inputRuns;
        private long priorEnd;
        private int currentWindow = -1;
        private bool reference;

        internal async ValueTask<CompactUsageMapBuilderResult> RunAsync(IAsyncEnumerable<CompactPageRun> runs, CancellationToken token)
        {
            this.rowPage[0] = Constants.PageTypes.Data;
            this.rowPage[1] = 1;
            BinaryPrimitives.WriteUInt16LittleEndian(this.rowPage.AsSpan(2), checked((ushort)(this.rowStart - format.DataPage.RowsStart - 2)));
            BinaryPrimitives.WriteUInt16LittleEndian(this.rowPage.AsSpan(format.DataPage.NumRows), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(this.rowPage.AsSpan(format.DataPage.RowsStart), checked((ushort)this.rowStart));
            destination.Position = firstPage * format.PageSize;
            await this.AppendPageAsync(this.rowPage, token).ConfigureAwait(false);
            await foreach (CompactPageRun run in runs.WithCancellation(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                if (run.FirstPageNumber <= 0 || run.PageCount <= 0 || run.FirstPageNumber < this.priorEnd
                    || run.FirstPageNumber >= firstPage || run.PageCount > firstPage - run.FirstPageNumber)
                {
                    throw new JetCorruptDataException(JetErrorCode.MalformedValue, "A private map allocation run is invalid, overlapping or out of order.");
                }

                if (this.inputRuns >= budget.MaxRuns || run.PageCount > budget.MaxListedPages - this.listedPages)
                {
                    throw Limit("Usage-map construction exceeds MaxRuns or MaxListedPages.");
                }

                this.inputRuns++;
                this.listedPages += run.PageCount;
                long end = run.FirstPageNumber + run.PageCount;
                for (long page = run.FirstPageNumber; page < end; page++)
                {
                    token.ThrowIfCancellationRequested();
                    if (!this.reference)
                    {
                        if (this.priorEnd == 0 && page == run.FirstPageNumber)
                        {
                            int capacity = (format.UsageMapRowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
                            int basePage = page < capacity ? 0 : UsageMap.AlignInlineBasePage(page);
                            BinaryPrimitives.WriteInt32LittleEndian(this.rowPage.AsSpan(this.rowStart + 1), basePage);
                        }

                        if (UsageMap.TrySetInlinePageState(this.rowPage, this.rowStart, format.UsageMapRowSize, page, isMarked: true))
                        {
                            continue;
                        }

                        await this.PromoteAsync(token).ConfigureAwait(false);
                    }

                    await this.AddReferenceAsync(page, token).ConfigureAwait(false);
                }

                this.priorEnd = end;
            }

            if (this.reference)
            {
                await this.AppendPageAsync(this.bitmap, token).ConfigureAwait(false);
            }

            destination.Position = firstPage * format.PageSize;
            await destination.WriteAsync(this.rowPage.AsMemory(), token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            await destination.FlushAsync(token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            destination.Position = destination.Length;
            return new(new UsageMapPointer(0, checked((int)firstPage)), firstPage, this.pageCount, this.listedPages, this.inputRuns);
        }

        private async ValueTask PromoteAsync(CancellationToken token)
        {
            this.rowPage.AsSpan(this.rowStart, format.UsageMapRowSize).CopyTo(this.originalInline);
            this.rowPage.AsSpan(this.rowStart, format.UsageMapRowSize).Clear();
            this.rowPage[this.rowStart] = Constants.UsageMap.ReferenceMapType;
            this.reference = true;
            int basePage = BinaryPrimitives.ReadInt32LittleEndian(this.originalInline.AsSpan(1));
            int capacity = (format.UsageMapRowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
            for (int bit = 0; bit < capacity; bit++)
            {
                token.ThrowIfCancellationRequested();
                if ((this.originalInline[Constants.UsageMap.InlineBitmapOffset + (bit / 8)] & (1 << (bit % 8))) != 0)
                {
                    await this.AddReferenceAsync((long)basePage + bit, token).ConfigureAwait(false);
                }
            }
        }

        private async ValueTask AddReferenceAsync(long page, CancellationToken token)
        {
            int window = UsageMap.ReferencePointerIndex(format.PageSize, page);
            if (window >= UsageMap.ReferencePointerCount(format.UsageMapRowSize))
            {
                throw Limit("The listed pages exceed the native REFERENCE row capacity.");
            }

            if (window != this.currentWindow)
            {
                if (this.currentWindow >= 0)
                {
                    await this.AppendPageAsync(this.bitmap, token).ConfigureAwait(false);
                }

                this.bitmap.AsSpan().Clear();
                this.bitmap[0] = Constants.PageTypes.UsageMap;
                this.bitmap[1] = 1;
                BinaryPrimitives.WriteInt32LittleEndian(this.rowPage.AsSpan(this.rowStart + 1 + (window * 4)), checked((int)(firstPage + this.pageCount)));
                this.currentWindow = window;
            }

            if (!UsageMap.TrySetReferencePageState(this.bitmap, format.PageSize, page, isMarked: true))
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "A private map page cannot be represented in its reference bitmap.");
            }
        }

        private async ValueTask AppendPageAsync(byte[] bytes, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (this.pageCount >= budget.MaxPages || this.pageCount >= budget.MaxOutputBytes / format.PageSize)
            {
                throw Limit("Usage-map construction exceeds MaxPages or MaxOutputBytes.");
            }

            destination.Position = (firstPage + this.pageCount) * format.PageSize;
            await destination.WriteAsync(bytes.AsMemory(), token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            this.pageCount++;
        }
    }
}
