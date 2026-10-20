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

/// <summary>Independently checks reopened native private maps against ordered allocation runs.</summary>
/// <remarks>
/// The immutable expected runs must be independently derived, rather than decoded from the map being checked.
/// The caller exclusively owns the reopened decrypted stream and retains its length and bytes throughout validation.
/// Every declared private page is covered exactly once; bitmap pointers must name sequential private allocations.
/// All set bits must match expected members, including bits outside the source allocation and map self-membership.
/// This is deliberately a validator for CompactUsageMapBuilder's private layout, not arbitrary native map layouts.
/// It proves neither the expected runs' global ownership nor source reclamation or complete database validity.
/// </remarks>
internal static class CompactUsageMapValidator
{
    /// <summary>Compares all private map bytes that define geometry, pointers and page membership.</summary>
    /// <param name="format">The preserved verified native format.</param>
    /// <param name="expectedRuns">Independent immutable strictly increasing disjoint allocation runs.</param>
    /// <param name="reopenedDestination">The reopened readable seekable private decrypted destination.</param>
    /// <param name="allocation">The complete map allocation and counts to verify independently.</param>
    /// <param name="budget">Aggregate work, read and fixed-storage admission.</param>
    /// <param name="cancellationToken">Cancels admission, enumeration and every read and bitmap check.</param>
    /// <returns>Exact map membership and private allocation evidence only.</returns>
    /// <exception cref="ArgumentException">The destination is not readable and seekable.</exception>
    /// <exception cref="JetCorruptDataException">Geometry, pointer coverage, expected runs or membership disagree.</exception>
    /// <exception cref="JetLimitationException">An aggregate allowance is exhausted.</exception>
    internal static async ValueTask<CompactUsageMapValidatorResult> ValidateAsync(
        JetFormat format,
        IAsyncEnumerable<CompactPageRun> expectedRuns,
        Stream reopenedDestination,
        CompactUsageMapBuilderResult allocation,
        CompactUsageMapValidatorBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(expectedRuns, nameof(expectedRuns));
        Guard.NotNull(reopenedDestination, nameof(reopenedDestination));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxRuns, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxListedPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxBitmapBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (!reopenedDestination.CanRead || !reopenedDestination.CanSeek)
        {
            throw new ArgumentException("The reopened private map stream must be readable and seekable.", nameof(reopenedDestination));
        }

        long length = reopenedDestination.Length;
        if (length % format.PageSize != 0 || allocation.FirstPageNumber is < 3 or > 0xFFFFFF
            || allocation.Pointer.PageNumber != allocation.FirstPageNumber || allocation.Pointer.RowIndex != 0
            || allocation.PageCount < 1 || allocation.PageCount > 0x1000000L - allocation.FirstPageNumber
            || allocation.FirstPageNumber + allocation.PageCount > length / format.PageSize
            || allocation.ListedPages < 0 || allocation.InputRuns < 0)
        {
            throw Corrupt("The private map allocation has invalid geometry or counts.");
        }

        if (allocation.PageCount > budget.MaxPages || allocation.ListedPages > budget.MaxListedPages
            || allocation.InputRuns > budget.MaxRuns || (2L * format.PageSize) + 4096 > budget.MaxWorkingBytes)
        {
            throw Limit("Private map validation exceeds its admitted pages, members, runs or working storage.");
        }

        IAsyncEnumerator<CompactPageRun> enumerator = expectedRuns.GetAsyncEnumerator(cancellationToken);
        try
        {
            return await new Validator(format, reopenedDestination, allocation, budget, length, enumerator)
                .RunAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { Reason = reason });

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private sealed class Validator(JetFormat format, Stream source, CompactUsageMapBuilderResult allocation, CompactUsageMapValidatorBudget budget, long length, IAsyncEnumerator<CompactPageRun> expected)
    {
        private readonly byte[] rowPage = new byte[format.PageSize];
        private readonly byte[] bitmapPage = new byte[format.PageSize];
        private long readBytes;
        private long bitmapBytes;
        private long members;
        private long runs;
        private long nextMember = -1;
        private long runEnd;
        private long priorEnd;
        private bool finished;

        internal async ValueTask<CompactUsageMapValidatorResult> RunAsync(CancellationToken token)
        {
            await this.AdvanceAsync(token).ConfigureAwait(false);
            await this.ReadPageAsync(allocation.FirstPageNumber, this.rowPage, token).ConfigureAwait(false);
            int start = format.PageSize - format.UsageMapRowSize;
            if (this.rowPage[0] != Constants.PageTypes.Data || this.rowPage[1] != 1
                || BinaryPrimitives.ReadUInt32LittleEndian(this.rowPage.AsSpan(4)) != 0
                || BinaryPrimitives.ReadUInt16LittleEndian(this.rowPage.AsSpan(format.DataPage.NumRows)) != 1
                || BinaryPrimitives.ReadUInt16LittleEndian(this.rowPage.AsSpan(format.DataPage.RowsStart)) != start
                || BinaryPrimitives.ReadUInt16LittleEndian(this.rowPage.AsSpan(2)) != start - format.DataPage.RowsStart - 2)
            {
                throw Corrupt("The private map row page has changed ownership or directory geometry.");
            }

            long pages = 1;
            switch (this.rowPage[start])
            {
                case Constants.UsageMap.InlineMapType:
                    long basePage = BinaryPrimitives.ReadInt32LittleEndian(this.rowPage.AsSpan(start + 1));
                    if (basePage < 0 || basePage % 8 != 0)
                    {
                        throw Corrupt("The private inline map has an invalid native base page.");
                    }

                    await this.CompareBitmapAsync(this.rowPage, start + Constants.UsageMap.InlineBitmapOffset, format.UsageMapRowSize - Constants.UsageMap.InlineBitmapOffset, basePage, token).ConfigureAwait(false);
                    break;
                case Constants.UsageMap.ReferenceMapType:
                    int count = UsageMap.ReferencePointerCount(format.UsageMapRowSize);
                    for (int index = 0; index < count; index++)
                    {
                        token.ThrowIfCancellationRequested();
                        long page = BinaryPrimitives.ReadInt32LittleEndian(this.rowPage.AsSpan(start + 1 + (index * 4)));
                        if (page == 0)
                        {
                            continue;
                        }

                        if (pages >= allocation.PageCount || page != allocation.FirstPageNumber + pages)
                        {
                            throw Corrupt("A private reference bitmap is aliased, outside its allocation or out of order.");
                        }

                        await this.ReadPageAsync(page, this.bitmapPage, token).ConfigureAwait(false);
                        if (this.bitmapPage[0] != Constants.PageTypes.UsageMap || this.bitmapPage[1] != 1
                            || this.bitmapPage[2] != 0 || this.bitmapPage[3] != 0)
                        {
                            throw Corrupt("A private reference bitmap has invalid native page geometry.");
                        }

                        long before = this.members;
                        await this.CompareBitmapAsync(this.bitmapPage, Constants.UsageMap.ReferenceMapBitmapOffset, format.PageSize - Constants.UsageMap.ReferenceMapBitmapOffset, (long)index * UsageMap.PagesPerReferenceMapPage(format.PageSize), token).ConfigureAwait(false);
                        if (this.members == before)
                        {
                            throw Corrupt("A private reference allocation contains an unnecessary empty bitmap page.");
                        }

                        pages++;
                    }

                    break;
                default:
                    throw Corrupt("The private map has an unknown native map type.");
            }

            token.ThrowIfCancellationRequested();
            if (!this.finished || pages != allocation.PageCount || this.members != allocation.ListedPages
                || this.runs != allocation.InputRuns || source.Length != length)
            {
                throw Corrupt("Private map membership, allocation coverage or declared counts disagree.");
            }

            return new(pages, this.members, this.runs, this.readBytes, this.bitmapBytes);
        }

        private async ValueTask CompareBitmapAsync(byte[] bytes, int offset, int count, long basePage, CancellationToken token)
        {
            for (int index = 0; index < count; index++)
            {
                token.ThrowIfCancellationRequested();
                if (this.bitmapBytes >= budget.MaxBitmapBytes)
                {
                    throw Limit("Private map validation exceeds MaxBitmapBytes.");
                }

                this.bitmapBytes++;
                int bits = bytes[offset + index];
                for (int bit = 0; bit < 8; bit++)
                {
                    if ((bits & (1 << bit)) == 0)
                    {
                        continue;
                    }

                    long page = basePage + ((long)index * 8) + bit;
                    if (this.finished || page != this.nextMember || page <= 0 || page >= allocation.FirstPageNumber)
                    {
                        throw Corrupt("A private map member differs from the independent allocation runs.");
                    }

                    if (this.members >= budget.MaxListedPages)
                    {
                        throw Limit("Private map validation exceeds MaxListedPages.");
                    }

                    this.members++;
                    this.nextMember++;
                    if (this.nextMember == this.runEnd)
                    {
                        await this.AdvanceAsync(token).ConfigureAwait(false);
                    }
                }
            }
        }

        private async ValueTask AdvanceAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            bool moved = await expected.MoveNextAsync().ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            if (!moved)
            {
                this.finished = true;
                return;
            }

            if (this.runs >= budget.MaxRuns)
            {
                throw Limit("Private map validation exceeds MaxRuns.");
            }

            CompactPageRun run = expected.Current;
            if (run.FirstPageNumber <= 0 || run.FirstPageNumber < this.priorEnd || run.PageCount <= 0
                || run.FirstPageNumber >= allocation.FirstPageNumber || run.PageCount > allocation.FirstPageNumber - run.FirstPageNumber)
            {
                throw Corrupt("Independent map allocation runs are invalid, overlapping or out of order.");
            }

            this.runs++;
            this.nextMember = run.FirstPageNumber;
            this.runEnd = run.FirstPageNumber + run.PageCount;
            this.priorEnd = this.runEnd;
        }

        private async ValueTask ReadPageAsync(long page, byte[] buffer, CancellationToken token)
        {
            source.Position = page * format.PageSize;
            int offset = 0;
            while (offset < buffer.Length)
            {
                token.ThrowIfCancellationRequested();
                int request = buffer.Length - offset;
                if (request > budget.MaxReadBytes - this.readBytes)
                {
                    throw Limit("Private map validation exceeds MaxReadBytes.");
                }

                this.readBytes += request;
                int read = await source.ReadAsync(buffer.AsMemory(offset, request), token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                if (read <= 0 || read > request)
                {
                    throw Corrupt("A private map page is truncated or returned an invalid read count.");
                }

                offset += read;
            }
        }
    }
}
