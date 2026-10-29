namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>Independently compares a reopened private global ledger against exact complete allocation evidence.</summary>
/// <remarks>
/// Expected allocations must come from independent validated staging evidence, never by decoding this ledger.
/// Every destination page is covered once, with the entire original source prefix explicitly pinned. Comparison
/// verifies each allocation's asserted role and original owner, including appended maps, against that evidence.
/// It does not inspect destination page contents, validate native global free maps, establish ownership or incoming
/// references, authorize reclamation or establish a complete compact database. Both streams and expected records
/// remain immutable and exclusively owned throughout validation, with distinct backing storage through any wrappers.
/// </remarks>
internal static class CompactGlobalAllocationValidator
{
    /// <summary>Checks all sidecar bytes and exact page coverage without changing destination or ledger storage.</summary>
    /// <param name="format">The verified preserved native page geometry.</param>
    /// <param name="pinnedSourcePages">The independently verified original source length in pages.</param>
    /// <param name="expectedAllocations">Independent complete increasing appended allocations, excluding pinned source.</param>
    /// <param name="reopenedDestination">The immutable seekable destination whose length must be covered exactly.</param>
    /// <param name="reopenedLedger">The separate immutable readable seekable complete sidecar.</param>
    /// <param name="allocation">The completed ledger geometry to check independently.</param>
    /// <param name="budget">Aggregate record, page, ledger, read and fixed-storage admission.</param>
    /// <param name="cancellationToken">Cancels admission, enumeration and every read attempt.</param>
    /// <returns>Exact complete geometry and independent evidence correspondence only.</returns>
    /// <exception cref="ArgumentException">A stream is unsuitable or aliases the other stream.</exception>
    /// <exception cref="JetCorruptDataException">Geometry, counts, record identities or exact page coverage disagree.</exception>
    /// <exception cref="JetLimitationException">An aggregate allowance is exhausted.</exception>
    internal static async ValueTask<CompactGlobalAllocationValidatorResult> ValidateAsync(
        JetFormat format,
        long pinnedSourcePages,
        IAsyncEnumerable<CompactGlobalAllocationRecord> expectedAllocations,
        Stream reopenedDestination,
        Stream reopenedLedger,
        CompactGlobalAllocationBuilderResult allocation,
        CompactGlobalAllocationBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(expectedAllocations, nameof(expectedAllocations));
        Guard.NotNull(reopenedDestination, nameof(reopenedDestination));
        Guard.NotNull(reopenedLedger, nameof(reopenedLedger));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxRecords, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxLedgerBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (!reopenedDestination.CanSeek || !reopenedLedger.CanSeek || !reopenedLedger.CanRead || Aliases(reopenedDestination, reopenedLedger))
        {
            throw new ArgumentException("The immutable destination must be seekable and its independent reopened ledger must be readable and seekable.", nameof(reopenedLedger));
        }

        long length = reopenedDestination.Length;
        long pages = length / format.PageSize;
        if (length % format.PageSize != 0 || pinnedSourcePages < 3 || pinnedSourcePages > pages || pages > 0x1000000
            || allocation.PinnedPages != pinnedSourcePages || allocation.DestinationPages != pages
            || allocation.Records < 1 || allocation.Records > pages || allocation.LedgerBytes != 40 + (allocation.Records * 32)
            || reopenedLedger.Length != allocation.LedgerBytes)
        {
            throw Corrupt("The reopened global allocation ledger has invalid complete destination, prefix, count or length geometry.");
        }

        if (pages > budget.MaxPages || allocation.Records > budget.MaxRecords || allocation.LedgerBytes > budget.MaxLedgerBytes || budget.MaxWorkingBytes < 4096)
        {
            throw Limit("Global allocation comparison exceeds its destination, record, ledger or working-storage admission.");
        }

        return await new Validator(format, pinnedSourcePages, pages, reopenedDestination, reopenedLedger, allocation, budget, length)
            .RunAsync(expectedAllocations, cancellationToken).ConfigureAwait(false);
    }

    private static bool Aliases(Stream first, Stream second)
        => ReferenceEquals(first, second) || (first is FileStream firstFile && second is FileStream secondFile
            && string.Equals(firstFile.Name, secondFile.Name, StringComparison.OrdinalIgnoreCase));

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.MalformedValue, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private sealed class Validator(JetFormat format, long pinned, long pages, Stream destination, Stream ledger, CompactGlobalAllocationBuilderResult allocation, CompactGlobalAllocationBudget budget, long length)
    {
        private readonly byte[] header = new byte[40];
        private readonly byte[] recordBytes = new byte[32];
        private long readBytes;
        private long records;

        internal async ValueTask<CompactGlobalAllocationValidatorResult> RunAsync(IAsyncEnumerable<CompactGlobalAllocationRecord> expected, CancellationToken token)
        {
            ledger.Position = 0;
            await this.ReadAsync(this.header, token).ConfigureAwait(false);
            if (BinaryPrimitives.ReadUInt32LittleEndian(this.header) != 0x31414743
                || BinaryPrimitives.ReadInt32LittleEndian(this.header.AsSpan(4)) != 1
                || BinaryPrimitives.ReadInt32LittleEndian(this.header.AsSpan(8)) != format.PageSize
                || BinaryPrimitives.ReadInt32LittleEndian(this.header.AsSpan(12)) != 0
                || BinaryPrimitives.ReadInt64LittleEndian(this.header.AsSpan(16)) != pinned
                || BinaryPrimitives.ReadInt64LittleEndian(this.header.AsSpan(24)) != pages
                || BinaryPrimitives.ReadInt64LittleEndian(this.header.AsSpan(32)) != allocation.Records)
            {
                throw Corrupt("The reopened allocation header disagrees with independently verified native geometry and declared record count.");
            }

            await this.CompareAsync(new(CompactGlobalAllocationKind.PinnedSource, 0, new(0, pinned)), token).ConfigureAwait(false);
            long next = pinned;
            await foreach (CompactGlobalAllocationRecord record in expected.WithCancellation(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                if (record.Kind is < CompactGlobalAllocationKind.Data or > CompactGlobalAllocationKind.TableDefinition
                    || record.OwnerPageNumber < 1 || record.OwnerPageNumber >= pinned
                    || (record.OwnerPageNumber == 1 && record.Kind != CompactGlobalAllocationKind.UsageMap)
                    || record.Pages.FirstPageNumber != next || record.Pages.PageCount <= 0 || record.Pages.PageCount > pages - next)
                {
                    throw Corrupt("Independent global allocation evidence contains an invalid identity, overlap, gap or out-of-bounds range.");
                }

                await this.CompareAsync(record, token).ConfigureAwait(false);
                next += record.Pages.PageCount;
            }

            token.ThrowIfCancellationRequested();
            if (next != pages || this.records != allocation.Records || destination.Length != length || ledger.Length != allocation.LedgerBytes)
            {
                throw Corrupt("Independent allocations do not exactly cover the complete immutable destination and ledger.");
            }

            return new(pinned, pages, this.records, this.readBytes);
        }

        private async ValueTask CompareAsync(CompactGlobalAllocationRecord expected, CancellationToken token)
        {
            if (this.records >= allocation.Records)
            {
                throw Corrupt("An independent global allocation has no complete reopened ledger record.");
            }

            await this.ReadAsync(this.recordBytes, token).ConfigureAwait(false);
            if (BinaryPrimitives.ReadInt32LittleEndian(this.recordBytes) != (int)expected.Kind
                || BinaryPrimitives.ReadInt32LittleEndian(this.recordBytes.AsSpan(4)) != 0
                || BinaryPrimitives.ReadInt64LittleEndian(this.recordBytes.AsSpan(8)) != expected.OwnerPageNumber
                || BinaryPrimitives.ReadInt64LittleEndian(this.recordBytes.AsSpan(16)) != expected.Pages.FirstPageNumber
                || BinaryPrimitives.ReadInt64LittleEndian(this.recordBytes.AsSpan(24)) != expected.Pages.PageCount)
            {
                throw Corrupt("A reopened global allocation differs from its independent role, owner or exact range evidence.");
            }

            this.records++;
        }

        private async ValueTask ReadAsync(byte[] buffer, CancellationToken token)
        {
            int offset = 0;
            while (offset < buffer.Length)
            {
                token.ThrowIfCancellationRequested();
                int request = buffer.Length - offset;
                if (request > budget.MaxReadBytes - this.readBytes)
                {
                    throw Limit("Global allocation comparison exceeds its cumulative read allowance.");
                }

                this.readBytes += request;
                int read = await ledger.ReadAsync(buffer.AsMemory(offset, request), token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                if (read <= 0 || read > request)
                {
                    throw Corrupt("A reopened global allocation header or record is truncated or returned an invalid read count.");
                }

                offset = StreamRead.AdvanceRead(buffer.Length, offset, read);
            }
        }
    }
}
