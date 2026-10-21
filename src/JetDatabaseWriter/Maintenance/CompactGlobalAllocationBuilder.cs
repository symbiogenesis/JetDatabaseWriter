namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>Streams a private disk-backed ledger covering the entire retained source and every appended allocation.</summary>
/// <remarks>
/// The complete source prefix remains pinned regardless of its original free map. Appended records must cover every
/// remaining destination page exactly once in physical order, including map rows and bitmap storage themselves.
/// This ledger is a private sidecar, not a native global free map. It neither reads nor changes destination bytes,
/// installs page-one maps, proves owner identities or incoming references, reclaims storage nor permits publication.
/// The caller exclusively owns immutable destination geometry and immutable allocation evidence from separately
/// validated staging operations. Ledger and destination must have distinct backing storage, including through wrappers.
/// Failures retain partial private ledger output and never return completed evidence.
/// </remarks>
internal static class CompactGlobalAllocationBuilder
{
    /// <summary>Writes and flushes a complete allocation ledger without editing or freeing any destination page.</summary>
    /// <param name="format">The verified preserved native page geometry.</param>
    /// <param name="pinnedSourcePages">The complete original source page count, all retained from page zero.</param>
    /// <param name="appendedAllocations">Immutable independently obtained increasing allocations, excluding the pinned prefix.</param>
    /// <param name="privateDestination">The separately owned immutable seekable destination whose complete length is accounted for.</param>
    /// <param name="privateLedger">The separate empty writable seekable private sidecar.</param>
    /// <param name="budget">Whole-operation record, page, output and fixed-storage limits.</param>
    /// <param name="cancellationToken">Cancels admission, enumeration, every write and flushing.</param>
    /// <returns>Completed sidecar geometry requiring independent reopened validation.</returns>
    /// <exception cref="ArgumentException">A stream is unsuitable, aliased or the ledger is not empty.</exception>
    /// <exception cref="JetCorruptDataException">Geometry, roles, owners or exact page coverage disagree.</exception>
    /// <exception cref="JetLimitationException">A cumulative allowance is exhausted.</exception>
    internal static async ValueTask<CompactGlobalAllocationBuilderResult> BuildAsync(
        JetFormat format,
        long pinnedSourcePages,
        IAsyncEnumerable<CompactGlobalAllocationRecord> appendedAllocations,
        Stream privateDestination,
        Stream privateLedger,
        CompactGlobalAllocationBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(appendedAllocations, nameof(appendedAllocations));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(privateLedger, nameof(privateLedger));
        Guard.NotNull(budget, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxRecords, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxLedgerBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (!privateDestination.CanSeek || !privateLedger.CanSeek || !privateLedger.CanWrite
            || Aliases(privateDestination, privateLedger) || privateLedger.Length != 0)
        {
            throw new ArgumentException("The destination must be seekable and its separate private ledger must be writable, seekable and empty.", nameof(privateLedger));
        }

        long length = privateDestination.Length;
        long pages = length / format.PageSize;
        if (length % format.PageSize != 0 || pinnedSourcePages < 3 || pinnedSourcePages > pages || pages > 0x1000000)
        {
            throw Corrupt("The global private destination or pinned prefix has invalid native page geometry.");
        }

        if (pages > budget.MaxPages || budget.MaxWorkingBytes < 4096 || budget.MaxLedgerBytes < 40)
        {
            throw Limit("Global allocation construction exceeds MaxPages, MaxWorkingBytes or MaxLedgerBytes.");
        }

        return await new Builder(format, pinnedSourcePages, pages, privateDestination, privateLedger, budget, length)
            .RunAsync(appendedAllocations, cancellationToken).ConfigureAwait(false);
    }

    private static bool Aliases(Stream first, Stream second)
        => ReferenceEquals(first, second) || (first is FileStream firstFile && second is FileStream secondFile
            && string.Equals(firstFile.Name, secondFile.Name, StringComparison.OrdinalIgnoreCase));

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.MalformedValue, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private sealed class Builder(JetFormat format, long pinned, long pages, Stream destination, Stream ledger, CompactGlobalAllocationBudget budget, long length)
    {
        private readonly byte[] header = new byte[40];
        private readonly byte[] recordBytes = new byte[32];
        private long records;
        private long writtenBytes = 40;

        internal async ValueTask<CompactGlobalAllocationBuilderResult> RunAsync(IAsyncEnumerable<CompactGlobalAllocationRecord> allocations, CancellationToken token)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(this.header, 0x31414743);
            BinaryPrimitives.WriteInt32LittleEndian(this.header.AsSpan(4), 1);
            BinaryPrimitives.WriteInt32LittleEndian(this.header.AsSpan(8), format.PageSize);
            BinaryPrimitives.WriteInt64LittleEndian(this.header.AsSpan(16), pinned);
            BinaryPrimitives.WriteInt64LittleEndian(this.header.AsSpan(24), pages);
            ledger.Position = 0;
            await ledger.WriteAsync(this.header.AsMemory(), token).ConfigureAwait(false);
            await this.WriteRecordAsync(new(CompactGlobalAllocationKind.PinnedSource, 0, new(0, pinned)), token).ConfigureAwait(false);
            long nextPage = pinned;
            await foreach (CompactGlobalAllocationRecord record in allocations.WithCancellation(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                if (record.Kind is < CompactGlobalAllocationKind.Data or > CompactGlobalAllocationKind.TableDefinition
                    || record.OwnerPageNumber <= 0 || record.OwnerPageNumber >= pinned
                    || (record.OwnerPageNumber == 1 && record.Kind != CompactGlobalAllocationKind.UsageMap)
                    || record.Pages.FirstPageNumber != nextPage || record.Pages.PageCount <= 0
                    || record.Pages.PageCount > pages - nextPage)
                {
                    throw Corrupt("An appended allocation has invalid role, pinned owner, overlap, gap or destination bounds.");
                }

                await this.WriteRecordAsync(record, token).ConfigureAwait(false);
                nextPage += record.Pages.PageCount;
            }

            token.ThrowIfCancellationRequested();
            if (nextPage != pages || destination.Length != length || ledger.Length != this.writtenBytes)
            {
                throw Corrupt("Global allocation evidence does not cover the complete immutable private destination.");
            }

            BinaryPrimitives.WriteInt64LittleEndian(this.header.AsSpan(32), this.records);
            ledger.Position = 0;
            token.ThrowIfCancellationRequested();
            await ledger.WriteAsync(this.header.AsMemory(), token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            await ledger.FlushAsync(token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            if (destination.Length != length || ledger.Length != this.writtenBytes)
            {
                throw Corrupt("Global allocation geometry changed while completing its private ledger.");
            }

            ledger.Position = this.writtenBytes;
            return new(pinned, pages, this.records, this.writtenBytes);
        }

        private async ValueTask WriteRecordAsync(CompactGlobalAllocationRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (this.records >= budget.MaxRecords || this.recordBytes.Length > budget.MaxLedgerBytes - this.writtenBytes)
            {
                throw Limit("Global allocation construction exceeds MaxRecords or MaxLedgerBytes.");
            }

            BinaryPrimitives.WriteInt32LittleEndian(this.recordBytes, (int)record.Kind);
            BinaryPrimitives.WriteInt64LittleEndian(this.recordBytes.AsSpan(8), record.OwnerPageNumber);
            BinaryPrimitives.WriteInt64LittleEndian(this.recordBytes.AsSpan(16), record.Pages.FirstPageNumber);
            BinaryPrimitives.WriteInt64LittleEndian(this.recordBytes.AsSpan(24), record.Pages.PageCount);
            await ledger.WriteAsync(this.recordBytes.AsMemory(), token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            this.records++;
            this.writtenBytes += this.recordBytes.Length;
        }
    }
}
