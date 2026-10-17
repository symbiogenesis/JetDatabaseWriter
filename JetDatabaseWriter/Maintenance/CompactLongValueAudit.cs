namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Observes raw LVAL storage without interpreting scalar values, envelopes, text or OLE content.</summary>
/// <remarks>
/// The caller keeps the source and column immutable. Every defined chain pointer is followed to null even after
/// the declared payload has been covered. Observed storage remains pinned: this is neither a relocation proof
/// nor authority to discard trailing bytes, unreachable rows or unknown incoming references.
/// </remarks>
internal static class CompactLongValueAudit
{
    /// <summary>Visits an entire original column slice, its defined external references and complete referenced rows.</summary>
    /// <param name="pages">The immutable decrypted source, read without populating caches.</param>
    /// <param name="format">The verified source format.</param>
    /// <param name="column">The original MEMO/OLE column slice, including its descriptor and all following bytes.</param>
    /// <param name="budget">Per-invocation traversal and storage limits.</param>
    /// <param name="visitor">Consumes borrowed bytes before returning; it must not retain their memory.</param>
    /// <param name="cancellationToken">Cancels reads, directory inspection and visitor delivery.</param>
    /// <param name="accounting">Optional aggregate accounting; a session accounts physical reads in its page source.</param>
    /// <returns>A task completed only after traversal and the terminal visitor succeed.</returns>
    /// <exception cref="JetLimitationException">Admission or traversal exceeds its budget.</exception>
    /// <exception cref="JetCorruptDataException">A descriptor or referenced row cannot be observed faithfully.</exception>
    internal static ValueTask AuditAsync(
        IPageSource pages,
        JetFormat format,
        ReadOnlyMemory<byte> column,
        CompactLongValueBudget budget,
        Func<CompactLongValueRecord, CancellationToken, ValueTask> visitor,
        CancellationToken cancellationToken,
        CompactDiscoveryAccounting? accounting = null)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(visitor, nameof(visitor));
        Guard.InRange(budget.MaxPages, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPageReads, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxChainRows, 0, int.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxDirectorySlots, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRawBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        long pageCount = pages.PageCount;
        if (pageCount > budget.MaxPages)
        {
            throw Limit(0, "The source page count exceeds MaxPages.");
        }

        if (pages.PageSize != format.PageSize || pageCount < 3)
        {
            throw Corrupt(0, "The raw long-value source geometry is invalid.");
        }

        // A growing HashSet can temporarily retain both bucket/entry arrays during
        // resize. Sixty-four bytes per admitted pointer covers both generations;
        // fixed scratch includes records, async state and up to two pooled pages.
        // Runtime overhead, caller bytes and pool retention are separate.
        long reservation = (64L * budget.MaxChainRows) + (2L * format.PageSize) + (64 * 1024);
        if (reservation > budget.MaxWorkingBytes)
        {
            throw Limit(0, "The raw long-value storage reservation exceeds MaxWorkingBytes.");
        }

        if (column.Length > budget.MaxRawBytes)
        {
            throw Limit(0, "The original column slice exceeds MaxRawBytes.");
        }

        return new Auditor(pages, format, pageCount, budget, visitor, accounting).RunAsync(column, reservation, cancellationToken);
    }

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private sealed class Auditor(
        IPageSource pages,
        JetFormat format,
        long pageCount,
        CompactLongValueBudget budget,
        Func<CompactLongValueRecord, CancellationToken, ValueTask> visitor,
        CompactDiscoveryAccounting? accounting)
    {
        private long rawBytes;
        private long directorySlots;
        private long pageReads;

        internal async ValueTask RunAsync(ReadOnlyMemory<byte> column, long reservation, CancellationToken cancellationToken)
        {
            using IDisposable? admission = accounting?.ReserveWorkingBytes(reservation);
            accounting?.Charge(CompactDiscoveryCharge.LongValues, 1);
            bool parsed = LongValueDescriptor.TryRead(column.Span, out LongValueDescriptor descriptor);
            int payloadOffset = Math.Min(Constants.LongValue.HeaderSize, column.Length);
            int declared = parsed && descriptor.IsInline ? Math.Min(descriptor.Length, column.Length - payloadOffset) : 0;
            var columnRecord = new CompactLongValueRecord
            {
                Kind = "lval-column",
                RawBytes = column,
                PayloadOffset = payloadOffset,
                DeclaredPayloadBytes = declared,
                TrailingBytes = column.Length - payloadOffset - declared,
            };
            await this.VisitAsync(columnRecord, cancellationToken).ConfigureAwait(false);
            if (!parsed)
            {
                throw Corrupt(0, "The long-value column has a truncated descriptor.");
            }

            if (descriptor.StorageMode is not Constants.LongValue.InlineStorageMode
                and not Constants.LongValue.SinglePageStorageMode
                and not Constants.LongValue.ChainedStorageMode)
            {
                throw Corrupt(0, "The long-value descriptor uses a reserved storage mode; its raw slice remains pinned.");
            }

            if (descriptor.IsInline)
            {
                if (descriptor.Length > column.Length - Constants.LongValue.HeaderSize)
                {
                    throw Corrupt(0, "The inline descriptor declares bytes outside its original column slice.");
                }
            }
            else
            {
                await this.VisitExternalAsync(descriptor, cancellationToken).ConfigureAwait(false);
            }

            var terminal = new CompactLongValueRecord
            {
                Kind = "lval-terminal",
                Coverage = "Original column slice and complete rows reached by defined LVAL references, including trailing storage. Incoming references, exclusive ownership and relocation safety are unproven.",
            };
            await this.VisitAsync(terminal, cancellationToken).ConfigureAwait(false);
        }

        private async ValueTask VisitExternalAsync(LongValueDescriptor descriptor, CancellationToken cancellationToken)
        {
            var seen = new HashSet<uint>();
            uint current = descriptor.FirstDp;
            int remaining = descriptor.Length;
            while (current != 0)
            {
                long number = LongValueStore.PageNumber(current);
                accounting?.Charge(CompactDiscoveryCharge.References, 1, number);
                await this.VisitAsync(new CompactLongValueRecord { Kind = "lval-reference", PageNumber = number, Pointer = current }, cancellationToken).ConfigureAwait(false);
                if (number <= 0 || number >= pageCount)
                {
                    throw Corrupt(number, "A long-value reference targets a page outside the source.");
                }

                if (seen.Contains(current))
                {
                    throw Corrupt(number, "The defined long-value references contain a cycle, including possible trailing storage.");
                }

                if (seen.Count >= budget.MaxChainRows)
                {
                    throw Limit(number, "The complete long-value traversal exceeds MaxChainRows.");
                }

                if (this.pageReads >= budget.MaxPageReads)
                {
                    throw Limit(number, "The complete long-value traversal exceeds MaxPageReads.");
                }

                accounting?.Charge(CompactDiscoveryCharge.GraphSteps, 1, number);
                accounting?.Charge(CompactDiscoveryCharge.LongValueRows, 1, number);
                _ = seen.Add(current);
                this.pageReads++;
                byte[] page = await pages.ReadUncachedPageAsync(number, cancellationToken).ConfigureAwait(false);
                try
                {
                    this.CheckSource(cancellationToken);
                    if (page.Length < format.PageSize || !LongValueStore.IsLvalPage(page))
                    {
                        throw Corrupt(number, "A long-value reference does not target a complete LVAL data page.");
                    }

                    (int start, int size) = this.LocateRow(page, number, LongValueStore.RowIndex(current), cancellationToken);
                    int prefix = descriptor.IsSinglePage ? 0 : sizeof(uint);
                    bool shortPrefix = size < prefix;
                    uint next = descriptor.IsSinglePage || shortPrefix ? 0 : Ru32(page, start);
                    int count = Math.Min(remaining, Math.Max(0, size - prefix));
                    var rowRecord = new CompactLongValueRecord
                    {
                        Kind = "lval-row",
                        RawBytes = page.AsMemory(start, size),
                        PageNumber = number,
                        Pointer = current,
                        NextPointer = descriptor.IsSinglePage || shortPrefix ? null : next,
                        RowStart = start,
                        PayloadOffset = Math.Min(prefix, size),
                        DeclaredPayloadBytes = count,
                        TrailingBytes = Math.Max(0, size - prefix - count),
                    };
                    await this.VisitAsync(rowRecord, cancellationToken).ConfigureAwait(false);
                    if (shortPrefix)
                    {
                        throw Corrupt(number, "The chained LVAL row is shorter than its defined next-pointer prefix.");
                    }

                    remaining -= count;
                    current = next;
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }

            if (remaining != 0)
            {
                throw Corrupt(LongValueStore.PageNumber(descriptor.FirstDp), $"The observed LVAL storage is missing {remaining} descriptor-declared byte(s).");
            }
        }

        private (int Start, int Size) LocateRow(byte[] page, long number, int target, CancellationToken cancellationToken)
        {
            int count = Ru16(page, format.DataPage.NumRows);
            if (count > (format.PageSize - format.DataPage.RowsStart) / sizeof(ushort) || target >= count)
            {
                throw Corrupt(number, "The LVAL directory count or referenced row index is invalid.");
            }

            int directoryEnd = format.DataPage.RowsStart + (count * sizeof(ushort));
            if (count > budget.MaxDirectorySlots - this.directorySlots)
            {
                throw Limit(number, "The inspected LVAL directory slots exceed MaxDirectorySlots.");
            }

            accounting?.Charge(CompactDiscoveryCharge.DirectorySlots, count, number);
            this.directorySlots += count;
            int rawTarget = Ru16(page, format.DataPage.RowsStart + (target * sizeof(ushort)));
            int start = rawTarget & Constants.DataPage.RowOffsetMask;
            int end = format.PageSize;
            if ((rawTarget & Constants.DataPage.NonLiveRowFlags) != 0)
            {
                throw Corrupt(number, "The LVAL reference targets a deleted or overflow slot.");
            }

            for (int index = 0; index < count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int raw = Ru16(page, format.DataPage.RowsStart + (index * sizeof(ushort)));
                int offset = raw & Constants.DataPage.RowOffsetMask;
                bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
                if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0
                    || (!deleted && (offset < directoryEnd || offset >= format.PageSize))
                    || (deleted && offset != 0 && (offset < directoryEnd || offset > format.PageSize)))
                {
                    throw Corrupt(number, $"The LVAL directory slot {index} has invalid flags or bounds.");
                }

                if (index != target && !deleted && offset == start)
                {
                    throw Corrupt(number, "The LVAL row aliases another live directory slot.");
                }

                if (offset > start && offset < end)
                {
                    end = offset;
                }
            }

            return (start, end - start);
        }

        private async ValueTask VisitAsync(CompactLongValueRecord record, CancellationToken cancellationToken)
        {
            this.CheckSource(cancellationToken);
            if (record.RawBytes.Length > budget.MaxRawBytes - this.rawBytes)
            {
                throw Limit(record.PageNumber, "The complete observed LVAL storage exceeds MaxRawBytes.");
            }

            accounting?.Charge(CompactDiscoveryCharge.LongValueBytes, record.RawBytes.Length, record.PageNumber);
            accounting?.Charge(CompactDiscoveryCharge.Records, 1, record.PageNumber);
            this.rawBytes += record.RawBytes.Length;
            await visitor(record, cancellationToken).ConfigureAwait(false);
            this.CheckSource(cancellationToken);
        }

        private void CheckSource(CancellationToken cancellationToken)
        {
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            if (pages.PageSize != format.PageSize || pages.PageCount != pageCount)
            {
                throw Corrupt(0, "The source geometry changed during raw long-value traversal.");
            }
        }
    }
}
