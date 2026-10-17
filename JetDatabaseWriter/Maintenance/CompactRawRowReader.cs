namespace JetDatabaseWriter.Maintenance;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Streams stored rows from physical owner headers without decoding fields or constructing table snapshots.</summary>
/// <remarks>
/// The caller must keep the source immutable throughout the scan. Matching owner headers establish the scan's coverage,
/// not complete ownership or permission to reclaim storage. Maps, missing pages, values and incoming aliases are not proven.
/// </remarks>
internal static class CompactRawRowReader
{
    /// <summary>Visits each discoverable live row, preserving its header bookmark and exact stored payload bytes.</summary>
    /// <param name="pages">The immutable decrypted physical page source; its caches are not populated.</param>
    /// <param name="format">The source's verified format.</param>
    /// <param name="ownerTDefPage">The table definition page named by data-page owner headers.</param>
    /// <param name="budget">Limits for this invocation, including repeated overflow reads.</param>
    /// <param name="visitor">Consumes the payload before returning; it must not retain the borrowed memory.</param>
    /// <param name="cancellationToken">Cancels reads, directory traversal and visitor delivery.</param>
    /// <returns>A task completing after the last row visitor and all scan checks succeed.</returns>
    /// <exception cref="JetLimitationException">Storage admission or traversal exceeds its allowance.</exception>
    /// <exception cref="JetCorruptDataException">A matching page or reachable row cannot be traversed faithfully.</exception>
    internal static ValueTask ScanAsync(
        IPageSource pages,
        JetFormat format,
        long ownerTDefPage,
        CompactRawRowBudget budget,
        Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(visitor, nameof(visitor));
        Guard.InRange(budget.MaxPages, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxPageReads, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRows, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxDirectorySlots, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOverflowHops, 1, Constants.DataPage.MaxOverflowHops, nameof(budget));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        long pageCount = pages.PageCount;
        if (pageCount > budget.MaxPages)
        {
            throw Limit(0, "The source page count exceeds MaxPages.");
        }

        if (pages.PageSize != format.PageSize || pageCount < 3 || ownerTDefPage <= 0 || ownerTDefPage >= pageCount)
        {
            throw Corrupt(ownerTDefPage, "The raw-row source geometry or table owner is invalid.");
        }

        // Two fixed directories, the physical page plus at most two overflow
        // buffers during a read, and conservative fixed object/async scratch.
        // Runtime overhead, shared-pool retention and visitor allocations are separate.
        int capacity = (format.PageSize - format.DataPage.RowsStart) / 2;
        long reservation = (3L * format.PageSize) + (16L * capacity) + (64 * 1024);
        if (reservation > budget.MaxWorkingBytes)
        {
            throw Limit(0, "The fixed raw-row storage reservation exceeds MaxWorkingBytes.");
        }

        return new Scanner(pages, format, pageCount, ownerTDefPage, budget, visitor, capacity).RunAsync(cancellationToken);
    }

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private sealed class Scanner(
        IPageSource pages,
        JetFormat format,
        long admittedPageCount,
        long owner,
        CompactRawRowBudget budget,
        Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor,
        int capacity)
    {
        private readonly long pageCount = admittedPageCount;
        private readonly Directory physicalDirectory = new(capacity);
        private readonly Directory overflowDirectory = new(capacity);
        private readonly long[] seenPages = new long[Constants.DataPage.MaxOverflowHops + 1];
        private readonly int[] seenRows = new int[Constants.DataPage.MaxOverflowHops + 1];
        private long pageReads;
        private long rows;
        private long directorySlots;

        internal async ValueTask RunAsync(CancellationToken cancellationToken)
        {
            // The first three format bootstrap pages are the header, global
            // usage-map storage and catalog definition, never table data pages.
            for (long number = 3; number < this.pageCount; number++)
            {
                byte[] page = await this.ReadAsync(number, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (Ri32(page, format.DataPage.TDefOff) != owner)
                    {
                        continue;
                    }

                    if (page[0] != Constants.PageTypes.Data)
                    {
                        if (page[0] is not 0 and not Constants.PageTypes.TableDefinition
                            and not Constants.PageTypes.IndexIntermediate and not Constants.PageTypes.IndexLeaf
                            and not Constants.PageTypes.UsageMap and not Constants.PageTypes.Freed)
                        {
                            throw Corrupt(number, "An unknown page type retains this table's owner header.");
                        }

                        continue;
                    }

                    this.LoadDirectory(this.physicalDirectory, page, number, cancellationToken);
                    for (int row = 0; row < this.physicalDirectory.Count; row++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        int raw = this.physicalDirectory.Raw[row];
                        if ((raw & Constants.DataPage.DeletedRowFlag) != 0)
                        {
                            continue;
                        }

                        if (this.rows >= budget.MaxRows)
                        {
                            throw Limit(number, "The live-row count exceeds MaxRows.");
                        }

                        this.rows++;
                        RowBound bound = this.physicalDirectory.GetBound(row, format.PageSize);
                        if ((raw & Constants.DataPage.OverflowRowFlag) != 0)
                        {
                            await this.VisitOverflowAsync(number, page, bound, cancellationToken).ConfigureAwait(false);
                        }
                        else
                        {
                            await visitor(new RowLocation(number, row, bound.RowStart, bound.RowSize), page.AsMemory(bound.RowStart, bound.RowSize), cancellationToken).ConfigureAwait(false);
                        }

                        pages.ThrowIfDisposedOrCancelled(cancellationToken);
                        this.CheckGeometry(number);
                    }
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }

            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            this.CheckGeometry(owner);
        }

        private async ValueTask VisitOverflowAsync(long headerNumber, byte[] headerPage, RowBound header, CancellationToken cancellationToken)
        {
            byte[] current = headerPage;
            RowBound pointer = header;
            long currentNumber = headerNumber;
            this.seenPages[0] = headerNumber;
            this.seenRows[0] = header.RowIndex;
            try
            {
                for (int hop = 0; hop < budget.MaxOverflowHops; hop++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (pointer.RowSize < Constants.DataPage.OverflowPointerSize)
                    {
                        throw Corrupt(currentNumber, $"Overflow slot {pointer.RowIndex} is shorter than four bytes.");
                    }

                    int targetRow = current[pointer.RowStart];
                    long targetPage = current[pointer.RowStart + 1] | (current[pointer.RowStart + 2] << 8) | (current[pointer.RowStart + 3] << 16);
                    if (targetPage <= 0 || targetPage >= this.pageCount)
                    {
                        throw Corrupt(currentNumber, $"Overflow slot {pointer.RowIndex} targets a page outside the source.");
                    }

                    for (int seen = 0; seen <= hop; seen++)
                    {
                        if (this.seenPages[seen] == targetPage && this.seenRows[seen] == targetRow)
                        {
                            throw Corrupt(currentNumber, $"Overflow slot {pointer.RowIndex} closes a row-pointer cycle.");
                        }
                    }

                    this.seenPages[hop + 1] = targetPage;
                    this.seenRows[hop + 1] = targetRow;
                    byte[] next = await this.ReadAsync(targetPage, cancellationToken).ConfigureAwait(false);
                    if (!ReferenceEquals(current, headerPage))
                    {
                        PageBuffers.Return(current);
                    }

                    current = next;
                    currentNumber = targetPage;
                    if (current[0] != Constants.PageTypes.Data || Ri32(current, format.DataPage.TDefOff) != owner)
                    {
                        throw Corrupt(targetPage, "The overflow target is not a data page of this table.");
                    }

                    this.LoadDirectory(this.overflowDirectory, current, targetPage, cancellationToken);
                    if (targetRow >= this.overflowDirectory.Count)
                    {
                        throw Corrupt(targetPage, $"Overflow target slot {targetRow} does not exist.");
                    }

                    if ((this.overflowDirectory.Raw[targetRow] & Constants.DataPage.DeletedRowFlag) == 0)
                    {
                        throw Corrupt(targetPage, $"Overflow target slot {targetRow} is also a live row identity; its ownership is ambiguous.");
                    }

                    pointer = this.overflowDirectory.GetBound(targetRow, format.PageSize);
                    if (pointer.RowSize <= 0 || pointer.RowStart < format.DataPage.RowsStart + (this.overflowDirectory.Count * 2))
                    {
                        throw Corrupt(targetPage, $"Overflow target slot {targetRow} has no stored payload.");
                    }

                    // Access marks the moved payload deleted. It remains the
                    // original header's row and is never emitted independently.
                    if ((this.overflowDirectory.Raw[targetRow] & Constants.DataPage.OverflowRowFlag) == 0)
                    {
                        var location = new RowLocation(headerNumber, header.RowIndex, pointer.RowStart, pointer.RowSize)
                        {
                            DataPageNumber = targetPage,
                            DataRowIndex = targetRow,
                        };
                        await visitor(location, current.AsMemory(pointer.RowStart, pointer.RowSize), cancellationToken).ConfigureAwait(false);
                        return;
                    }
                }

                throw Limit(currentNumber, "The overflow traversal exceeds MaxOverflowHops.");
            }
            finally
            {
                if (!ReferenceEquals(current, headerPage))
                {
                    PageBuffers.Return(current);
                }
            }
        }

        private async ValueTask<byte[]> ReadAsync(long number, CancellationToken cancellationToken)
        {
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            this.CheckGeometry(number);
            if (this.pageReads >= budget.MaxPageReads)
            {
                throw Limit(number, "The aggregate physical and overflow reads exceed MaxPageReads.");
            }

            this.pageReads++;
            byte[] page = await pages.ReadUncachedPageAsync(number, cancellationToken).ConfigureAwait(false);
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                this.CheckGeometry(number);
                if (page.Length < format.PageSize)
                {
                    throw Corrupt(number, "The source returned a truncated physical page.");
                }

                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        private void CheckGeometry(long page)
        {
            if (pages.PageSize != format.PageSize || pages.PageCount != this.pageCount)
            {
                throw Corrupt(page, "The source geometry changed during raw-row traversal.");
            }
        }

        private void LoadDirectory(Directory directory, byte[] page, long number, CancellationToken cancellationToken)
        {
            int count = Ru16(page, format.DataPage.NumRows);
            if (count > directory.Raw.Length)
            {
                throw Corrupt(number, "The declared directory count exceeds the physical row-directory capacity.");
            }

            int end = format.DataPage.RowsStart + (count * 2);
            for (int row = 0; row < count; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (this.directorySlots >= budget.MaxDirectorySlots)
                {
                    throw Limit(number, "The aggregate inspected row slots exceed MaxDirectorySlots.");
                }

                this.directorySlots++;
                int raw = Ru16(page, format.DataPage.RowsStart + (row * 2));
                if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0)
                {
                    throw Corrupt(number, $"Row slot {row} has an unsupported directory flag.");
                }

                int start = raw & Constants.DataPage.RowOffsetMask;
                bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
                if ((!deleted && (start < end || start >= format.PageSize))
                    || (deleted && start != 0 && (start < end || start > format.PageSize)))
                {
                    throw Corrupt(number, $"Row slot {row} has an invalid stored offset {start}.");
                }

                directory.Raw[row] = raw;
                directory.Sorted[row] = (start << 16) | row;
            }

            directory.Count = count;
            Array.Sort(directory.Sorted, 0, count);
            int previousLiveStart = -1;
            for (int index = 0; index < count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int row = directory.Sorted[index] & 0xFFFF;
                if ((directory.Raw[row] & Constants.DataPage.DeletedRowFlag) == 0)
                {
                    int start = directory.Sorted[index] >> 16;
                    if (start == previousLiveStart)
                    {
                        throw Corrupt(number, $"Row slot {row} aliases another live row's stored payload.");
                    }

                    previousLiveStart = start;
                }
            }
        }
    }

    private sealed class Directory(int capacity)
    {
        internal int[] Raw { get; } = new int[capacity];

        internal int[] Sorted { get; } = new int[capacity];

        internal int Count { get; set; }

        internal RowBound GetBound(int row, int pageSize)
        {
            int start = this.Raw[row] & Constants.DataPage.RowOffsetMask;
            int low = 0;
            int high = this.Count;
            while (low < high)
            {
                int middle = low + ((high - low) / 2);
                if ((this.Sorted[middle] >> 16) <= start)
                {
                    low = middle + 1;
                }
                else
                {
                    high = middle;
                }
            }

            int end = low < this.Count ? this.Sorted[low] >> 16 : pageSize;
            return new RowBound(row, start, end - start);
        }
    }
}
