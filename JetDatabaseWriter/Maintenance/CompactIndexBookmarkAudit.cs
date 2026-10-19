namespace JetDatabaseWriter.Maintenance;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Composes raw index observation with bounded validation of the rows named by stored bookmarks.</summary>
/// <remarks>
/// The source must remain immutable. The ordinary path proves that observed leaf and intermediate bookmarks name live
/// same-owner headers with readable stored payloads, including deleted moved-row slots. An optional trusted resolver
/// may instead recheck exact accepted catalog recoveries under separate shared session limits; those entries and
/// terminals disclose recovery and withhold the same-owner proof. This scan does not compare
/// field values with keys, prove that every row is indexed, or prove ownership of incoming references.
/// Repeated bookmarks are inspected independently without an unbounded target cache. All storage remains pinned.
/// </remarks>
internal static class CompactIndexBookmarkAudit
{
    /// <summary>Streams raw index observations after validating each entry's bookmark target.</summary>
    /// <param name="pages">The stable caller-owned decrypted page source; its caches are not populated.</param>
    /// <param name="format">The verified source format.</param>
    /// <param name="ownerTDefPage">The expected table-definition owner.</param>
    /// <param name="rootPage">The index root.</param>
    /// <param name="indexBudget">The raw index scan's limits and optional session accounting.</param>
    /// <param name="bookmarkBudget">The repeated target inspection limits and optional session accounting.</param>
    /// <param name="visitor">Consumes each verified entry and the successful terminal proof.</param>
    /// <param name="cancellationToken">Cancels reads, directory inspection and visitor delivery.</param>
    /// <param name="recoveredCatalogResolver">Optionally rechecks exact accepted catalog recoveries under independently enforced shared session limits; its work is separate from the ordinary verifier's local read and directory limits. Recovered targets retain ownership ambiguity and withhold the same-owner proof.</param>
    /// <returns>A task completing only after the composed index and bookmark proofs succeed.</returns>
    /// <exception cref="ArgumentException">Recovered evidence is supplied for a table other than the catalog.</exception>
    /// <exception cref="JetCorruptDataException">An observed bookmark or target directory is malformed or ambiguous.</exception>
    /// <exception cref="JetLimitationException">Target reads, inspected slots, overflow hops or working storage exceed a limit.</exception>
    internal static async ValueTask ScanAsync(
        IPageSource pages,
        JetFormat format,
        long ownerTDefPage,
        long rootPage,
        CompactIndexBudget indexBudget,
        CompactIndexBookmarkBudget bookmarkBudget,
        Func<CompactIndexRecord, CancellationToken, ValueTask> visitor,
        CancellationToken cancellationToken,
        Func<long, int, CancellationToken, ValueTask<RowLocation?>>? recoveredCatalogResolver = null)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(indexBudget, nameof(indexBudget));
        Guard.NotNull(bookmarkBudget, nameof(bookmarkBudget));
        Guard.NotNull(visitor, nameof(visitor));
        if (recoveredCatalogResolver is not null && ownerTDefPage != 2)
        {
            throw new ArgumentException("Recovered bookmark evidence is limited to the catalog.", nameof(recoveredCatalogResolver));
        }

        Guard.InRange(bookmarkBudget.MaxPageReads, 1L, long.MaxValue, nameof(bookmarkBudget));
        Guard.InRange(bookmarkBudget.MaxDirectorySlots, 1L, long.MaxValue, nameof(bookmarkBudget));
        Guard.InRange(bookmarkBudget.MaxWorkingBytes, 1L, long.MaxValue, nameof(bookmarkBudget));
        Guard.InRange(bookmarkBudget.MaxOverflowHops, 1, Constants.DataPage.MaxOverflowHops, nameof(bookmarkBudget));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        long pageCount = pages.PageCount;
        if (pages.PageSize != format.PageSize || ownerTDefPage <= 0 || ownerTDefPage >= pageCount)
        {
            throw Corrupt(ownerTDefPage, "The bookmark source geometry or table owner is invalid.");
        }

        // A full raw directory, one byte per possible live row offset, one
        // target page and bounded cycle coordinates. Include async/object scratch.
        int capacity = (format.PageSize - format.DataPage.RowsStart) / 2;
        long storage = (4L * capacity) + (2L * format.PageSize) + (16L * (bookmarkBudget.MaxOverflowHops + 1)) + 65_536;
        if (storage > bookmarkBudget.MaxWorkingBytes)
        {
            throw Limit(rootPage, "The bookmark verification storage reservation exceeds MaxWorkingBytes.");
        }

        IDisposable? reservation = bookmarkBudget.Accounting?.ReserveWorkingBytes(storage, rootPage);
        try
        {
            var verifier = new Verifier(pages, format, pageCount, ownerTDefPage, bookmarkBudget, capacity);
            bool recoveredTargets = false;
            await CompactIndexAudit.ScanAsync(
                pages,
                format,
                ownerTDefPage,
                rootPage,
                indexBudget,
                async (record, token) =>
                {
                    verifier.CheckSource(record.PageNumber, token);
                    if (record.Kind is "leaf-entry" or "intermediate-entry")
                    {
                        long page = record.TargetPageNumber!.Value;
                        int row = record.TargetRowIndex!.Value;
                        RowLocation? recovery = recoveredCatalogResolver is null ? null : await recoveredCatalogResolver(page, row, token).ConfigureAwait(false);
                        if (recovery is { } recovered)
                        {
                            if (recovered.PageNumber != page || recovered.RowIndex != row)
                            {
                                throw Corrupt(page, "Recovered catalog evidence does not match the index's original bookmark.");
                            }

                            recoveredTargets = true;
                            record = record with { BookmarkTargetRecovered = true, BookmarkDataPageNumber = recovered.DataPageNumber, BookmarkDataRowIndex = recovered.DataRowIndex };
                        }
                        else
                        {
                            RowLocation location = await verifier.ValidateAsync(page, row, token).ConfigureAwait(false);
                            record = record with { BookmarkTargetValidated = true, BookmarkDataPageNumber = location.DataPageNumber, BookmarkDataRowIndex = location.DataRowIndex };
                        }
                    }
                    else if (record.Kind == "index-terminal")
                    {
                        record = record with
                        {
                            BookmarkTargetsValidated = !recoveredTargets,
                            BookmarkTargetsRecovered = recoveredTargets,
                            Coverage = record.Coverage + (recoveredTargets
                                ? " Observed bookmarks include exact independently recovered catalog identities with rechecked stored Ids and readable payloads. Damaged owner headers remain unchanged and same-owner proof is withheld. Key/value correspondence, missing index entries and incoming row aliases remain unverified."
                                : " Observed leaf and intermediate bookmarks identify live same-owner row headers and bounded readable payload chains. Key/value correspondence, missing index entries and incoming row aliases remain unverified."),
                        };
                    }

                    await visitor(record, token).ConfigureAwait(false);
                    verifier.CheckSource(record.PageNumber, token);
                },
                cancellationToken).ConfigureAwait(false);
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

    private sealed class Verifier(IPageSource pages, JetFormat format, long pageCount, long owner, CompactIndexBookmarkBudget budget, int capacity)
    {
        private readonly int[] directory = new int[capacity];
        private readonly bool[] liveOffsets = new bool[format.PageSize];
        private readonly long[] seenPages = new long[budget.MaxOverflowHops + 1];
        private readonly int[] seenRows = new int[budget.MaxOverflowHops + 1];
        private long reads;
        private long slots;

        internal async ValueTask<RowLocation> ValidateAsync(long headerPage, int headerRow, CancellationToken cancellationToken)
        {
            long targetPage = headerPage;
            int targetRow = headerRow;
            for (int hop = 0; ; hop++)
            {
                this.CheckSource(targetPage, cancellationToken);
                if (targetPage <= 0 || targetPage >= pageCount)
                {
                    throw Corrupt(targetPage, "A bookmark payload reference is outside the source page range.");
                }

                for (int seen = 0; seen < hop; seen++)
                {
                    if (this.seenPages[seen] == targetPage && this.seenRows[seen] == targetRow)
                    {
                        throw Corrupt(targetPage, "A bookmark payload chain closes a row-pointer cycle.");
                    }
                }

                this.seenPages[hop] = targetPage;
                this.seenRows[hop] = targetRow;
                if (this.reads >= budget.MaxPageReads)
                {
                    throw Limit(targetPage, "Repeated bookmark target reads exceed MaxPageReads.");
                }

                this.reads++;
                byte[] page = await pages.ReadUncachedPageAsync(targetPage, cancellationToken).ConfigureAwait(false);
                try
                {
                    this.CheckSource(targetPage, cancellationToken);
                    if (page.Length < format.PageSize || page[0] != Constants.PageTypes.Data || Ri32(page, format.DataPage.TDefOff) != owner)
                    {
                        throw Corrupt(targetPage, "A bookmark target is truncated or is not a data page of its table.");
                    }

                    RowBound bound = this.ValidateDirectory(targetPage, page, targetRow, hop > 0, cancellationToken);
                    if (!bound.IsOverflowPointer)
                    {
                        return new(headerPage, headerRow, bound.RowStart, bound.RowSize) { DataPageNumber = targetPage, DataRowIndex = targetRow };
                    }

                    if (bound.RowSize < Constants.DataPage.OverflowPointerSize)
                    {
                        throw Corrupt(targetPage, "A bookmark overflow header is shorter than its pointer.");
                    }

                    if (hop >= budget.MaxOverflowHops)
                    {
                        throw Limit(targetPage, "The bookmark payload chain exceeds MaxOverflowHops.");
                    }

                    budget.Accounting?.Charge(CompactDiscoveryCharge.GraphSteps, 1, targetPage);
                    budget.Accounting?.Charge(CompactDiscoveryCharge.References, 1, targetPage);
                    targetRow = page[bound.RowStart];
                    targetPage = page[bound.RowStart + 1] | (page[bound.RowStart + 2] << 8) | (page[bound.RowStart + 3] << 16);
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }
        }

        internal void CheckSource(long page, CancellationToken cancellationToken)
        {
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            if (pages.PageCount != pageCount || pages.PageSize != format.PageSize)
            {
                throw Corrupt(page, "The source geometry changed during bookmark verification.");
            }
        }

        private RowBound ValidateDirectory(long number, byte[] page, int target, bool moved, CancellationToken cancellationToken)
        {
            int count = Ru16(page, format.DataPage.NumRows);
            if (count > this.directory.Length || target >= count)
            {
                throw Corrupt(number, "A bookmark target directory is oversized or its row slot does not exist.");
            }

            Array.Clear(this.liveOffsets, 0, this.liveOffsets.Length);
            int directoryEnd = format.DataPage.RowsStart + (count * 2);
            for (int row = 0; row < count; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (this.slots >= budget.MaxDirectorySlots)
                {
                    throw Limit(number, "Repeated bookmark directory inspections exceed MaxDirectorySlots.");
                }

                budget.Accounting?.Charge(CompactDiscoveryCharge.DirectorySlots, 1, number);
                this.slots++;
                int raw = Ru16(page, format.DataPage.RowsStart + (row * 2));
                int start = raw & Constants.DataPage.RowOffsetMask;
                bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
                if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0
                    || (!deleted && (start < directoryEnd || start >= format.PageSize))
                    || (deleted && start != 0 && (start < directoryEnd || start > format.PageSize)))
                {
                    throw Corrupt(number, $"Bookmark target directory slot {row} has invalid flags or stored bounds.");
                }

                if (!deleted)
                {
                    if (this.liveOffsets[start])
                    {
                        throw Corrupt(number, $"Bookmark target directory slot {row} aliases another live payload.");
                    }

                    this.liveOffsets[start] = true;
                }

                this.directory[row] = raw;
            }

            int targetRaw = this.directory[target];
            int targetStart = targetRaw & Constants.DataPage.RowOffsetMask;
            bool targetDeleted = (targetRaw & Constants.DataPage.DeletedRowFlag) != 0;
            if (targetDeleted != moved || targetStart < directoryEnd || targetStart >= format.PageSize || (moved && this.liveOffsets[targetStart]))
            {
                throw Corrupt(number, moved ? "A moved bookmark payload is absent, aliases a live row or is itself a live identity." : "An index bookmark names a deleted or absent row header.");
            }

            int end = format.PageSize;
            for (int row = 0; row < count; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int start = this.directory[row] & Constants.DataPage.RowOffsetMask;
                if (start > targetStart && start < end)
                {
                    end = start;
                }
            }

            return new(target, targetStart, end - targetStart, (targetRaw & Constants.DataPage.OverflowRowFlag) != 0);
        }
    }
}
