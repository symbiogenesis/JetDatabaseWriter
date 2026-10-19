namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Recovers independently indexed catalog identities without changing physical ownership.</summary>
internal static class CompactCatalogRecovery
{
    /// <summary>Observes direct live rows named by a catalog Id index when their owner header is damaged.</summary>
    /// <param name="pages">The bounded immutable inventory source.</param>
    /// <param name="format">The verified format.</param>
    /// <param name="catalog">The verified catalog image.</param>
    /// <param name="decoder">The shared catalog identity decoder.</param>
    /// <param name="budget">Inventory admission and work limits.</param>
    /// <param name="physicalRows">Identities already discovered from intact owner headers.</param>
    /// <param name="maxReferences">The remaining aggregate inventory reference allowance.</param>
    /// <param name="maxDirectorySlots">The remaining aggregate inventory map/directory inspection allowance.</param>
    /// <param name="maxWorkingBytes">The working allowance remaining after the enclosing inventory reservation.</param>
    /// <param name="cancellationToken">Cancels all reads and inspections.</param>
    /// <returns>Bounded positive evidence and explicit index/target uncertainty; this is not a coverage or ownership proof.</returns>
    /// <exception cref="JetLimitationException">A shared traversal or storage allowance is exhausted.</exception>
    internal static async ValueTask<Result> ScanAsync(IPageSource pages, JetFormat format, TDefImage catalog, CatalogRowReader decoder, CompactTraversalBudget budget, IReadOnlyList<CatalogRow> physicalRows, long maxReferences, long maxDirectorySlots, long maxWorkingBytes, CancellationToken cancellationToken)
    {
        var result = new Result();
        ColumnInfo? idColumn = catalog.Definition.FindColumn("Id");
        TDefIndexDescriptor? identityIndex = null;
        if (idColumn?.Type == ColumnType.LongIntegerType)
        {
            foreach (TDefIndexDescriptor index in catalog.RealIndexes)
            {
                if (index.RootPage != 0 && index.Columns.Count == 1 && index.Columns[0].ColNum == idColumn.ColNum)
                {
                    identityIndex = index;
                    break;
                }
            }
        }

        if (identityIndex is null || idColumn is null)
        {
            result.Diagnostics.Add((2, "No readable catalog Id index is declared; physical-owner discovery cannot recover identities hidden by damaged owners. All storage remains pinned."));
            return result;
        }

        if (maxReferences <= 0)
        {
            throw Limit(identityIndex.RootPage, "Catalog Id index references exceed MaxReferences.");
        }

        if (maxWorkingBytes <= 0)
        {
            throw Limit(identityIndex.RootPage, "Catalog Id index storage exceeds MaxWorkingBytes after inventory admission.");
        }

        var identities = new Dictionary<long, (long Page, int Row)>();
        foreach (CatalogRow row in physicalRows)
        {
            _ = identities.TryAdd(row.Id, (row.PageNumber, row.RowIndex));
        }

        var scanner = new Scanner(pages, format, catalog, decoder, budget, identityIndex, idColumn, result, identities, maxDirectorySlots, budget.MaxObjects - physicalRows.Count);
        try
        {
            await CompactIndexAudit.ScanAsync(
                pages,
                format,
                2,
                identityIndex.RootPage,
                new CompactIndexBudget
                {
                    Accounting = budget.Accounting,
                    MaxTreePages = checked((int)Math.Min(pages.PageCount - 1, budget.MaxObjects)),
                    MaxEntries = 2L * budget.MaxObjects,
                    MaxCanonicalBytes = 18L * budget.MaxObjects,
                    MaxRetainedKeyBytes = 18L * budget.MaxObjects,
                    MaxReferences = maxReferences,
                    MaxGraphSteps = budget.MaxMapSteps,
                    MaxRecords = budget.MaxRecords,
                    MaxWorkingBytes = maxWorkingBytes,
                },
                scanner.VisitAsync,
                cancellationToken).ConfigureAwait(false);
        }
        catch (JetCorruptDataException error) when (error.ErrorCode == JetErrorCode.CorruptIndex)
        {
            scanner.Diagnose(error.ErrorInfo.PageNumber ?? identityIndex.RootPage, "The catalog Id index cannot be completely audited: " + error.Message + " Positive recovered identities remain observations; coverage and ownership are unproven.");
        }

        return result;
    }

    internal sealed class Result
    {
        internal List<RecoveredRow> Rows { get; } = [];

        internal long References { get; set; }

        internal long DirectorySlots { get; set; }

        internal List<(long Page, string Reason)> Diagnostics { get; } = [];
    }

    internal sealed record RecoveredRow(CatalogRow Identity, long IndexPage, int LiveRowsOnPage, uint PhysicalOwner);

    private sealed class Scanner(IPageSource pages, JetFormat format, TDefImage catalog, CatalogRowReader decoder, CompactTraversalBudget budget, TDefIndexDescriptor index, ColumnInfo idColumn, Result result, Dictionary<long, (long Page, int Row)> identities, long maxDirectorySlots, int maxCandidates)
    {
        private readonly HashSet<(long Page, int Row)> recovered = [];
        private readonly bool[] liveStarts = new bool[format.PageSize];
        private int candidates;

        internal async ValueTask VisitAsync(CompactIndexRecord record, CancellationToken cancellationToken)
        {
            if (record.Kind is "leaf-entry" or "intermediate-entry" or "index-reference")
            {
                result.References++;
            }

            if (record.Kind != "leaf-entry" || record.TargetPageNumber is not { } number || record.TargetRowIndex is not { } target)
            {
                return;
            }

            byte[] page = await pages.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
            try
            {
                uint owner = Ru32(page, format.DataPage.TDefOff);
                if (owner == 2)
                {
                    return;
                }

                try
                {
                    if (page[0] != Constants.PageTypes.Data)
                    {
                        throw Corrupt(number, "An Id index bookmark does not name a data page.");
                    }

                    (RowBound bound, int liveRows) = this.ReadBound(number, page, target, cancellationToken);
                    if (bound.IsOverflowPointer)
                    {
                        throw Corrupt(number, "A damaged-owner catalog overflow header requires separate payload ownership evidence; its identity is not inferred.");
                    }

                    if (this.candidates >= maxCandidates)
                    {
                        throw Limit(number, "Catalog recovery candidate decoding exceeds the remaining MaxObjects allowance.");
                    }

                    budget.Accounting?.Charge(CompactDiscoveryCharge.CatalogObjects, 1, number);
                    this.candidates++;
                    CatalogRow identity = decoder.DecodeRow(catalog.Definition, page, new RowLocation(number, target, bound.RowStart, bound.RowSize), 64);
                    byte[] expected = IndexKeyEncoder.EncodeColumnEntry(format, idColumn, checked((int)identity.Id), index.Columns[0].Ascending);
                    if (record.CanonicalKey is null || !expected.AsSpan().SequenceEqual(record.CanonicalKey))
                    {
                        throw Corrupt(number, "The catalog Id index key disagrees with the bookmarked row's stored Id; this does not authorize catalog recovery.");
                    }

                    if (this.recovered.Contains((number, target)))
                    {
                        return;
                    }

                    if (identities.TryGetValue(identity.Id, out (long Page, int Row) previous) && previous != (number, target))
                    {
                        _ = result.Rows.RemoveAll(row => row.Identity.Id == identity.Id);
                        throw Corrupt(number, $"The catalog Id index names live rows {previous.Page}:{previous.Row} and {number}:{target} for stored Id {identity.Id}; neither conflicting recovery is accepted.");
                    }

                    if (result.Rows.Count >= budget.MaxObjects)
                    {
                        throw Limit(number, "Recovered catalog identities exceed MaxObjects.");
                    }

                    _ = identities.TryAdd(identity.Id, (number, target));
                    _ = this.recovered.Add((number, target));
                    result.Rows.Add(new(identity, record.PageNumber, liveRows, owner));
                }
                catch (JetCorruptDataException error) when (error.ErrorCode is JetErrorCode.CorruptCatalog or JetErrorCode.MalformedValue)
                {
                    this.Diagnose(number, error.ErrorInfo.Reason ?? error.Message);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        internal void Diagnose(long page, string reason)
        {
            if (result.Diagnostics.Count >= budget.MaxDiagnostics)
            {
                throw Limit(page, "Catalog recovery uncertainty exceeds MaxDiagnostics.");
            }

            result.Diagnostics.Add((page, reason));
        }

        private (RowBound Bound, int LiveRows) ReadBound(long number, byte[] page, int target, CancellationToken cancellationToken)
        {
            int count = Ru16(page, format.DataPage.NumRows);
            int endDirectory = format.DataPage.RowsStart + (count * 2);
            if (endDirectory > format.PageSize || target >= count)
            {
                throw Corrupt(number, "An Id index bookmark names an absent or oversized catalog row directory.");
            }

            Array.Clear(this.liveStarts, 0, this.liveStarts.Length);
            int live = 0;
            int rawTarget = Ru16(page, format.DataPage.RowsStart + (target * 2));
            int startTarget = rawTarget & Constants.DataPage.RowOffsetMask;
            int endTarget = format.PageSize;
            for (int row = 0; row < count; row++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (result.DirectorySlots >= maxDirectorySlots)
                {
                    throw Limit(number, "Catalog recovery directory inspections exceed MaxMapSteps.");
                }

                budget.Accounting?.Charge(CompactDiscoveryCharge.MapSteps, 1, number);
                result.DirectorySlots++;
                int raw = Ru16(page, format.DataPage.RowsStart + (row * 2));
                int start = raw & Constants.DataPage.RowOffsetMask;
                bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
                if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0
                    || (!deleted && (start < endDirectory || start >= format.PageSize))
                    || (deleted && start != 0 && (start < endDirectory || start > format.PageSize)))
                {
                    throw Corrupt(number, "An Id index bookmark target has invalid directory flags or bounds.");
                }

                if (!deleted)
                {
                    if (this.liveStarts[start])
                    {
                        throw Corrupt(number, "An Id index bookmark target aliases another live row payload.");
                    }

                    this.liveStarts[start] = true;
                    live++;
                }

                if (start > startTarget && start < endTarget)
                {
                    endTarget = start;
                }
            }

            if ((rawTarget & Constants.DataPage.DeletedRowFlag) != 0 || startTarget < endDirectory || startTarget >= format.PageSize)
            {
                throw Corrupt(number, "An Id index bookmark names a deleted or absent catalog row identity.");
            }

            return (new(target, startTarget, endTarget - startTarget, (rawTarget & Constants.DataPage.OverflowRowFlag) != 0), live);
        }
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptCatalog, reason, new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });
}
