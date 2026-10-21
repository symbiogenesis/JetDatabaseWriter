namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Text;
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
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Recovers independently indexed catalog identities without changing physical ownership.</summary>
internal static class CompactCatalogRecovery
{
    /// <summary>Reads intact catalog owner headers, deferring damaged moved owners to independent index recovery.</summary>
    /// <param name="pages">The bounded immutable inventory source.</param>
    /// <param name="format">The verified format.</param>
    /// <param name="catalog">The verified catalog image.</param>
    /// <param name="decoder">The shared catalog identity decoder.</param>
    /// <param name="budget">Inventory admission and work limits.</param>
    /// <param name="maxReferences">The remaining aggregate reference allowance.</param>
    /// <param name="maxDirectorySlots">The remaining directory and map-step allowance.</param>
    /// <param name="cancellationToken">Cancels reads and inspections.</param>
    /// <returns>Physical identities and explicit deferred-owner diagnostics, without claiming coverage or ownership.</returns>
    internal static async ValueTask<Result> ScanPhysicalAsync(IPageSource pages, JetFormat format, TDefImage catalog, CatalogRowReader decoder, CompactTraversalBudget budget, long maxReferences, long maxDirectorySlots, CancellationToken cancellationToken)
    {
        var result = new Result();
        var scanner = new Scanner(pages, format, catalog, decoder, budget, null, null, result, [], [], maxReferences, maxDirectorySlots, Math.Min(budget.MaxObjects, Math.Max(1, budget.MaxSchemaBytes / 64)));
        await scanner.ScanPhysicalAsync(cancellationToken).ConfigureAwait(false);
        return result;
    }

    /// <summary>Observes indexed catalog identities with matching stored Ids when header and possibly moved-payload ownership is damaged.</summary>
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
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
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

        if (identityIndex is not null && idColumn is not null)
        {
            result = await ScanIndexAsync(pages, format, catalog, decoder, budget, physicalRows, identityIndex, [idColumn], maxReferences, maxDirectorySlots, maxWorkingBytes, null, cancellationToken).ConfigureAwait(false);
            if (result.IndexValidated && !result.HasUnresolvedTargets)
            {
                return result;
            }
        }
        else
        {
            result.Diagnostics.Add((2, "No readable catalog Id index is declared; recovery requires another independently audited catalog key. All storage remains pinned."));
        }

        ColumnInfo? parentColumn = catalog.Definition.FindColumn("ParentId");
        ColumnInfo? nameColumn = catalog.Definition.FindColumn("Name");
        if (idColumn?.Type != ColumnType.LongIntegerType
            || parentColumn?.Type != ColumnType.LongIntegerType || nameColumn?.Type != ColumnType.TextType)
        {
            return result;
        }

        if (!nameColumn.TextSortOrder.IsSupported)
        {
            if (result.Diagnostics.Count >= budget.MaxDiagnostics)
            {
                throw Limit(2, "Catalog recovery uncertainty exceeds MaxDiagnostics.");
            }

            result.Diagnostics.Add((2, "The catalog Name collation is unsupported; ParentIdName recovery cannot verify native stored keys. All storage remains pinned."));
            return result;
        }

        foreach (TDefIndexDescriptor index in catalog.RealIndexes)
        {
            if (index.RootPage == 0 || !index.Slot.IsUnique || index.Columns.Count != 2
                || index.Columns[0].ColNum != parentColumn.ColNum || index.Columns[1].ColNum != nameColumn.ColNum)
            {
                continue;
            }

            var knownRows = new List<CatalogRow>(physicalRows);
            foreach (RecoveredRow evidence in result.Rows)
            {
                knownRows.Add(evidence.Identity);
            }

            Result fallback = await ScanIndexAsync(pages, format, catalog, decoder, budget, knownRows, index, [parentColumn, nameColumn], maxReferences - result.References, maxDirectorySlots - result.DirectorySlots - result.OverflowHops, maxWorkingBytes, result, cancellationToken).ConfigureAwait(false);
            if (result.Diagnostics.Count > budget.MaxDiagnostics - fallback.Diagnostics.Count)
            {
                throw Limit(index.RootPage, "Catalog recovery uncertainty exceeds MaxDiagnostics.");
            }

            _ = result.Rows.RemoveAll(row => fallback.ConflictingIds.Contains(row.Identity.Id)
                || fallback.ConflictingBookmarks.Contains((row.Identity.PageNumber, row.Identity.RowIndex))
                || fallback.ConflictingPayloads.Contains((row.Location.DataPageNumber, row.Location.RowStart)));
            result.Rows.AddRange(fallback.Rows);
            result.Diagnostics.AddRange(fallback.Diagnostics);
            result.References += fallback.References;
            result.DirectorySlots += fallback.DirectorySlots;
            result.OverflowHops += fallback.OverflowHops;
            break;
        }

        return result;
    }

    private static async ValueTask<Result> ScanIndexAsync(IPageSource pages, JetFormat format, TDefImage catalog, CatalogRowReader decoder, CompactTraversalBudget budget, IReadOnlyList<CatalogRow> physicalRows, TDefIndexDescriptor identityIndex, IReadOnlyList<ColumnInfo> keyColumns, long maxReferences, long maxDirectorySlots, long maxWorkingBytes, Result? previous, CancellationToken cancellationToken)
    {
        var result = new Result
        {
            ConflictingIds = previous?.ConflictingIds ?? [],
            ConflictingBookmarks = previous?.ConflictingBookmarks ?? [],
            ConflictingPayloads = previous?.ConflictingPayloads ?? [],
            PayloadHeaders = previous?.PayloadHeaders ?? [],
        };
        string keyName = keyColumns.Count == 1 ? "Id" : "ParentIdName";
        if (maxReferences <= 0)
        {
            throw Limit(identityIndex.RootPage, "Catalog " + keyName + " index references exceed MaxReferences.");
        }

        if (maxWorkingBytes <= 0)
        {
            throw Limit(identityIndex.RootPage, "Catalog " + keyName + " index storage exceeds MaxWorkingBytes after inventory admission.");
        }

        var identities = new Dictionary<long, (long Page, int Row)>();
        var physicalBookmarks = new HashSet<(long Page, int Row)>();
        foreach (CatalogRow row in physicalRows)
        {
            _ = identities.TryAdd(row.Id, (row.PageNumber, row.RowIndex));
            _ = physicalBookmarks.Add((row.PageNumber, row.RowIndex));
        }

        var scanner = new Scanner(pages, format, catalog, decoder, budget, identityIndex, keyColumns, result, identities, physicalBookmarks, maxReferences, maxDirectorySlots, budget.MaxObjects - physicalRows.Count);
        bool indexAuditCompleted = false;
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
                    MaxCanonicalBytes = (keyColumns.Count == 1 ? 18L : 1024L) * budget.MaxObjects,
                    MaxRetainedKeyBytes = (keyColumns.Count == 1 ? 18L : 1024L) * budget.MaxObjects,
                    MaxReferences = maxReferences,
                    MaxGraphSteps = budget.MaxMapSteps,
                    MaxRecords = budget.MaxRecords,
                    MaxWorkingBytes = maxWorkingBytes,
                },
                scanner.VisitAsync,
                cancellationToken).ConfigureAwait(false);
            indexAuditCompleted = true;
        }
        catch (JetCorruptDataException error) when (error.ErrorCode == JetErrorCode.CorruptIndex)
        {
            scanner.Diagnose(error.ErrorInfo.PageNumber ?? identityIndex.RootPage, "The catalog " + keyName + " index cannot be completely audited: " + error.Message + " Positive recovered identities remain observations; coverage and ownership are unproven.");
        }

        scanner.Complete(indexAuditCompleted, cancellationToken);
        return result;
    }

    internal sealed class Result
    {
        internal bool HasUnresolvedTargets { get; set; }

        internal HashSet<long> ConflictingIds { get; init; } = [];

        internal HashSet<(long Page, int Row)> ConflictingBookmarks { get; init; } = [];

        internal HashSet<(long Page, int Start)> ConflictingPayloads { get; init; } = [];

        internal Dictionary<(long Page, int Start), (long Page, int Row)> PayloadHeaders { get; init; } = [];

        internal bool IndexValidated { get; set; }

        internal List<RecoveredRow> Rows { get; } = [];

        internal long References { get; set; }

        internal long DirectorySlots { get; set; }

        internal long OverflowHops { get; set; }

        internal List<(long Page, string Reason)> Diagnostics { get; } = [];
    }

    internal sealed record RecoveredRow(CatalogRow Identity, long IndexPage, int LiveRowsOnPage, uint PhysicalOwner)
    {
        internal string IdentityKey { get; init; } = "Id";

        internal RowLocation Location { get; init; }

        internal bool HasDamagedMovedOwner { get; init; }
    }

    private sealed class Scanner(IPageSource pages, JetFormat format, TDefImage catalog, CatalogRowReader decoder, CompactTraversalBudget budget, TDefIndexDescriptor? index, IReadOnlyList<ColumnInfo>? keyColumns, Result result, Dictionary<long, (long Page, int Row)> identities, HashSet<(long Page, int Row)> physicalBookmarks, long maxReferences, long maxDirectorySlots, int maxCandidates)
    {
        private readonly HashSet<(long Page, int Row)> recovered = [];
        private readonly bool[] liveStarts = new bool[format.PageSize];
        private readonly (long Page, int Row)[] chain = new (long, int)[Constants.DataPage.MaxOverflowHops + 1];
        private int candidates;
        private bool independentIndexValidated;

        private static void ValidateUnicodeUnit(int unit, ref bool pendingSurrogate, long page)
        {
            bool low = unit is >= 0xDC00 and <= 0xDFFF;
            if (pendingSurrogate != low)
            {
                throw Corrupt(page, "The secondary catalog identity Name contains an unpaired Unicode surrogate.");
            }

            pendingSurrogate = unit is >= 0xD800 and <= 0xDBFF;
        }

        internal async ValueTask ScanPhysicalAsync(CancellationToken cancellationToken)
        {
            for (long number = 3; number < pages.PageCount; number++)
            {
                pages.ThrowIfDisposedOrCancelled(cancellationToken);
                byte[] page = await pages.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (Ru32(page, format.DataPage.TDefOff) != 2)
                    {
                        continue;
                    }

                    if (page[0] != Constants.PageTypes.Data)
                    {
                        if (page[0] is not 0 and not Constants.PageTypes.TableDefinition
                            and not Constants.PageTypes.IndexIntermediate and not Constants.PageTypes.IndexLeaf
                            and not Constants.PageTypes.UsageMap and not Constants.PageTypes.Freed)
                        {
                            throw Corrupt(number, "An unknown page type retains the catalog owner header.");
                        }

                        continue;
                    }

                    _ = this.ReadBound(number, page, -1, moved: false, cancellationToken);
                    int count = Ru16(page, format.DataPage.NumRows);
                    for (int row = 0; row < count; row++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        int raw = Ru16(page, format.DataPage.RowsStart + (row * 2));
                        if ((raw & Constants.DataPage.DeletedRowFlag) == 0)
                        {
                            await this.RecoverAsync(null, number, row, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }

            pages.ThrowIfDisposedOrCancelled(cancellationToken);
        }

        internal async ValueTask VisitAsync(CompactIndexRecord record, CancellationToken cancellationToken)
        {
            if (record.Kind == "index-terminal")
            {
                this.independentIndexValidated = record.NavigableGraphAcyclic && record.ObservedLeafChainOrdered
                    && record.BookmarkInclusiveOrderValidated && record.ObservedKeyRoutingValidated;
            }

            if (record.Kind is "leaf-entry" or "intermediate-entry" or "index-reference")
            {
                this.Reference(record.PageNumber, overflow: false);
            }

            if (record.Kind != "leaf-entry" || record.TargetPageNumber is not { } number || record.TargetRowIndex is not { } target)
            {
                return;
            }

            try
            {
                await this.RecoverAsync(record, number, target, cancellationToken).ConfigureAwait(false);
            }
            catch (JetCorruptDataException error) when (error.ErrorCode is JetErrorCode.CorruptCatalog or JetErrorCode.MalformedValue)
            {
                result.HasUnresolvedTargets = true;
                this.Diagnose(error.ErrorInfo.PageNumber ?? number, error.ErrorInfo.Reason ?? error.Message);
            }
        }

        internal void Complete(bool indexAuditCompleted, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (indexAuditCompleted && this.independentIndexValidated)
            {
                result.IndexValidated = true;
                return;
            }

            int retained = 0;
            for (int i = 0; i < result.Rows.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RecoveredRow row = result.Rows[i];
                if (!row.HasDamagedMovedOwner && row.IdentityKey == "Id")
                {
                    result.Rows[retained++] = row;
                }
            }

            if (retained < result.Rows.Count)
            {
                result.Rows.RemoveRange(retained, result.Rows.Count - retained);
                this.Diagnose(index!.RootPage, "Catalog identities with damaged moved ownership or a secondary identity key require a completed independent index audit, ordered observed leaf chain and verified key routing; those candidates are withheld.");
            }
        }

        private async ValueTask RecoverAsync(CompactIndexRecord? record, long headerPage, int headerRow, CancellationToken cancellationToken)
        {
            if (physicalBookmarks.Contains((headerPage, headerRow)))
            {
                return;
            }

            if (result.ConflictingBookmarks.Contains((headerPage, headerRow)))
            {
                throw Corrupt(headerPage, "A previously conflicting catalog bookmark cannot be accepted through another index.");
            }

            long number = headerPage;
            int target = headerRow;
            uint headerOwner = 0;
            int headerLiveRows = 0;
            bool damagedMovedOwner = false;
            for (int hop = 0; ; hop++)
            {
                pages.ThrowIfDisposedOrCancelled(cancellationToken);
                if (number <= 0 || number >= pages.PageCount)
                {
                    throw Corrupt(number, "A catalog moved-payload pointer is outside the source page range.");
                }

                for (int seen = 0; seen < hop; seen++)
                {
                    if (this.chain[seen] == (number, target))
                    {
                        throw Corrupt(number, "A catalog moved-payload chain closes a row-pointer cycle.");
                    }
                }

                this.chain[hop] = (number, target);
                byte[] page = await pages.ReadPageAsync(number, cancellationToken).ConfigureAwait(false);
                try
                {
                    uint owner = Ru32(page, format.DataPage.TDefOff);
                    if (hop == 0)
                    {
                        headerOwner = owner;
                        if (this.candidates >= maxCandidates)
                        {
                            throw Limit(number, "Catalog recovery candidate decoding exceeds the remaining MaxObjects allowance.");
                        }

                        budget.Accounting?.Charge(CompactDiscoveryCharge.CatalogObjects, 1, number);
                        this.candidates++;
                    }
                    else if (owner != 2)
                    {
                        if (record is null)
                        {
                            this.Diagnose(headerPage, "An intact catalog header reaches a moved payload with damaged ownership; physical traversal defers this bookmark to independent Id-index recovery. All storage remains pinned.");
                            return;
                        }

                        damagedMovedOwner = true;
                    }

                    if (page[0] != Constants.PageTypes.Data)
                    {
                        throw Corrupt(number, "An Id index bookmark or moved-payload pointer does not name a data page.");
                    }

                    (RowBound bound, int liveRows) = this.ReadBound(number, page, target, hop > 0, cancellationToken);
                    if (hop == 0)
                    {
                        headerLiveRows = liveRows;
                    }

                    if (bound.IsOverflowPointer)
                    {
                        if (bound.RowSize < Constants.DataPage.OverflowPointerSize)
                        {
                            throw Corrupt(number, "A catalog overflow header is shorter than its moved-payload pointer.");
                        }

                        if (hop >= Constants.DataPage.MaxOverflowHops)
                        {
                            throw Limit(number, "Catalog recovery exceeds the bounded MaxOverflowHops allowance.");
                        }

                        this.Step(number, overflow: true, cancellationToken);
                        this.Reference(number, overflow: true);
                        target = page[bound.RowStart];
                        number = page[bound.RowStart + 1] | (page[bound.RowStart + 2] << 8) | (page[bound.RowStart + 3] << 16);
                        continue;
                    }

                    var location = new RowLocation(headerPage, headerRow, bound.RowStart, bound.RowSize) { DataPageNumber = number, DataRowIndex = target };
                    if (hop > 0)
                    {
                        this.ValidatePayloadHeader(location);
                    }

                    CatalogRow identity = decoder.DecodeRow(catalog.Definition, page, location, 64);
                    if (result.ConflictingIds.Contains(identity.Id))
                    {
                        throw Corrupt(number, "A previously conflicting stored catalog Id cannot be accepted through another index.");
                    }

                    if (record is not null)
                    {
                        byte[] expected = this.EncodeIdentityKey(identity, page, location);
                        if (record.CanonicalKey is null || !expected.AsSpan().SequenceEqual(record.CanonicalKey))
                        {
                            throw Corrupt(number, "The catalog index key disagrees with the bookmarked row's stored identity fields; this does not authorize catalog recovery.");
                        }
                    }

                    if (this.recovered.Contains((headerPage, headerRow)))
                    {
                        return;
                    }

                    if (identities.TryGetValue(identity.Id, out (long Page, int Row) previous) && previous != (headerPage, headerRow))
                    {
                        _ = result.ConflictingIds.Add(identity.Id);
                        _ = result.ConflictingBookmarks.Add(previous);
                        _ = result.ConflictingBookmarks.Add((headerPage, headerRow));
                        _ = result.Rows.RemoveAll(row => row.Identity.Id == identity.Id);
                        throw Corrupt(number, $"The catalog Id index names live rows {previous.Page}:{previous.Row} and {headerPage}:{headerRow} for stored Id {identity.Id}; neither conflicting recovery is accepted.");
                    }

                    if (result.Rows.Count >= budget.MaxObjects)
                    {
                        throw Limit(number, "Recovered catalog identities exceed MaxObjects.");
                    }

                    _ = identities.TryAdd(identity.Id, (headerPage, headerRow));
                    _ = this.recovered.Add((headerPage, headerRow));
                    result.Rows.Add(new(identity, record?.PageNumber ?? 0, headerLiveRows, headerOwner) { Location = location, HasDamagedMovedOwner = damagedMovedOwner, IdentityKey = keyColumns?.Count == 2 ? "ParentIdName" : "Id" });
                    return;
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }
        }

        private byte[] EncodeIdentityKey(CatalogRow identity, byte[] page, RowLocation location)
        {
            if (keyColumns!.Count == 1)
            {
                return IndexKeyEncoder.EncodeColumnEntry(format, keyColumns[0], checked((int)identity.Id), index!.Columns[0].Ascending);
            }

            string storedParent = ScalarColumnReader.DecodeSimpleColumnValue(format, page, location.RowStart, location.RowSize, keyColumns[0]);
            if (!CatalogValueReader.TryParseInt32(storedParent, out int parent))
            {
                throw Corrupt(location.DataPageNumber, "A secondary catalog identity key requires an intact signed ParentId value.");
            }

            this.ValidateIdentityName(identity, page, location);
            byte[] parentKey = IndexKeyEncoder.EncodeColumnEntry(format, keyColumns[0], parent, index!.Columns[0].Ascending);
            byte[] nameKey = IndexKeyEncoder.EncodeColumnEntry(format, keyColumns[1], identity.Name, index.Columns[1].Ascending);
            byte[] resultKey = new byte[parentKey.Length + nameKey.Length];
            parentKey.CopyTo(resultKey, 0);
            nameKey.CopyTo(resultKey, parentKey.Length);
            return resultKey;
        }

        private void ValidateIdentityName(CatalogRow identity, byte[] page, RowLocation location)
        {
            if (!RowDecodePlan.TryParseRowLayout(format.RowFields, page, location.RowStart, location.RowSize, hasVarColumns: true, out RowLayout layout))
            {
                throw Corrupt(location.DataPageNumber, "The secondary catalog identity key has an invalid row layout.");
            }

            ColumnInfo name = keyColumns![1];
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, location.RowStart, location.RowSize, layout, name);
            ReadOnlySpan<byte> stored = page.AsSpan(location.RowStart + slice.DataStart, slice.DataLen);
            if (format.IsJet3)
            {
                try
                {
                    if (!format.EncodeText(identity.Name).AsSpan().SequenceEqual(stored))
                    {
                        throw Corrupt(location.DataPageNumber, "The secondary catalog identity Name does not round-trip through its declared code page.");
                    }
                }
                catch (EncoderFallbackException)
                {
                    throw Corrupt(location.DataPageNumber, "The secondary catalog identity Name cannot be represented in its declared code page.");
                }

                return;
            }

            if (name.IsFixed || stored.Length < 2 || stored[0] != 0xFF || stored[1] != 0xFE)
            {
                if ((stored.Length & 1) != 0)
                {
                    throw Corrupt(location.DataPageNumber, "The secondary catalog identity Name contains an incomplete Unicode code unit.");
                }

                bool pending = false;
                for (int offset = 0; offset < stored.Length; offset += 2)
                {
                    ValidateUnicodeUnit(stored[offset] | (stored[offset + 1] << 8), ref pending, location.DataPageNumber);
                }

                if (pending)
                {
                    throw Corrupt(location.DataPageNumber, "The secondary catalog identity Name contains an unpaired Unicode surrogate.");
                }

                return;
            }

            bool compressed = true;
            bool pendingSurrogate = false;
            for (int offset = 2; offset < stored.Length;)
            {
                if (compressed)
                {
                    int unit = stored[offset++];
                    compressed = unit != 0;
                    if (compressed)
                    {
                        ValidateUnicodeUnit(unit, ref pendingSurrogate, location.DataPageNumber);
                    }
                }
                else
                {
                    if (stored[offset] == 0)
                    {
                        compressed = true;
                        offset++;
                        continue;
                    }

                    if (offset > stored.Length - 2)
                    {
                        throw Corrupt(location.DataPageNumber, "The secondary catalog identity Name contains an incomplete compressed Unicode code unit.");
                    }

                    ValidateUnicodeUnit(stored[offset] | (stored[offset + 1] << 8), ref pendingSurrogate, location.DataPageNumber);
                    offset += 2;
                }
            }

            if (pendingSurrogate)
            {
                throw Corrupt(location.DataPageNumber, "The secondary catalog identity Name contains an unpaired compressed Unicode surrogate.");
            }
        }

        private void ValidatePayloadHeader(RowLocation location)
        {
            (long Page, int Start) target = (location.DataPageNumber, location.RowStart);
            (long Page, int Row) header = (location.PageNumber, location.RowIndex);
            if (result.ConflictingPayloads.Contains(target))
            {
                throw Corrupt(location.DataPageNumber, "A previously ambiguous catalog payload cannot be accepted through another index.");
            }

            if (result.PayloadHeaders.TryGetValue(target, out (long Page, int Row) previous) && previous != header)
            {
                _ = result.ConflictingPayloads.Add(target);
                _ = result.ConflictingBookmarks.Add(previous);
                _ = result.ConflictingBookmarks.Add(header);
                _ = result.Rows.RemoveAll(row => (row.Location.DataPageNumber, row.Location.RowStart) == target);
                throw Corrupt(location.DataPageNumber, "Distinct live catalog bookmarks reach the same moved-payload start; neither ambiguous recovery is accepted.");
            }

            if (!result.PayloadHeaders.ContainsKey(target) && result.PayloadHeaders.Count >= budget.MaxObjects)
            {
                throw Limit(location.DataPageNumber, "Catalog moved-payload identity tracking exceeds MaxObjects across independent evidence passes.");
            }

            _ = result.PayloadHeaders.TryAdd(target, header);
        }

        private void Reference(long page, bool overflow)
        {
            if (result.References >= maxReferences)
            {
                throw Limit(page, "Catalog Id index and moved-payload references exceed MaxReferences.");
            }

            if (overflow)
            {
                budget.Accounting?.Charge(CompactDiscoveryCharge.References, 1, page);
            }

            result.References++;
        }

        private void Step(long page, bool overflow, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (result.DirectorySlots + result.OverflowHops >= maxDirectorySlots)
            {
                throw Limit(page, "Catalog recovery directory inspections and overflow hops exceed MaxMapSteps.");
            }

            budget.Accounting?.Charge(CompactDiscoveryCharge.MapSteps, 1, page);
            if (overflow)
            {
                budget.Accounting?.Charge(CompactDiscoveryCharge.GraphSteps, 1, page);
                result.OverflowHops++;
            }
            else
            {
                result.DirectorySlots++;
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

        private (RowBound Bound, int LiveRows) ReadBound(long number, byte[] page, int target, bool moved, CancellationToken cancellationToken)
        {
            int count = Ru16(page, format.DataPage.NumRows);
            int endDirectory = format.DataPage.RowsStart + (count * 2);
            if (endDirectory > format.PageSize || target >= count)
            {
                throw Corrupt(number, "An Id index bookmark names an absent or oversized catalog row directory.");
            }

            Array.Clear(this.liveStarts, 0, this.liveStarts.Length);
            int live = 0;
            int rawTarget = target < 0 ? 0 : Ru16(page, format.DataPage.RowsStart + (target * 2));
            int startTarget = rawTarget & Constants.DataPage.RowOffsetMask;
            int endTarget = format.PageSize;
            for (int row = 0; row < count; row++)
            {
                this.Step(number, overflow: false, cancellationToken);
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

            if (target < 0)
            {
                return (default, live);
            }

            bool targetDeleted = (rawTarget & Constants.DataPage.DeletedRowFlag) != 0;
            if (targetDeleted != moved || startTarget < endDirectory || startTarget >= format.PageSize || (moved && this.liveStarts[startTarget]))
            {
                throw Corrupt(number, moved ? "A moved catalog payload is absent, aliases a live row or is itself a live identity." : "An Id index bookmark names a deleted or absent catalog row identity.");
            }

            return (new(target, startTarget, endTarget - startTarget, (rawTarget & Constants.DataPage.OverflowRowFlag) != 0), live);
        }
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptCatalog, reason, new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });
}
