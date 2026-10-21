namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Inventories known structural references while pinning every physical page.</summary>
internal static class CompactStorageInventory
{
    /// <summary>Streams physical, catalog, TDEF and map observations, without claiming complete incoming-reference coverage.</summary>
    /// <param name="pages">The immutable decrypted page source.</param>
    /// <param name="format">The source's verified format.</param>
    /// <param name="tableDefs">Supplies definition limits; its cached images are not used or populated.</param>
    /// <param name="budget">The traversal limits.</param>
    /// <param name="visitor">Consumes each observation before traversal continues.</param>
    /// <param name="cancellationToken">Cancels traversal.</param>
    /// <param name="definitionVisitor">Optionally consumes each discovered local definition while inventory storage remains admitted.</param>
    /// <param name="recoveredCatalogVisitor">Optionally consumes accepted indexed catalog recovery evidence before any definition callback.</param>
    /// <returns>A task that completes only when this inventory phase succeeds.</returns>
    /// <exception cref="JetLimitationException">A traversal or schema-reader limit is exceeded.</exception>
    /// <exception cref="JetCorruptDataException">A known structural reference is malformed.</exception>
    internal static ValueTask ScanAsync(IPageSource pages, JetFormat format, TableDefReader tableDefs, CompactTraversalBudget budget, Func<CompactStorageRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken, Func<long, TDefImage, CancellationToken, ValueTask>? definitionVisitor = null, Func<CompactCatalogRecovery.RecoveredRow, CancellationToken, ValueTask>? recoveredCatalogVisitor = null)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(tableDefs, nameof(tableDefs));
        Guard.Positive(tableDefs.MaxLogicalBytes, nameof(tableDefs));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(visitor, nameof(visitor));
        Guard.InRange(budget.MaxPages, 1L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxObjects, nameof(budget));
        Guard.InRange(budget.MaxReferences, 1L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxDiagnostics, nameof(budget));
        Guard.Positive(budget.MaxSchemaBytes, nameof(budget));
        Guard.InRange(budget.MaxPageReads, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxMapSteps, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRecords, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        if (pages.PageCount > budget.MaxPages)
        {
            throw Limit("The physical page count exceeds MaxPages.");
        }

        if (tableDefs.MaxSchemaTraversalBytes > budget.MaxSchemaBytes)
        {
            throw Limit("The definition reader must be bounded by MaxSchemaBytes before inventory.");
        }

        if (pages.PageSize != format.PageSize || pages.PageCount < 3)
        {
            throw Corrupt(0, "The physical database size or page layout is invalid.");
        }

        // Reserve worst-case collection capacities before any traversal allocates
        // them. Readers are private to this scan, so these collections cannot grow
        // across calls or retain every parsed table image in the caller's cache.
        long remainingWorkingBytes = budget.MaxWorkingBytes;
        ReserveWorkingStorage(ref remainingWorkingBytes, 4 * 1024 * 1024, 1);
        ReserveWorkingStorage(ref remainingWorkingBytes, budget.MaxObjects, 1024);
        ReserveWorkingStorage(ref remainingWorkingBytes, Math.Min(tableDefs.MaxLogicalBytes, budget.MaxSchemaBytes), 16);
        long reservation = budget.MaxWorkingBytes - remainingWorkingBytes;
        return RunOwnedAsync(pages, format, tableDefs.MaxLogicalBytes, budget, visitor, reservation, definitionVisitor, recoveredCatalogVisitor, cancellationToken);
    }

    private static void ReserveWorkingStorage(ref long remaining, long count, int bytesPerItem)
    {
        if (count > remaining / bytesPerItem)
        {
            throw Limit("The conservative scan-owned storage reservation exceeds MaxWorkingBytes.");
        }

        remaining -= count * bytesPerItem;
    }

    private static async ValueTask RunOwnedAsync(IPageSource pages, JetFormat format, int maxLogicalBytes, CompactTraversalBudget budget, Func<CompactStorageRecord, CancellationToken, ValueTask> visitor, long reservation, Func<long, TDefImage, CancellationToken, ValueTask>? definitionVisitor, Func<CompactCatalogRecovery.RecoveredRow, CancellationToken, ValueTask>? recoveredCatalogVisitor, CancellationToken cancellationToken)
    {
        using IDisposable? admission = budget.Accounting?.ReserveWorkingBytes(reservation);
        var bounded = new CompactInventoryPageSource(pages, format, budget.MaxPageReads, budget.Accounting);
        using var definitions = new TableDefReader(bounded, format, cacheResults: false, Math.Min(maxLogicalBytes, budget.MaxSchemaBytes), budget.MaxSchemaBytes);
        using var owned = new OwnedDataPages(bounded, format, cacheResults: false);
        var catalog = new CatalogRowReader(format, definitions, owned);
        await new Scanner(bounded, format, definitions, catalog, budget, visitor, definitionVisitor, recoveredCatalogVisitor, reservation).RunAsync(cancellationToken).ConfigureAwait(false);
    }

    private static JetLimitationException Limit(string reason, long? page = null)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptTableDefinition, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private sealed class Scanner(IPageSource pages, JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows, CompactTraversalBudget budget, Func<CompactStorageRecord, CancellationToken, ValueTask> visitor, Func<long, TDefImage, CancellationToken, ValueTask>? definitionVisitor, Func<CompactCatalogRecovery.RecoveredRow, CancellationToken, ValueTask>? recoveredCatalogVisitor, long inventoryReservation)
    {
        private readonly HashSet<long> recoveredCatalogPages = [];
        private long references;
        private int diagnostics;
        private long mapSteps;
        private long records;

        internal async ValueTask RunAsync(CancellationToken cancellationToken)
        {
            await this.EmitAsync(
                new CompactStorageRecord
                {
                    Kind = "coverage",
                    Role = "physical-catalog-tdef-maps",
                    Detail = "Known physical headers, catalog identities, TDEF chains and allocation maps only; in-file map bits are observations, not ownership proofs. Row fields, LVAL chains, index trees and opaque incoming references are not covered. All storage is pinned; this is not relocation permission. Hashes cover decrypted physical pages.",
                },
                cancellationToken).ConfigureAwait(false);
            await this.ScanPhysicalPagesAsync(cancellationToken).ConfigureAwait(false);
            await this.ScanMapAsync(1, 0, 1, "global-free", cancellationToken).ConfigureAwait(false);

            _ = await this.ValidateDefinitionChainAsync(2, budget.MaxSchemaBytes, cancellationToken).ConfigureAwait(false);
            TDefImage catalog = await tableDefs.ReadImageAsync(2, budget.MaxSchemaBytes, cancellationToken).ConfigureAwait(false)
                ?? throw Corrupt(2, "The catalog definition cannot be read.");
            CompactCatalogRecovery.Result physical = await CompactCatalogRecovery.ScanPhysicalAsync(pages, format, catalog, catalogRows, budget, budget.MaxReferences - this.references, budget.MaxMapSteps - this.mapSteps, cancellationToken).ConfigureAwait(false);
            this.references += physical.References;
            this.mapSteps += physical.DirectorySlots + physical.OverflowHops;
            var rows = new List<CatalogRow>(physical.Rows.Count);
            foreach (CompactCatalogRecovery.RecoveredRow evidence in physical.Rows)
            {
                rows.Add(evidence.Identity);
            }

            foreach ((long page, string reason) in physical.Diagnostics)
            {
                await this.DiagnosticAsync(page, reason, cancellationToken).ConfigureAwait(false);
            }

            CompactCatalogRecovery.Result recovered = await CompactCatalogRecovery.ScanAsync(pages, format, catalog, catalogRows, budget, rows, budget.MaxReferences - this.references, budget.MaxMapSteps - this.mapSteps, budget.MaxWorkingBytes - inventoryReservation, cancellationToken).ConfigureAwait(false);
            this.references += recovered.References;
            this.mapSteps += recovered.DirectorySlots + recovered.OverflowHops;
            var recoveredCounts = new Dictionary<long, int>();
            foreach (CompactCatalogRecovery.RecoveredRow evidence in recovered.Rows)
            {
                CatalogRow identity = evidence.Identity;
                if (rows.Count >= budget.MaxObjects || rows.Count >= Math.Max(1, tableDefs.MaxSchemaTraversalBytes / 64))
                {
                    throw Limit("The recovered catalog object count exceeds its object or schema allowance.", identity.PageNumber);
                }

                rows.Add(identity);
                recoveredCounts.TryGetValue(identity.PageNumber, out int count);
                recoveredCounts[identity.PageNumber] = count + 1;
                if (count == 0)
                {
                    await this.DiagnosticAsync(identity.PageNumber, "Catalog identities are recovered from a " + evidence.IdentityKey + " index bookmark and matching stored identity fields despite uncertain header or moved-payload ownership (header owner " + evidence.PhysicalOwner + "); this proves neither exclusive ownership nor complete catalog coverage. All storage remains pinned.", cancellationToken).ConfigureAwait(false);
                }

                if (count + 1 == evidence.LiveRowsOnPage)
                {
                    _ = this.recoveredCatalogPages.Add(identity.PageNumber);
                }

                await this.EmitAsync(
                    new CompactStorageRecord
                    {
                        Kind = "recovery",
                        Role = "catalog-recovered-bookmark",
                        PageNumber = evidence.IndexPage,
                        TargetPageNumber = identity.PageNumber,
                        TargetRowIndex = identity.RowIndex,
                        ObjectId = identity.Id,
                        Detail = evidence switch
                        {
                            { IdentityKey: "ParentIdName" } => "Reconstructed native ParentIdName bookmark and matching complete stored ParentId/Name key using the declared supported collation, complete audited index chain, bookmark ordering and key-routing evidence. The full signed Id is retained from that stored row. Source headers are unchanged and ownership remains ambiguous.",
                            { HasDamagedMovedOwner: true } => "Reconstructed native Id-index bookmark and matching stored row Id despite damaged moved-payload owners, using complete audited Id-index chain, bookmark ordering and key-routing evidence. The compressed bookmark has no single physical offset; source headers are unchanged and ownership remains ambiguous.",
                            _ => "Reconstructed native Id-index bookmark and matching stored row Id, with verified intact catalog owner headers on moved-payload pages; the compressed bookmark does not have a single physical byte offset. The physical owner header is unchanged and ownership remains ambiguous.",
                        },
                    },
                    cancellationToken).ConfigureAwait(false);
                if (recoveredCatalogVisitor is not null)
                {
                    await recoveredCatalogVisitor(evidence, cancellationToken).ConfigureAwait(false);
                    pages.ThrowIfDisposedOrCancelled(cancellationToken);
                }
            }

            foreach ((long page, string reason) in recovered.Diagnostics)
            {
                await this.DiagnosticAsync(page, reason, cancellationToken).ConfigureAwait(false);
            }

            rows.Sort(static (left, right) => left.PageNumber != right.PageNumber ? left.PageNumber.CompareTo(right.PageNumber) : left.RowIndex.CompareTo(right.RowIndex));
            if (rows.Count != catalog.Header.Counters.RowCount)
            {
                await this.DiagnosticAsync(2, "The advisory catalog row count differs from physically discovered live rows; all discovered identities are retained and all storage remains pinned.", cancellationToken).ConfigureAwait(false);
            }

            var roots = new HashSet<long> { 2 };
            foreach (CatalogRow row in rows)
            {
                if (row.Name.Length > 64)
                {
                    throw Corrupt(row.PageNumber, "A catalog object name exceeds the native name limit.");
                }

                bool local = row.ObjectType == Constants.SystemObjects.UserTableType
                    || (row.ObjectType == Constants.SystemObjects.LinkedOdbcType && row.Id > 0);
                await this.EmitAsync(
                new CompactStorageRecord
                {
                    Kind = "catalog-object",
                    Role = local ? "local-table" : "opaque-catalog-object",
                    PageNumber = row.PageNumber,
                    TargetRowIndex = row.RowIndex,
                    OwnerTDefPage = local ? row.TDefPage : null,
                    Name = row.Name,
                    ObjectType = row.ObjectType,
                    ObjectId = row.Id,
                    Flags = row.Flags,
                    Detail = "Catalog identity only; application, security and replication payloads remain uninterpreted and pinned.",
                },
                cancellationToken).ConfigureAwait(false);
                if (local)
                {
                    _ = roots.Add(row.TDefPage);
                }
            }

            int remainingBytes = budget.MaxSchemaBytes;
            foreach (long root in roots)
            {
                remainingBytes -= await this.ValidateDefinitionChainAsync(root, remainingBytes, cancellationToken).ConfigureAwait(false);
            }

            await catalogRows.ValidateTableReferencesAsync(rows, cancellationToken).ConfigureAwait(false);
            remainingBytes = budget.MaxSchemaBytes;
            foreach (long root in roots)
            {
                TDefImage image = await tableDefs.ReadImageAsync(root, remainingBytes, cancellationToken).ConfigureAwait(false)
                    ?? throw Corrupt(root, "A local table definition cannot be read.");
                remainingBytes -= image.LogicalByteLength;
                await this.ScanDefinitionAsync(root, image, cancellationToken).ConfigureAwait(false);
                if (definitionVisitor is not null)
                {
                    await definitionVisitor(root, image, cancellationToken).ConfigureAwait(false);
                    pages.ThrowIfDisposedOrCancelled(cancellationToken);
                }
            }
        }

        private async ValueTask<int> ValidateDefinitionChainAsync(long root, int remainingBytes, CancellationToken cancellationToken)
        {
            var seen = new HashSet<long>();
            int capacity = 0;
            int limit = Math.Min(remainingBytes, tableDefs.MaxLogicalBytes);
            long current = root;
            while (current != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (current < 2 || current >= pages.PageCount || seen.Contains(current))
                {
                    throw Corrupt(root, "A definition chain has an invalid or cyclic page reference.");
                }

                int length = capacity == 0 ? format.PageSize : format.PageSize - 8;
                if (capacity > limit - length)
                {
                    throw Limit("The definition chain exceeds its per-table or aggregate schema byte limit.", root);
                }

                budget.Accounting?.Charge(CompactDiscoveryCharge.GraphSteps, 1, current);
                _ = seen.Add(current);
                byte[] page = await pages.ReadPageAsync(current, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (page.Length < format.PageSize || page[0] != Constants.PageTypes.TableDefinition)
                    {
                        throw Corrupt(root, "A definition chain has a truncated page or invalid page type.");
                    }

                    capacity += length;
                    current = Ru32(page, 4);
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }

            return capacity;
        }

        private async ValueTask ScanPhysicalPagesAsync(CancellationToken cancellationToken)
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            for (long pageNumber = 0; pageNumber < pages.PageCount; pageNumber++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
                try
                {
                    hash.AppendData(page, 0, format.PageSize);
                    string role = pageNumber == 0 ? "database-header" : page[0] switch
                    {
                        Constants.PageTypes.Data => "data-or-map-or-lval",
                        Constants.PageTypes.TableDefinition => "table-definition-page",
                        Constants.PageTypes.IndexIntermediate => "index-intermediate-page",
                        Constants.PageTypes.IndexLeaf => "index-leaf-page",
                        Constants.PageTypes.UsageMap => "usage-bitmap-page",
                        Constants.PageTypes.Freed => "freed-marker-page",
                        _ => "opaque-page",
                    };
                    await this.EmitAsync(
                new CompactStorageRecord
                {
                    Kind = "page",
                    Role = role,
                    PageNumber = pageNumber,
                    ByteLength = format.PageSize,
                    Sha256 = ToHexStringNoSeparator(hash.GetHashAndReset()),
                },
                cancellationToken).ConfigureAwait(false);
                    if (role == "opaque-page")
                    {
                        await this.DiagnosticAsync(pageNumber, "An unrecognized page type remains opaque and pinned.", cancellationToken).ConfigureAwait(false);
                    }

                    if (pageNumber != 0 && page[0] == Constants.PageTypes.TableDefinition)
                    {
                        long next = Ru32(page, 4);
                        if (next != 0)
                        {
                            await this.ReferenceAsync(pageNumber, 4, "tdef-continuation", "uint32-le", next, null, null, null, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    else if (pageNumber > 1 && page[0] is Constants.PageTypes.Data or Constants.PageTypes.IndexIntermediate or Constants.PageTypes.IndexLeaf)
                    {
                        long owner = Ru32(page, 4);
                        if (owner is > 1 and not 0x4C41564C)
                        {
                            await this.ReferenceAsync(pageNumber, 4, "page-owner-tdef", "uint32-le", owner, null, owner, null, cancellationToken, allowInvalid: true).ConfigureAwait(false);
                            if (owner >= pages.PageCount)
                            {
                                await this.DiagnosticAsync(pageNumber, "The physical owner header points outside the database; its unresolved reference remains pinned and independent catalog identity evidence is still inspected.", cancellationToken).ConfigureAwait(false);
                            }
                        }
                    }
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }
        }

        private async ValueTask ScanDefinitionAsync(long root, TDefImage image, CancellationToken cancellationToken)
        {
            if (image.Issues != TDefParseIssues.None)
            {
                throw Corrupt(root, "A local definition has uncertain structural boundaries.");
            }

            LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(root, cancellationToken).ConfigureAwait(false);
            await this.EmitAsync(
                new CompactStorageRecord
                {
                    Kind = "table-definition",
                    Role = "raw-definition-pinned",
                    PageNumber = root,
                    OwnerTDefPage = root,
                    ByteLength = image.LogicalByteLength,
                    Detail = "Unknown header, descriptor, trailer and padding bytes are retained by the physical page hashes.",
                },
                cancellationToken).ConfigureAwait(false);
            await this.DefinitionMapAsync(root, chain, format.TDef.UsedPages, "table-owned", cancellationToken).ConfigureAwait(false);
            await this.DefinitionMapAsync(root, chain, format.TDef.FreePages, "table-free", cancellationToken).ConfigureAwait(false);
            foreach (TDefIndexDescriptor index in image.RealIndexes)
            {
                await this.DefinitionMapAsync(root, chain, index.Slot.FirstDpOffset - 4, "index-owned", cancellationToken).ConfigureAwait(false);
                if (index.RootPage != 0)
                {
                    await this.DefinitionReferenceAsync(root, chain, index.Slot.FirstDpOffset, "index-root", "uint32-le", index.RootPage, null, cancellationToken).ConfigureAwait(false);
                }
            }

            if (!LongValueUsageMaps.TryRead(format, chain.Bytes, out List<LongValueUsageMap> maps, out _))
            {
                await this.DiagnosticAsync(root, "The definition trailer is opaque; no additional LVAL map references are inferred.", cancellationToken).ConfigureAwait(false);
                return;
            }

            foreach (LongValueUsageMap map in maps)
            {
                await this.DefinitionMapAsync(root, chain, map.OwnedOffset, "lval-owned", cancellationToken).ConfigureAwait(false);
                await this.DefinitionMapAsync(root, chain, map.FreeOffset, "lval-free", cancellationToken).ConfigureAwait(false);
            }
        }

        private async ValueTask DefinitionMapAsync(long root, LogicalTDefChain chain, int offset, string role, CancellationToken cancellationToken)
        {
            if (!UsageMap.TryReadPointer(chain.Bytes, offset, out UsageMapPointer pointer))
            {
                throw Corrupt(root, "A definition map pointer is truncated.");
            }

            if (pointer.PageNumber == 0 && pointer.RowIndex == 0)
            {
                return;
            }

            bool catalogHint = root == 2 && role is "table-owned" or "table-free";
            await this.DefinitionReferenceAsync(root, chain, offset, role + "-map", "row8-page24-le", pointer.PageNumber, pointer.RowIndex, cancellationToken, allowInvalid: catalogHint).ConfigureAwait(false);
            if (catalogHint && (pointer.PageNumber <= 0 || pointer.PageNumber >= pages.PageCount))
            {
                await this.DiagnosticAsync(root, "The catalog allocation hint points outside usable physical storage and remains an unresolved pinned reference; independently recovered identities do not prove complete coverage.", cancellationToken).ConfigureAwait(false);
                return;
            }

            try
            {
                await this.ScanMapAsync(pointer.PageNumber, pointer.RowIndex, root, role, cancellationToken).ConfigureAwait(false);
            }
            catch (JetCorruptDataException error) when (root == 2 && role is "table-owned" or "table-free" && error.ErrorCode == JetErrorCode.CorruptTableDefinition)
            {
                await this.DiagnosticAsync(root, "The catalog allocation hint is malformed and remains pinned: " + error.Message + " Physical owners and independently matched Id-index bookmarks are retained; complete coverage is unproven.", cancellationToken).ConfigureAwait(false);
            }
        }

        private ValueTask DefinitionReferenceAsync(long root, LogicalTDefChain chain, int offset, string role, string encoding, long target, int? row, CancellationToken cancellationToken, bool allowInvalid = false)
        {
            (int pageIndex, int pageOffset) = LogicalTDefChain.LogicalToPhysicalOffset(format.PageSize, offset);
            return this.ReferenceAsync(chain.PageNumbers[pageIndex], pageOffset, role, encoding, target, row, root, offset, cancellationToken, allowInvalid);
        }

        private async ValueTask ScanMapAsync(long pageNumber, int rowIndex, long owner, string role, CancellationToken cancellationToken)
        {
            byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data
                    || !UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, rowIndex, out RowBound row)
                    || row.RowSize < Constants.UsageMap.InlineMapHeaderSize)
                {
                    throw Corrupt(owner, "A known usage-map row has invalid bounds or page type.");
                }

                if (page[row.RowStart] == Constants.UsageMap.InlineMapType)
                {
                    int first = Ri32(page, row.RowStart + 1);
                    if (first < 0 || (owner == 1 && first != 0))
                    {
                        throw Corrupt(owner, "A known inline usage map has an invalid base page.");
                    }

                    await this.ScanBitsAsync(pageNumber, page, row.RowStart + Constants.UsageMap.InlineBitmapOffset, row.RowStart + row.RowSize, first, owner, role, cancellationToken).ConfigureAwait(false);
                }
                else if (page[row.RowStart] == Constants.UsageMap.ReferenceMapType)
                {
                    var seen = new HashSet<long>();
                    int count = UsageMap.ReferencePointerCount(row.RowSize);
                    for (int index = 0; index < count; index++)
                    {
                        this.StepMap(pageNumber, cancellationToken);
                        int offset = row.RowStart + 1 + (index * 4);
                        long bitmapNumber = Ru32(page, offset);
                        if (bitmapNumber == 0)
                        {
                            continue;
                        }

                        if (bitmapNumber <= 1 || !seen.Add(bitmapNumber))
                        {
                            throw Corrupt(owner, "A usage map has a repeated or reserved bitmap reference.");
                        }

                        await this.ReferenceAsync(pageNumber, offset, role + "-bitmap", "uint32-le", bitmapNumber, null, owner, null, cancellationToken).ConfigureAwait(false);
                        byte[] bitmap = await pages.ReadPageAsync(bitmapNumber, cancellationToken).ConfigureAwait(false);
                        try
                        {
                            if (bitmap[0] != Constants.PageTypes.UsageMap || bitmap[1] != 1)
                            {
                                throw Corrupt(owner, "A usage-map bitmap has an invalid page type.");
                            }

                            await this.ScanBitsAsync(bitmapNumber, bitmap, Constants.UsageMap.ReferenceMapBitmapOffset, format.PageSize, (long)index * UsageMap.PagesPerReferenceMapPage(format.PageSize), owner, role, cancellationToken).ConfigureAwait(false);
                        }
                        finally
                        {
                            PageBuffers.Return(bitmap);
                        }
                    }
                }
                else
                {
                    throw Corrupt(owner, "A known usage map has an unrecognized map type.");
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        private async ValueTask ScanBitsAsync(long source, byte[] page, int firstOffset, int endOffset, long firstPage, long owner, string role, CancellationToken cancellationToken)
        {
            for (int offset = firstOffset; offset < endOffset; offset++)
            {
                this.StepMap(source, cancellationToken);
                for (int bit = 0; bit < 8; bit++)
                {
                    if ((page[offset] & (1 << bit)) == 0)
                    {
                        continue;
                    }

                    long target = firstPage + ((offset - firstOffset) * 8L) + bit;
                    if (owner == 1 && target >= pages.PageCount)
                    {
                        // Native global maps can mark unused capacity past EOF free.
                        continue;
                    }

                    await this.ReferenceAsync(source, offset, role + "-member", "bitmap-bit-" + bit, target, null, owner, null, cancellationToken).ConfigureAwait(false);
                    if (owner == 2 && role == "table-owned")
                    {
                        await this.ValidateCatalogMapCandidateAsync(target, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        private async ValueTask ValidateCatalogMapCandidateAsync(long pageNumber, CancellationToken cancellationToken)
        {
            byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] == Constants.PageTypes.Data && this.recoveredCatalogPages.Contains(pageNumber))
                {
                    return;
                }

                if (page[0] != Constants.PageTypes.Data || Ru32(page, format.DataPage.TDefOff) != 2)
                {
                    // A positive map bit cannot prove ownership, but contradicting
                    // physical evidence cannot prove that no catalog rows were lost.
                    // Keep the emitted edge and refuse rather than reinterpret rows
                    // or treat the physical-only discovery as a complete inventory.
                    const string reason = "A catalog owned-data map member has a conflicting page type or physical owner; complete catalog discovery cannot be established.";
                    throw new JetCorruptDataException(
                        JetErrorCode.CorruptCatalog,
                        reason,
                        new JetErrorInfo { TableName = Constants.SystemTableNames.Objects, PageNumber = pageNumber, Reason = reason });
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        private ValueTask ReferenceAsync(long source, int offset, string role, string encoding, long target, int? row, long? owner, int? logicalOffset, CancellationToken cancellationToken, bool allowInvalid = false)
        {
            bool valid = target >= 0 && target < pages.PageCount && (!row.HasValue || target > 0);
            if (!valid && !allowInvalid)
            {
                throw Corrupt(owner ?? source, "A known reference points outside the physical database.");
            }

            if (this.references >= budget.MaxReferences)
            {
                throw Limit("The known-reference count exceeds MaxReferences.", source);
            }

            budget.Accounting?.Charge(CompactDiscoveryCharge.References, 1, source);
            this.references++;
            return this.EmitAsync(
                new CompactStorageRecord
                {
                    Kind = valid ? "reference" : "unresolved-reference",
                    Role = role,
                    PageNumber = source,
                    ByteOffset = offset,
                    ByteLength = encoding.StartsWith("bitmap-", StringComparison.Ordinal) ? 1 : 4,
                    LogicalOffset = logicalOffset,
                    Encoding = encoding,
                    TargetPageNumber = target,
                    TargetRowIndex = row,
                    OwnerTDefPage = owner,
                },
                cancellationToken);
        }

        private ValueTask DiagnosticAsync(long page, string reason, CancellationToken cancellationToken)
        {
            if (this.diagnostics >= budget.MaxDiagnostics)
            {
                throw Limit("The uncertainty diagnostic count exceeds MaxDiagnostics.", page);
            }

            budget.Accounting?.Charge(CompactDiscoveryCharge.Diagnostics, 1, page);
            this.diagnostics++;
            return this.EmitAsync(
                new CompactStorageRecord { Kind = "diagnostic", Role = "opaque-pinned", PageNumber = page, Detail = reason },
                cancellationToken);
        }

        private ValueTask EmitAsync(CompactStorageRecord record, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (this.records >= budget.MaxRecords)
            {
                throw Limit("The observation count exceeds MaxRecords.", record.PageNumber);
            }

            budget.Accounting?.Charge(CompactDiscoveryCharge.Records, 1, record.PageNumber);
            this.records++;
            return visitor(record, cancellationToken);
        }

        private void StepMap(long page, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (this.mapSteps >= budget.MaxMapSteps)
            {
                throw Limit("The usage-map traversal exceeds MaxMapSteps.", page);
            }

            budget.Accounting?.Charge(CompactDiscoveryCharge.MapSteps, 1, page);
            this.mapSteps++;
        }
    }
}
