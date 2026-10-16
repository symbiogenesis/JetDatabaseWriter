namespace JetDatabaseWriter.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Decides whether the writer may extend a local table's owned-page usage map.
/// User tables, MSysObjects and Jet4/ACE system tables need allocated data pages listed
/// so native scans and compact retain their rows. Their rows and bitmap pages are
/// proved exclusive, isolating shared native maps on demand. Other imported Jet3 system maps
/// remain untouched. Decisions are cached by TDEF page and restored on rollback;
/// registration also permits a newly created table at a formerly refused page.
/// </summary>
/// <param name="format">The file's format profile.</param>
/// <param name="tableDefs">Reads the <c>MSysObjects</c> table definition.</param>
/// <param name="catalogRows">Reads the <c>MSysObjects</c> rows that name a table's TDEF page.</param>
/// <param name="pages">Reads physical map rows while proving exclusive ownership.</param>
/// <param name="pager">Writes isolated map storage.</param>
/// <param name="allocator">Allocates private map pages.</param>
/// <param name="tdefWriter">Retargets table map pointers.</param>
internal sealed class CatalogOwnedMapPolicy(JetFormat format, TableDefReader tableDefs, CatalogRowReader catalogRows, IPageSource pages, Pager pager, PageAllocator allocator, TDefWriter tdefWriter) : IOwnedMapPolicy, IPageWriteObserver
{
#if NET9_0_OR_GREATER
    private readonly Lock ownedMapSetsLock = new();
#else
    private readonly object ownedMapSetsLock = new();
#endif
    private readonly TDefImageCache definitionShape = new(format);
    private readonly HashSet<long> knownRoots = [2];
    private readonly HashSet<long> writableTdefs = [];
    private readonly HashSet<long> refusedTdefs = [];

    /// <inheritdoc/>
    public async ValueTask<bool> CanMaintainAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        if (tdefPageNumber <= 0)
        {
            return false;
        }

        if (this.TryGetDecision(tdefPageNumber, out bool decided))
        {
            return decided;
        }

        // MSysObjects has a fixed root and does not need a catalog lookup.
        if (tdefPageNumber == 2)
        {
            await this.EnsureExclusiveMapsAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
            this.RecordDecision(tdefPageNumber, true);

            return true;
        }

        bool writable = await this.IsLocalTableAsync(tdefPageNumber, cancellationToken).ConfigureAwait(false);
        this.RecordDecision(tdefPageNumber, writable);
        return writable;
    }

    /// <inheritdoc/>
    public void RegisterWritable(long tdefPageNumber)
    {
        lock (this.ownedMapSetsLock)
        {
            _ = this.knownRoots.Add(tdefPageNumber);
            _ = this.writableTdefs.Add(tdefPageNumber);
            _ = this.refusedTdefs.Remove(tdefPageNumber);
        }
    }

    /// <inheritdoc/>
    public OwnedMapPolicyState Capture()
    {
        lock (this.ownedMapSetsLock)
        {
            return new OwnedMapPolicyState([.. this.writableTdefs], [.. this.refusedTdefs]);
        }
    }

    /// <inheritdoc/>
    public void Restore(OwnedMapPolicyState state)
    {
        lock (this.ownedMapSetsLock)
        {
            this.knownRoots.Clear();
            _ = this.knownRoots.Add(2);
            this.knownRoots.UnionWith(state.WritableTdefs);
            this.knownRoots.UnionWith(state.RefusedTdefs);
            this.writableTdefs.Clear();
            this.writableTdefs.UnionWith(state.WritableTdefs);
            this.refusedTdefs.Clear();
            this.refusedTdefs.UnionWith(state.RefusedTdefs);
        }
    }

    /// <inheritdoc/>
    public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
    {
        bool invalidate = false;
        if (before.Length == format.PageSize && before[0] == Constants.PageTypes.TableDefinition)
        {
            bool knownRoot;
            lock (this.ownedMapSetsLock)
            {
                knownRoot = this.knownRoots.Contains(pageNumber);
                if (after.Length == 0 || after[0] != Constants.PageTypes.TableDefinition)
                {
                    _ = this.knownRoots.Remove(pageNumber);
                }
            }

            invalidate = knownRoot
                ? !this.definitionShape.StructuralBytesEqual(before, after)
                : !before.SequenceEqual(after);
        }
        else if (before.Length == format.PageSize && after.Length == format.PageSize
            && before[0] == Constants.PageTypes.Data && Ri32(before, format.DataPage.TDefOff) == 0)
        {
            invalidate = this.MapReferencesChanged(before, after);
        }

        if (invalidate)
        {
            lock (this.ownedMapSetsLock)
            {
                this.writableTdefs.Clear();
                this.refusedTdefs.Clear();
            }
        }
    }

    /// <inheritdoc/>
    public void OnInvalidateAll()
    {
        lock (this.ownedMapSetsLock)
        {
            this.knownRoots.Clear();
            _ = this.knownRoots.Add(2);
            this.writableTdefs.Clear();
            this.refusedTdefs.Clear();
        }
    }

    /// <summary>
    /// Returns whether the first <c>MSysObjects</c> table row that names
    /// <paramref name="tdefPageNumber"/> is a named local table.
    /// </summary>
    /// <param name="tdefPageNumber">The table's TDEF page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> for a local table; <see langword="false"/> for an unnamed, nonlocal or unrepresented table.</returns>
    private async ValueTask<bool> IsLocalTableAsync(long tdefPageNumber, CancellationToken cancellationToken)
    {
        TableDef? msys = await tableDefs.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys is null)
        {
            return false;
        }

        int maxEntries = format.IsJet3 ? Math.Max(1, tableDefs.MaxLogicalBytes / format.TDef.BlockEnd) : int.MaxValue;
        List<CatalogRow> rows = await catalogRows.GetCatalogRowsAsync(msys, cancellationToken, maxEntries).ConfigureAwait(false);
        lock (this.ownedMapSetsLock)
        {
            foreach (CatalogRow local in rows)
            {
                if (local.ObjectType == Constants.SystemObjects.UserTableType
                    || (local.ObjectType == Constants.SystemObjects.LinkedOdbcType && local.Id > 0))
                {
                    _ = this.knownRoots.Add(local.TDefPage);
                }
            }
        }

        foreach (CatalogRow row in rows)
        {
            if (row.TDefPage == tdefPageNumber && row.ObjectType == Constants.SystemObjects.UserTableType)
            {
                if (!format.IsJet3 || (unchecked((uint)row.Flags) & Constants.SystemObjects.SystemTableMask) == 0)
                {
                    if (string.IsNullOrEmpty(row.Name))
                    {
                        return false;
                    }

                    await this.EnsureExclusiveMapsAsync(row.TDefPage, cancellationToken).ConfigureAwait(false);
                    return true;
                }

                if (string.Equals(row.Name, Constants.SystemTableNames.Aces, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(row.Name, Constants.SystemTableNames.Queries, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(row.Name, Constants.SystemTableNames.Relationships, StringComparison.OrdinalIgnoreCase))
                {
                    await this.EnsureExclusiveMapsAsync(row.TDefPage, cancellationToken).ConfigureAwait(false);
                    return true;
                }

                return false;
            }
        }

        return false;
    }

    private async ValueTask EnsureExclusiveMapsAsync(long tdefPage, CancellationToken cancellationToken)
    {
        UsageMapOwnership.Proof owned = await UsageMapOwnership.InspectAsync(format, tableDefs, catalogRows, pages, tdefPage, format.TDef.UsedPages, cancellationToken).ConfigureAwait(false);
        UsageMapOwnership.Proof free = await UsageMapOwnership.InspectAsync(format, tableDefs, catalogRows, pages, tdefPage, format.TDef.FreePages, cancellationToken).ConfigureAwait(false);
        bool copyOwned = !owned.IsExclusiveRow || !owned.HasExclusiveBitmapPages;
        bool copyFree = !free.IsExclusiveRow || !free.HasExclusiveBitmapPages;
        if (!copyOwned && !copyFree)
        {
            return;
        }

        // Shared native maps can omit this table's pages or contain another
        // table's pages. Reconstruct both memberships from physical ownership
        // before detaching, while preserving every byte of the shared source.
        (List<long> ownedPages, List<long> freePages) = await this.ReadTableMapMembersAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        var ownedCopy = new UsageMapOwnership.Proof(owned.Pointer, owned.RowSize, ownedPages, owned.BitmapPages, exclusiveRow: true);
        var freeCopy = new UsageMapOwnership.Proof(free.Pointer, free.RowSize, freePages, free.BitmapPages, exclusiveRow: true);
        UsageMapPointer ownedPointer = owned.Pointer;
        UsageMapPointer freePointer = free.Pointer;
        try
        {
            ownedPointer = await UsageMapOwnership.CreatePrivateCopyAsync(format, pager, allocator, ownedCopy, cancellationToken).ConfigureAwait(false);
            freePointer = await UsageMapOwnership.CreatePrivateCopyAsync(format, pager, allocator, freeCopy, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (ownedPointer != owned.Pointer)
            {
                await UsageMapOwnership.ReleasePrivateCopyAsync(format, pager, allocator, ownedPointer).ConfigureAwait(false);
            }

            throw;
        }

        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        UsageMap.WritePointer(chain.Bytes, format.TDef.UsedPages, ownedPointer.RowIndex, ownedPointer.PageNumber);
        UsageMap.WritePointer(chain.Bytes, format.TDef.FreePages, freePointer.RowIndex, freePointer.PageNumber);
        await tdefWriter.WriteChainInPlaceAsync(chain, cancellationToken).ConfigureAwait(false);
        var candidates = new HashSet<long>(owned.BitmapPages);
        candidates.UnionWith(free.BitmapPages);
        _ = candidates.Add(owned.Pointer.PageNumber);
        _ = candidates.Add(free.Pointer.PageNumber);
        HashSet<long> unreferenced = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(format, tableDefs, catalogRows, pages, candidates, ignoredTdefPage: 0, cancellationToken).ConfigureAwait(false);
        foreach (long pageNumber in unreferenced)
        {
            await allocator.DeallocatePageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<(List<long> Owned, List<long> Free)> ReadTableMapMembersAsync(long owner, CancellationToken cancellationToken)
    {
        var owned = new List<long>();
        var free = new List<long>();
        var freePages = new List<long>();
        byte[] global = await pages.ReadPageAsync(1, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!UsageMap.TryGetFirstRowBound(global, format.DataPage, format.PageSize, out RowBound row)
                || !await UsageMap.TryEnumeratePagesAsync(global, row, format.PageSize, pages.PageCount, 2, false, pages.ReadPageAsync, PageBuffers.Return, freePages, cancellationToken).ConfigureAwait(false))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptTableDefinition, "The global usage map is invalid.", new JetErrorInfo { PageNumber = 1 });
            }
        }
        finally
        {
            PageBuffers.Return(global);
        }

        var globallyFree = new HashSet<long>(freePages);
        for (long pageNumber = 2; pageNumber < pages.PageCount; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (globallyFree.Contains(pageNumber))
            {
                continue;
            }

            byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data || Ri32(page, format.DataPage.TDefOff) != owner)
                {
                    continue;
                }

                owned.Add(pageNumber);
                int rowCount = Ru16(page, format.DataPage.NumRows);
                int slotTableEnd = format.DataPage.RowsStart + (rowCount * 2);
                if (slotTableEnd > format.PageSize)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptTableDefinition, "A table page has an invalid row directory.", new JetErrorInfo { PageNumber = pageNumber });
                }

                int first = format.PageSize;
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
                {
                    int raw = Ru16(page, format.DataPage.RowsStart + (rowIndex * 2));
                    int start = raw & Constants.DataPage.RowOffsetMask;
                    if (start == 0 || (start == format.PageSize && (raw & Constants.DataPage.DeletedRowFlag) != 0))
                    {
                        continue;
                    }

                    if (start < slotTableEnd || start >= format.PageSize)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptTableDefinition, "A table row lies outside its row storage.", new JetErrorInfo { PageNumber = pageNumber });
                    }

                    first = Math.Min(first, start);
                }

                if (rowCount < Constants.DataPage.MaxRowsPerPage && first - slotTableEnd - 2 >= 1)
                {
                    free.Add(pageNumber);
                }
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return (owned, free);
    }

    private bool MapReferencesChanged(ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
    {
        if (after[0] != Constants.PageTypes.Data || Ri32(after, format.DataPage.TDefOff) != 0)
        {
            return true;
        }

        int rowCount = Ru16(before, format.DataPage.NumRows);
        int slotsLength = rowCount * 2;
        if (rowCount != Ru16(after, format.DataPage.NumRows)
            || slotsLength > format.PageSize - format.DataPage.RowsStart
            || !before.Slice(format.DataPage.RowsStart, slotsLength).SequenceEqual(after.Slice(format.DataPage.RowsStart, slotsLength)))
        {
            return true;
        }

        for (int index = 0; index < rowCount; index++)
        {
            if (!UsageMap.TryGetRowBound(before, format.DataPage, format.PageSize, index, out RowBound row))
            {
                return true;
            }

            if (before[row.RowStart] != after[row.RowStart]
                || (before[row.RowStart] == Constants.UsageMap.ReferenceMapType
                    && !before.Slice(row.RowStart + 1, row.RowSize - 1).SequenceEqual(after.Slice(row.RowStart + 1, row.RowSize - 1))))
            {
                return true;
            }
        }

        return false;
    }

    private bool TryGetDecision(long tdefPageNumber, out bool writable)
    {
        lock (this.ownedMapSetsLock)
        {
            writable = this.writableTdefs.Contains(tdefPageNumber);
            return writable || this.refusedTdefs.Contains(tdefPageNumber);
        }
    }

    private void RecordDecision(long tdefPageNumber, bool writable)
    {
        lock (this.ownedMapSetsLock)
        {
            HashSet<long> decided = writable ? this.writableTdefs : this.refusedTdefs;
            _ = decided.Add(tdefPageNumber);
        }
    }
}
