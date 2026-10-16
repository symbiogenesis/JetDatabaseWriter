namespace JetDatabaseWriter.LongValues;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Maintains each long-value column's independent native owned and free map pointers.</summary>
/// <param name="format">The database format.</param>
/// <param name="pager">The transaction-aware page writer.</param>
/// <param name="allocator">The page allocator.</param>
/// <param name="tableDefs">The logical definition reader.</param>
/// <param name="catalogRows">The catalog reader used to prove map ownership.</param>
/// <param name="catalog">Reads persisted calculated result types when their descriptor alone does not identify long storage.</param>
internal sealed class LongValueUsageMapMaintainer(JetFormat format, Pager pager, PageAllocator allocator, TableDefReader tableDefs, CatalogRowReader catalogRows, TableCatalog catalog)
{
    private readonly UsageMapEditor editor = new(format, pager, allocator);

    /// <summary>Adds native map records for long columns of a newly created or native definition.</summary>
    /// <param name="format">The format profile.</param>
    /// <param name="pager">The transaction-aware page writer.</param>
    /// <param name="allocator">The page allocator.</param>
    /// <param name="tdefPage">The table identity.</param>
    /// <param name="definition">The column layout, including persisted calculated result types.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completed when the trailer and new map rows are linked.</returns>
    /// <exception cref="InvalidDataException">The native trailer or column layout cannot be read completely.</exception>
    internal static async ValueTask InitializeAsync(JetFormat format, Pager pager, PageAllocator allocator, long tdefPage, TableDef definition, CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(tdefPage, format.PageSize, pager.ReadPageAsync, PageBuffers.Return, true, cancellationToken).ConfigureAwait(false);
        if (!LongValueUsageMaps.TryRead(format, chain.Bytes, out List<LongValueUsageMap> maps, out int sentinel))
        {
            throw new InvalidDataException("The long-value usage-map trailer is malformed.");
        }

        var missing = definition.Columns.Where(column => (ResolveValueType(column) is MemoType or OleType) && !maps.Any(map => map.ColumnNumber == column.ColNum)).ToList();
        if (missing.Count == 0)
        {
            return;
        }

        UsageMapPointer[] rows = await LongValueUsageMaps.CreateRowsAsync(format, allocator, missing.Count * 2, cancellationToken).ConfigureAwait(false);
        int oldEnd = checked(Ri32(chain.Bytes, 8) + 8);
        bool needsSentinel = sentinel == oldEnd;
        int added = checked((missing.Count * 10) + (needsSentinel ? 10 : 0));
        int newEnd = checked(oldEnd + added);
        byte[] bytes = chain.EnsureCapacity(newEnd);
        Buffer.BlockCopy(bytes, sentinel, bytes, sentinel + (missing.Count * 10), oldEnd - sentinel);
        for (int i = 0; i < missing.Count; i++)
        {
            int offset = sentinel + (i * 10);
            Wu16(bytes, offset, missing[i].ColNum);
            WritePointer(bytes, offset + 2, rows[i * 2]);
            WritePointer(bytes, offset + 6, rows[(i * 2) + 1]);
        }

        if (needsSentinel)
        {
            Array.Clear(bytes, newEnd - 10, 10);
            Wu16(bytes, newEnd - 10, ushort.MaxValue);
        }

        await chain.WriteAsync(bytes, newEnd, allocator.AllocatePageAsync, pager.WritePageAsync, allocator.DeallocatePageAsync, format.WritesTDefFreeSpace, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Proves and, when necessary, isolates the selected column's map rows before allocation.</summary>
    /// <param name="tdefPage">The table identity.</param>
    /// <param name="columnNumber">The row-layout column number.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The owned and free row pointers.</returns>
    /// <exception cref="InvalidDataException">The table or selected column does not declare readable long-value storage.</exception>
    internal async ValueTask<(UsageMapPointer Owned, UsageMapPointer Free)> PrepareColumnAsync(long tdefPage, int columnNumber, CancellationToken cancellationToken)
    {
        List<LongValueUsageMap> maps = await this.ReadMapsAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (!maps.Any(entry => entry.ColumnNumber == columnNumber))
        {
            // Access can retain a Text descriptor for a calculated Memo. Its
            // persisted ResultType establishes long storage after the catalog
            // entry exists, including a schema rewrite's private replacement.
            TableDef definition = await catalog.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidDataException("The long-value column definitions are malformed.");
            if (!definition.Columns.Any(column => column.ColNum == columnNumber && !column.IsFixed && ResolveValueType(column) is MemoType or OleType))
            {
                throw new InvalidDataException("The selected column does not declare long-value storage.");
            }

            await InitializeAsync(format, pager, allocator, tdefPage, definition, cancellationToken).ConfigureAwait(false);
            maps = await this.ReadMapsAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        }

        LongValueUsageMap map = maps.Single(entry => entry.ColumnNumber == columnNumber);
        UsageMapPointer owned = await this.PreparePointerAsync(tdefPage, map.OwnedOffset, cancellationToken).ConfigureAwait(false);
        UsageMapPointer free = await this.PreparePointerAsync(tdefPage, map.FreeOffset, cancellationToken).ConfigureAwait(false);
        return (owned, free);
    }

    /// <summary>Prepares all existing column maps before releasing a table row's external values.</summary>
    /// <param name="tdefPage">The table identity.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Independent column map pointers.</returns>
    internal async ValueTask<List<(UsageMapPointer Owned, UsageMapPointer Free)>> PrepareTableAsync(long tdefPage, CancellationToken cancellationToken)
    {
        List<LongValueUsageMap> maps = await this.ReadMapsAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        var result = new List<(UsageMapPointer Owned, UsageMapPointer Free)>(maps.Count);
        foreach (LongValueUsageMap map in maps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            result.Add((await this.PreparePointerAsync(tdefPage, map.OwnedOffset, cancellationToken).ConfigureAwait(false), await this.PreparePointerAsync(tdefPage, map.FreeOffset, cancellationToken).ConfigureAwait(false)));
        }

        return result;
    }

    /// <summary>Records an allocated LVAL page and its actual remaining row capacity.</summary>
    /// <param name="maps">The column's independent maps.</param>
    /// <param name="pageNumber">The allocated page.</param>
    /// <param name="hasSpace">Whether another nonempty LVAL row and slot fit.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completed after updating the membership.</returns>
    internal async ValueTask MarkPageAsync((UsageMapPointer Owned, UsageMapPointer Free) maps, long pageNumber, bool hasSpace, CancellationToken cancellationToken)
    {
        await this.editor.MarkPageAsync(maps.Owned.PageNumber, [maps.Owned.RowIndex], pageNumber, cancellationToken).ConfigureAwait(false);
        if (hasSpace)
        {
            await this.editor.MarkPageAsync(maps.Free.PageNumber, [maps.Free.RowIndex], pageNumber, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await this.editor.ClearPageAsync(maps.Free.PageNumber, maps.Free.RowIndex, pageNumber, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Clears a released page from every column that formerly owned it.</summary>
    /// <param name="maps">The table's independent column maps.</param>
    /// <param name="pageNumber">The page released to the global allocator.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completed after clearing membership.</returns>
    internal async ValueTask ClearPageAsync(IReadOnlyList<(UsageMapPointer Owned, UsageMapPointer Free)> maps, long pageNumber, CancellationToken cancellationToken)
    {
        foreach ((UsageMapPointer owned, UsageMapPointer free) in maps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await this.editor.ClearPageAsync(owned.PageNumber, owned.RowIndex, pageNumber, cancellationToken).ConfigureAwait(false);
            await this.editor.ClearPageAsync(free.PageNumber, free.RowIndex, pageNumber, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Appends a new LVAL row to an existing page named by this column's free map.</summary>
    /// <param name="maps">The column's prepared owned and free maps.</param>
    /// <param name="source">A one-row page carrying the new row's exact bytes.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The new row pointer, or zero when no existing page fits.</returns>
    internal async ValueTask<uint> TryAppendRowAsync((UsageMapPointer Owned, UsageMapPointer Free) maps, byte[] source, CancellationToken cancellationToken)
    {
        List<long> candidates = await this.ReadMembershipAsync(maps.Free, cancellationToken).ConfigureAwait(false);
        List<long> owned = await this.ReadMembershipAsync(maps.Owned, cancellationToken).ConfigureAwait(false);
        int sourceStart = Ru16(source, format.DataPage.RowsStart);
        int length = format.PageSize - sourceStart;
        foreach (long pageNumber in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!owned.Contains(pageNumber))
            {
                continue;
            }

            byte[] page = await pager.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                int rows = Ru16(page, format.DataPage.NumRows);
                if (!LongValueStore.IsLvalPage(page) || rows >= Constants.DataPage.MaxRowsPerPage)
                {
                    continue;
                }

                int first = format.PageSize;
                for (int row = 0; row < rows; row++)
                {
                    int offset = Ru16(page, format.DataPage.RowsStart + (row * 2)) & Constants.DataPage.RowOffsetMask;
                    if (offset > 0 && offset < first)
                    {
                        first = offset;
                    }
                }

                int rowStart = first - length;
                int freeSpace = rowStart - format.DataPage.RowsStart - ((rows + 1) * 2);
                if (freeSpace < 0)
                {
                    continue;
                }

                Buffer.BlockCopy(source, sourceStart, page, rowStart, length);
                Wu16(page, format.DataPage.RowsStart + (rows * 2), rowStart);
                Wu16(page, format.DataPage.NumRows, rows + 1);
                Wu16(page, 2, freeSpace);
                await pager.WritePageAsync(pageNumber, page, cancellationToken).ConfigureAwait(false);
                await this.MarkPageAsync(maps, pageNumber, freeSpace >= 3 && rows + 1 < Constants.DataPage.MaxRowsPerPage, cancellationToken).ConfigureAwait(false);
                return LongValueStore.MakeRowPointer(pageNumber, rows);
            }
            finally
            {
                PageBuffers.Return(page);
            }
        }

        return 0;
    }

    /// <summary>Restores a partially occupied LVAL page to the free map after deletion.</summary>
    /// <param name="maps">The table's prepared column maps.</param>
    /// <param name="pageNumber">The surviving LVAL page.</param>
    /// <param name="hasSpace">Whether another row and directory slot fit.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completed after updating each owning column.</returns>
    internal async ValueTask RefreshFreeSpaceAsync(IReadOnlyList<(UsageMapPointer Owned, UsageMapPointer Free)> maps, long pageNumber, bool hasSpace, CancellationToken cancellationToken)
    {
        foreach ((UsageMapPointer owned, UsageMapPointer free) in maps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if ((await this.ReadMembershipAsync(owned, cancellationToken).ConfigureAwait(false)).Contains(pageNumber))
            {
                await this.MarkPageAsync((owned, free), pageNumber, hasSpace, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static void WritePointer(byte[] bytes, int offset, UsageMapPointer pointer)
    {
        bytes[offset] = checked((byte)pointer.RowIndex);
        WriteUInt24(bytes, offset + 1, pointer.PageNumber);
    }

    private async ValueTask<List<long>> ReadMembershipAsync(UsageMapPointer pointer, CancellationToken cancellationToken)
    {
        byte[] map = await pager.ReadPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            var members = new List<long>();
            if (!UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row)
                || !await UsageMap.TryEnumeratePagesAsync(map, row, format.PageSize, pager.PageCount, 1, true, pager.ReadPageAsync, PageBuffers.Return, members, cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidDataException("The column's LVAL usage-map members are malformed.");
            }

            return members;
        }
        finally
        {
            PageBuffers.Return(map);
        }
    }

    private async ValueTask<List<LongValueUsageMap>> ReadMapsAsync(long tdefPage, CancellationToken cancellationToken)
    {
        byte[] tdef = await tableDefs.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException("The long-value table definition is unreadable.");
        if (!LongValueUsageMaps.TryRead(format, tdef, out List<LongValueUsageMap> maps, out _))
        {
            throw new InvalidDataException("The long-value usage-map trailer is malformed.");
        }

        return maps;
    }

    private async ValueTask<UsageMapPointer> PreparePointerAsync(long tdefPage, int offset, CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        _ = UsageMap.TryReadPointer(chain.Bytes, offset, out UsageMapPointer pointer);
        if (pointer.PageNumber != 0)
        {
            UsageMapOwnership.Proof proof = await UsageMapOwnership.InspectForMutationAsync(format, tableDefs, catalogRows, pager, tdefPage, offset, cancellationToken).ConfigureAwait(false);
            if (proof.IsExclusiveRow && proof.HasExclusiveBitmapPages)
            {
                return pointer;
            }

            pointer = await UsageMapOwnership.CreatePrivateCopyAsync(format, pager, allocator, proof, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            pointer = (await LongValueUsageMaps.CreateRowsAsync(format, allocator, 1, cancellationToken).ConfigureAwait(false))[0];
        }

        WritePointer(chain.Bytes, offset, pointer);
        await chain.WriteInPlaceAsync(pager.ReadPageAsync, PageBuffers.Return, pager.WritePageAsync, cancellationToken).ConfigureAwait(false);
        return pointer;
    }
}
