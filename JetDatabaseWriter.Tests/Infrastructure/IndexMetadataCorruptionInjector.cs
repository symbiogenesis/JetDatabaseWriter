namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Injects malformed index metadata into a table for tests that verify
/// write operations refuse unmaintainable indexes without changing the file.
/// Every method patches pages through the writer's
/// <see cref="JetDatabaseWriter.Pages.Paging.Pager"/> and
/// <see cref="TDefWriter"/> directly, below any writer workflow.
/// </summary>
internal static class IndexMetadataCorruptionInjector
{
    /// <summary>
    /// The <c>col_num</c> <see cref="SetPhantomKeyColumnAsync"/> writes: column
    /// 768, which no table has (Access allows 255 columns).
    /// </summary>
    public const int PhantomColumnNumber = 0x0300;

    /// <summary>
    /// Injects a misplaced <c>used_pages</c> row byte into wide Jet4 and ACE
    /// tables. Each real index's logical <c>used_pages</c> offset <c>L</c> is
    /// split into a page and an offset by plain division by the page size,
    /// ignoring the 8-byte header of each TDEF continuation page. The row
    /// byte (real index + 2) lands at physical offset <c>L % pageSize</c> of
    /// chain page <c>L / pageSize</c>, 8 bytes early per continuation page,
    /// and the real row byte is set to 0.
    /// One continuation page early is the high byte of <c>col_map</c> slot 7,
    /// which then reads as key column <c>0x(ri+2)FF</c>, a column the table
    /// does not have. When <c>L</c> is a multiple of the page size, the byte
    /// lands on the page-type byte of that continuation page instead (the
    /// write-atomicity probe's header hit with offset 0). The page then no
    /// longer reads as a TDEF page, so reading the chain is refused rather
    /// than exposing a partial schema.
    /// </summary>
    /// <param name="writer">The open writer layers.</param>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The real-index numbers that now name a missing key column, in ascending order, or an empty list when the injected header damage makes the entire chain unreadable.</returns>
    /// <exception cref="InvalidOperationException">The database is Jet3, or a stray byte would land in a continuation page's header past its page-type byte.</exception>
    public static async ValueTask<IReadOnlyList<int>> InjectStrayUsedPagesByteAsync(WriterHarness writer, long tdefPage, CancellationToken cancellationToken)
    {
        DatabaseFile db = writer.Database;
        if (db.Format.Kind == DatabaseFormat.Jet3Mdb)
        {
            throw new InvalidOperationException("The misplaced used_pages byte injector supports only Jet4 and ACE tables.");
        }

        LogicalTDefChain chain = await db.TableDefs.ReadTDefChainAsync(tdefPage, cancellationToken);
        byte[] td = chain.Bytes;
        int pageSize = db.Format.PageSize;
        int realIdxDescStart = LocateRealIdxDescStart(db, td, out int numRealIdx);

        var strayBytes = new List<(int PageIndex, int Offset, byte Value)>();
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            if (!db.Format.Index.TryReadRealIdxSlot(td, realIdxDescStart, ri, out RealIdxSlot slot))
            {
                throw new InvalidOperationException($"Real index {ri} of the table at page {tdefPage} could not be read.");
            }

            int usedPagesOffset = slot.FirstDpOffset - 4;
            if (usedPagesOffset < pageSize)
            {
                // On the first page the logical and physical offsets agree.
                continue;
            }

            int pageIndex = usedPagesOffset / pageSize;
            int offset = usedPagesOffset % pageSize;

            // At offset 0 the byte overwrites the continuation page's type.
            // Offsets 1 to 7 hold the rest of the page header, including the
            // next-page pointer; the injector avoids those offsets.
            if ((offset is > 0 and < 8) || pageIndex >= chain.PageNumbers.Count)
            {
                throw new InvalidOperationException(
                    $"The stray byte of real index {ri} would land at offset {offset} of TDEF page {pageIndex}, in its page header past the page type; choose another column count.");
            }

            td[usedPagesOffset] = 0;
            strayBytes.Add((pageIndex, offset, checked((byte)(ri + 2))));
        }

        await writer.Services.TDefWriter.WriteChainInPlaceAsync(chain, cancellationToken);
        foreach ((int pageIndex, int offset, byte value) in strayBytes)
        {
            long pageNumber = chain.PageNumbers[pageIndex];
            byte[] page = await db.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);
            page[offset] = value;
            await writer.Pager.WritePageAsync(pageNumber, page, cancellationToken);
        }

        return strayBytes.Exists(stray => stray.Offset == 0)
            ? []
            : await FindPhantomIndexesAsync(db, tdefPage, cancellationToken);
    }

    /// <summary>
    /// Writes <see cref="PhantomColumnNumber"/> into <c>col_map</c> slot 1 of
    /// real index <paramref name="realIndexNumber"/>, so the index names a
    /// second key column the table does not have. This injects the phantom
    /// column directly on formats where a misplaced row byte cannot do so.
    /// </summary>
    /// <param name="writer">The open writer layers.</param>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="realIndexNumber">The real index to damage.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the TDEF has been written.</returns>
    /// <exception cref="InvalidOperationException">The real index does not exist, or its slot 1 already names a column.</exception>
    public static async ValueTask SetPhantomKeyColumnAsync(WriterHarness writer, long tdefPage, int realIndexNumber, CancellationToken cancellationToken)
    {
        DatabaseFile db = writer.Database;
        LogicalTDefChain chain = await db.TableDefs.ReadTDefChainAsync(tdefPage, cancellationToken);
        byte[] td = chain.Bytes;
        int realIdxDescStart = LocateRealIdxDescStart(db, td, out int numRealIdx);
        if (realIndexNumber < 0 || realIndexNumber >= numRealIdx
            || !db.Format.Index.TryReadRealIdxSlot(td, realIdxDescStart, realIndexNumber, out RealIdxSlot slot))
        {
            throw new InvalidOperationException($"The table at page {tdefPage} has no real index {realIndexNumber}.");
        }

        int slotOffset = db.Format.Index.ColMapSlotOffset(slot.PhysStart, 1);
        if (Ru16(td, slotOffset) != Constants.TableDefinition.ColMapPaddingSlot)
        {
            throw new InvalidOperationException($"Real index {realIndexNumber} already has a second key column.");
        }

        Wu16(td, slotOffset, PhantomColumnNumber);
        await writer.Services.TDefWriter.WriteChainInPlaceAsync(chain, cancellationToken);
    }

    /// <summary>Returns the real indexes of the table whose <c>col_map</c> names a column the table does not have.</summary>
    /// <param name="db">The open database file.</param>
    /// <param name="tdefPage">The table's first TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The real-index numbers, in ascending order.</returns>
    /// <exception cref="InvalidOperationException">The table definition cannot be read or walked.</exception>
    public static async ValueTask<IReadOnlyList<int>> FindPhantomIndexesAsync(DatabaseFile db, long tdefPage, CancellationToken cancellationToken)
    {
        TableDef tableDef = await db.TableDefs.ReadTableDefAsync(tdefPage, cancellationToken)
            ?? throw new InvalidOperationException($"The table definition at page {tdefPage} could not be read.");
        var columnNumbers = new HashSet<int>();
        foreach (ColumnInfo column in tableDef.Columns)
        {
            columnNumbers.Add(column.ColNum);
        }

        byte[] td = (await db.TableDefs.ReadTDefChainAsync(tdefPage, cancellationToken)).Bytes;
        int realIdxDescStart = LocateRealIdxDescStart(db, td, out int numRealIdx);
        var phantoms = new List<int>();
        for (int ri = 0; ri < numRealIdx; ri++)
        {
            if (db.Format.Index.TryReadRealIdxSlotWithKeyColumns(td, realIdxDescStart, ri, out _, out List<KeyColumn> keyColumns)
                && keyColumns.Exists(key => !columnNumbers.Contains(key.ColNum)))
            {
                phantoms.Add(ri);
            }
        }

        return phantoms;
    }

    private static int LocateRealIdxDescStart(DatabaseFile db, byte[] td, out int numRealIdx)
    {
        int numCols = Ru16(td, db.Format.TDef.NumCols);
        numRealIdx = Ri32(td, db.Format.TDef.NumRealIdx);
        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(db.Format, td, numCols, numRealIdx);
        return realIdxDescStart >= 0
            ? realIdxDescStart
            : throw new InvalidOperationException("The column-name section of the table definition could not be walked.");
    }
}
