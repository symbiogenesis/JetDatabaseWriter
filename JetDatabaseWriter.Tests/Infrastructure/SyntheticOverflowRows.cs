namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using Xunit;

/// <summary>
/// Rewrites a writer-created table's row the way Access stores a row that outgrew
/// its page: the row's bytes move to a new row-offset slot flagged deleted
/// (<c>0x8000</c>), and the original slot is flagged overflow (<c>0x4000</c>) and
/// holds a 4-byte pointer, the target row index followed by the target page as a
/// little-endian 24-bit number. Works on an unencrypted database image in place.
/// </summary>
internal static class SyntheticOverflowRows
{
    private const int DeletedFlag = 0x8000;
    private const int OverflowFlag = 0x4000;
    private const int OffsetMask = 0x1FFF;

    /// <summary>
    /// Creates a database holding one table, <paramref name="tableName"/> (<c>Id</c>
    /// Long, <c>Name</c> Text), with at least <paramref name="minimumRows"/> rows
    /// (<c>Id</c> 1, 2, ...) spread over several data pages, and enough rows more
    /// that the last data page has room for two more rows, so a row can be moved there.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="minimumRows">The number of rows to insert at least.</param>
    /// <param name="primaryKey">Whether <c>Id</c> is the table's primary key.</param>
    /// <param name="cancellationToken">A token used to cancel the writes.</param>
    /// <returns>The database image.</returns>
    public static ValueTask<byte[]> CreateTableAsync(DatabaseFormat format, string tableName, int minimumRows, bool primaryKey, CancellationToken cancellationToken)
        => CreateTableAsync(format, tableName, minimumRows, primaryKey, extraColumns: [], extraValues: null, cancellationToken);

    /// <summary>
    /// Creates the table <see cref="CreateTableAsync(DatabaseFormat, string, int, bool, CancellationToken)"/>
    /// creates, with <paramref name="extraColumns"/> after <c>Id</c> and <c>Name</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="minimumRows">The number of rows to insert at least.</param>
    /// <param name="primaryKey">Whether <c>Id</c> is the table's primary key.</param>
    /// <param name="extraColumns">The columns that follow <c>Id</c> and <c>Name</c>.</param>
    /// <param name="extraValues">Returns the values of <paramref name="extraColumns"/> for an <c>Id</c>; <see langword="null"/> leaves them null.</param>
    /// <param name="cancellationToken">A token used to cancel the writes.</param>
    /// <returns>The database image.</returns>
    public static async ValueTask<byte[]> CreateTableAsync(
        DatabaseFormat format,
        string tableName,
        int minimumRows,
        bool primaryKey,
        IReadOnlyList<ColumnDefinition> extraColumns,
        Func<int, object[]>? extraValues,
        CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken))
        {
            await FillTableAsync(writer, ms, tableName, minimumRows, primaryKey, extraColumns, extraValues, cancellationToken);
        }

        return ms.ToArray();
    }

    /// <summary>
    /// Adds the table <see cref="CreateTableAsync(DatabaseFormat, string, int, bool, IReadOnlyList{ColumnDefinition}, Func{int, object[]}, CancellationToken)"/>
    /// creates to a copy of an existing database, such as an Access-authored one
    /// that has the system tables a writer-created file lacks.
    /// </summary>
    /// <param name="image">The database image to copy; it is not changed.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="minimumRows">The number of rows to insert at least.</param>
    /// <param name="primaryKey">Whether <c>Id</c> is the table's primary key.</param>
    /// <param name="extraColumns">The columns that follow <c>Id</c> and <c>Name</c>.</param>
    /// <param name="extraValues">Returns the values of <paramref name="extraColumns"/> for an <c>Id</c>; <see langword="null"/> leaves them null.</param>
    /// <param name="cancellationToken">A token used to cancel the writes.</param>
    /// <returns>The new database image.</returns>
    public static async ValueTask<byte[]> AddTableAsync(
        byte[] image,
        string tableName,
        int minimumRows,
        bool primaryKey,
        IReadOnlyList<ColumnDefinition> extraColumns,
        Func<int, object[]>? extraValues,
        CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream();
        await ms.WriteAsync(image, cancellationToken);
        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            ms,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken))
        {
            await FillTableAsync(writer, ms, tableName, minimumRows, primaryKey, extraColumns, extraValues, cancellationToken);
        }

        return ms.ToArray();
    }

    /// <summary>
    /// Moves the first live row of <paramref name="tableName"/>'s first data page
    /// (its last data page for <see cref="OverflowRowLayout.SamePage"/>) according
    /// to <paramref name="layout"/>.
    /// </summary>
    /// <param name="image">The unencrypted database image, changed in place.</param>
    /// <param name="tableName">The user table whose row moves.</param>
    /// <param name="layout">How the moved row is laid out.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>Where the header and the row data are.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="layout"/> is not a defined layout.</exception>
    public static async ValueTask<SyntheticOverflowRow> MoveRowAsync(byte[] image, string tableName, OverflowRowLayout layout, CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream(image, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        DatabaseFile db = harness.Database;
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, cancellationToken);
        Assert.NotNull(entry);

        IReadOnlyList<long> pages = await db.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken);
        Assert.True(pages.Count >= 2, $"'{tableName}' needs at least two data pages; it has {pages.Count}.");

        long sourcePage = layout == OverflowRowLayout.SamePage ? pages[^1] : pages[0];
        RowBound source = DataPageRows.EnumerateLiveRowBounds(db.Format, PageCopy(image, db, sourcePage)).First();
        byte[] rowBytes = image.AsSpan(PageOffset(db, sourcePage) + source.RowStart, source.RowSize).ToArray();
        long spill = layout == OverflowRowLayout.SamePage
            ? sourcePage
            : FindPageWithRoom(image, db, pages.Skip(1).Reverse(), rowBytes.Length + 8);

        switch (layout)
        {
            case OverflowRowLayout.CrossPage:
            case OverflowRowLayout.SamePage:
            {
                int dataRow = AppendSlot(image, db, spill, rowBytes, DeletedFlag, out int dataStart);
                WriteHeader(image, db, sourcePage, source, dataRow, spill);
                return new SyntheticOverflowRow(sourcePage, source.RowIndex, spill, dataRow, dataStart, rowBytes.Length);
            }

            case OverflowRowLayout.TwoHop:
            {
                int dataRow = AppendSlot(image, db, spill, rowBytes, DeletedFlag, out int dataStart);
                int middleRow = AppendSlot(image, db, spill, Pointer(dataRow, spill), DeletedFlag | OverflowFlag, out _);
                WriteHeader(image, db, sourcePage, source, middleRow, spill);
                return new SyntheticOverflowRow(sourcePage, source.RowIndex, spill, dataRow, dataStart, rowBytes.Length);
            }

            case OverflowRowLayout.PointerPastEndOfFile:
                WriteHeader(image, db, sourcePage, source, 0, db.Pages.PageCount + 10);
                break;

            case OverflowRowLayout.PointerToOtherTable:
                IReadOnlyList<long> catalogPages = await db.OwnedPages.GetOwnedDataPagesAsync(2, cancellationToken);
                WriteHeader(image, db, sourcePage, source, 0, catalogPages[0]);
                break;

            case OverflowRowLayout.ShortPointer:
            {
                // A deleted slot two bytes into the header's row ends the header
                // there, so its bytes are too short to hold a 4-byte pointer.
                int dataRow = AppendSlot(image, db, spill, rowBytes, DeletedFlag, out _);
                WriteHeader(image, db, sourcePage, source, dataRow, spill);
                AppendSlotEntry(image, db, sourcePage, (source.RowStart + 2) | DeletedFlag);
                break;
            }

            case OverflowRowLayout.Cycle:
            {
                int middleRow = AppendSlot(image, db, spill, Pointer(source.RowIndex, sourcePage), DeletedFlag | OverflowFlag, out _);
                WriteHeader(image, db, sourcePage, source, middleRow, spill);
                break;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(layout), layout, null);
        }

        return new SyntheticOverflowRow(sourcePage, source.RowIndex, 0, 0, 0, 0);
    }

    /// <summary>Returns the raw 16-bit row-offset slot of <paramref name="rowIndex"/> on <paramref name="pageNumber"/>.</summary>
    /// <param name="image">The database image.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="rowsStart">The data-page offset of the row-offset table.</param>
    /// <param name="pageNumber">The data page.</param>
    /// <param name="rowIndex">The slot's row index.</param>
    public static int ReadSlot(byte[] image, int pageSize, int rowsStart, long pageNumber, int rowIndex)
        => BinaryPrimitives.ReadUInt16LittleEndian(image.AsSpan(checked((int)(pageNumber * pageSize)) + rowsStart + (rowIndex * 2)));

    private static async ValueTask FillTableAsync(
        AccessWriter writer,
        MemoryStream ms,
        string tableName,
        int minimumRows,
        bool primaryKey,
        IReadOnlyList<ColumnDefinition> extraColumns,
        Func<int, object[]>? extraValues,
        CancellationToken cancellationToken)
    {
        await writer.CreateTableAsync(
            tableName,
            [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Name", typeof(string), maxLength: 100), .. extraColumns],
            primaryKey ? [new IndexDefinition("PrimaryKey", "Id") { IsPrimaryKey = true }] : [],
            cancellationToken);
        await writer.InsertRowsAsync(tableName, Enumerable.Range(1, minimumRows).Select(Row), cancellationToken);

        int nextId = minimumRows + 1;
        while (!await LastPageHasRoomAsync(ms.ToArray(), tableName, cancellationToken))
        {
            Assert.True(nextId < minimumRows + 500, "The last data page never gained room for two rows.");
            await writer.InsertRowAsync(tableName, Row(nextId++), cancellationToken);
        }

        object[] Row(int id) =>
        [
            id,
            $"Item {id:D4} " + new string('x', 40),
            .. extraValues?.Invoke(id) ?? Enumerable.Repeat<object>(DBNull.Value, extraColumns.Count),
        ];
    }

    private static async ValueTask<bool> LastPageHasRoomAsync(byte[] image, string tableName, CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream(image, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        DatabaseFile db = harness.Database;
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, cancellationToken);
        Assert.NotNull(entry);

        IReadOnlyList<long> pages = await db.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken);
        if (pages.Count < 2)
        {
            return false;
        }

        ReadOnlySpan<byte> last = image.AsSpan(PageOffset(db, pages[^1]), db.Format.PageSize);
        RowBound row = DataPageRows.EnumerateLiveRowBounds(db.Format, last.ToArray()).First();
        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(last[db.Format.DataPage.NumRows..]);
        int free = LowestRowStart(last, db, numRows) - (db.Format.DataPage.RowsStart + (numRows * 2));
        return free >= (2 * (row.RowSize + 2)) + 8;
    }

    private static byte[] Pointer(int rowIndex, long pageNumber) =>
    [
        checked((byte)rowIndex),
        (byte)(pageNumber & 0xFF),
        (byte)((pageNumber >> 8) & 0xFF),
        (byte)((pageNumber >> 16) & 0xFF),
    ];

    private static int PageOffset(DatabaseFile db, long pageNumber) => checked((int)(pageNumber * db.Format.PageSize));

    private static byte[] PageCopy(byte[] image, DatabaseFile db, long pageNumber)
        => image.AsSpan(PageOffset(db, pageNumber), db.Format.PageSize).ToArray();

    /// <summary>
    /// Turns <paramref name="source"/>'s slot into an overflow header: the first four
    /// bytes of its row become the pointer, the rest are zeroed, and the slot gains
    /// the overflow flag.
    /// </summary>
    /// <param name="image">The database image.</param>
    /// <param name="db">The open database, which supplies the page layout.</param>
    /// <param name="sourcePage">The page holding the slot.</param>
    /// <param name="source">The slot's current row bounds.</param>
    /// <param name="targetRow">The row index the pointer names.</param>
    /// <param name="targetPage">The page the pointer names.</param>
    private static void WriteHeader(byte[] image, DatabaseFile db, long sourcePage, RowBound source, int targetRow, long targetPage)
    {
        Span<byte> row = image.AsSpan(PageOffset(db, sourcePage) + source.RowStart, source.RowSize);
        row.Clear();
        Pointer(targetRow, targetPage).CopyTo(row);
        int slotOffset = PageOffset(db, sourcePage) + db.Format.DataPage.RowsStart + (source.RowIndex * 2);
        BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(slotOffset), checked((ushort)(source.RowStart | OverflowFlag)));
    }

    /// <summary>
    /// Appends a slot holding <paramref name="bytes"/> below the lowest row on the page,
    /// as the data-page inserter does, and returns its row index.
    /// </summary>
    /// <param name="image">The database image.</param>
    /// <param name="db">The open database, which supplies the page layout.</param>
    /// <param name="pageNumber">The data page.</param>
    /// <param name="bytes">The new slot's row bytes.</param>
    /// <param name="flags">The flag bits of the new slot.</param>
    /// <param name="rowStart">Receives the new slot's row offset.</param>
    private static int AppendSlot(byte[] image, DatabaseFile db, long pageNumber, byte[] bytes, int flags, out int rowStart)
    {
        int pageOffset = PageOffset(db, pageNumber);
        Span<byte> page = image.AsSpan(pageOffset, db.Format.PageSize);
        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page[db.Format.DataPage.NumRows..]);
        int firstStart = LowestRowStart(page, db, numRows);
        rowStart = firstStart - bytes.Length;
        Assert.True(rowStart >= db.Format.DataPage.RowsStart + ((numRows + 1) * 2), $"Page {pageNumber} has no room for {bytes.Length} more bytes.");

        bytes.CopyTo(page[rowStart..]);
        BinaryPrimitives.WriteUInt16LittleEndian(page[(db.Format.DataPage.RowsStart + (numRows * 2))..], checked((ushort)(rowStart | flags)));
        BinaryPrimitives.WriteUInt16LittleEndian(page[db.Format.DataPage.NumRows..], checked((ushort)(numRows + 1)));
        return numRows;
    }

    /// <summary>Appends a slot entry with no row bytes of its own.</summary>
    /// <param name="image">The database image.</param>
    /// <param name="db">The open database, which supplies the page layout.</param>
    /// <param name="pageNumber">The data page.</param>
    /// <param name="raw">The raw 16-bit slot value.</param>
    private static void AppendSlotEntry(byte[] image, DatabaseFile db, long pageNumber, int raw)
    {
        Span<byte> page = image.AsSpan(PageOffset(db, pageNumber), db.Format.PageSize);
        int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page[db.Format.DataPage.NumRows..]);
        Assert.True(LowestRowStart(page, db, numRows) >= db.Format.DataPage.RowsStart + ((numRows + 1) * 2), $"Page {pageNumber} has no room for another slot.");
        BinaryPrimitives.WriteUInt16LittleEndian(page[(db.Format.DataPage.RowsStart + (numRows * 2))..], checked((ushort)raw));
        BinaryPrimitives.WriteUInt16LittleEndian(page[db.Format.DataPage.NumRows..], checked((ushort)(numRows + 1)));
    }

    private static int LowestRowStart(ReadOnlySpan<byte> page, DatabaseFile db, int numRows)
    {
        int lowest = db.Format.PageSize;
        for (int r = 0; r < numRows; r++)
        {
            int start = BinaryPrimitives.ReadUInt16LittleEndian(page[(db.Format.DataPage.RowsStart + (r * 2))..]) & OffsetMask;
            if (start > 0 && start < lowest)
            {
                lowest = start;
            }
        }

        return lowest;
    }

    private static long FindPageWithRoom(byte[] image, DatabaseFile db, IEnumerable<long> candidates, int bytesNeeded)
    {
        foreach (long pageNumber in candidates)
        {
            ReadOnlySpan<byte> page = image.AsSpan(PageOffset(db, pageNumber), db.Format.PageSize);
            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(page[db.Format.DataPage.NumRows..]);
            int free = LowestRowStart(page, db, numRows) - (db.Format.DataPage.RowsStart + ((numRows + 2) * 2));
            if (free >= bytesNeeded)
            {
                return pageNumber;
            }
        }

        Assert.Fail($"No data page has {bytesNeeded} free bytes for the moved row.");
        return 0;
    }
}
