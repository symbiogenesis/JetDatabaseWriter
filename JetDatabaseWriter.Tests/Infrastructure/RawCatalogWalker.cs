namespace JetDatabaseWriter.Tests.Infrastructure;

using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Lists the user tables of a database straight from the <c>MSysObjects</c> data
/// pages, independently of the library's row directory, page-ownership discovery
/// and catalog scan. It applies the Jackcess rules: every data page owned by TDEF
/// page 2 is visited, a row ends at the next greater offset of any slot on its page,
/// a slot flagged deleted (<c>0x8000</c>) is skipped, and a slot flagged overflow
/// (<c>0x4000</c>) is followed through its pointer (target row byte, then the
/// target page as a little-endian 24-bit number). Only the column values are
/// decoded through the library.
/// </summary>
internal static class RawCatalogWalker
{
    private const int DeletedFlag = 0x8000;
    private const int OverflowFlag = 0x4000;
    private const int OffsetMask = 0x1FFF;
    private const uint SystemObjectFlags = 0x80000002U;

    /// <summary>
    /// Returns the names of the user tables: <c>MSysObjects</c> rows of type 1 whose
    /// flags carry neither system bit, with a name and a TDEF page.
    /// </summary>
    /// <param name="harness">The open database.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    public static async ValueTask<SortedSet<string>> GetUserTableNamesAsync(ReaderHarness harness, CancellationToken cancellationToken)
    {
        DatabaseFile db = harness.Database;
        TableDef? msys = await harness.ReadTableDefAsync(2, cancellationToken);
        Assert.NotNull(msys);
        ColumnInfo? name = msys.FindColumn("Name");
        ColumnInfo? type = msys.FindColumn("Type");
        ColumnInfo? flags = msys.FindColumn("Flags");
        ColumnInfo? id = msys.FindColumn("Id");
        Assert.NotNull(name);
        Assert.NotNull(type);
        Assert.NotNull(flags);
        Assert.NotNull(id);

        var names = new SortedSet<string>(StringComparer.Ordinal);
        for (long pageNumber = 3; pageNumber < db.Pages.PageCount; pageNumber++)
        {
            byte[] page = await harness.ReadPageCopyAsync(pageNumber, cancellationToken);
            if (page[0] != 0x01 || BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(db.Format.DataPage.TDefOff)) != 2)
            {
                continue;
            }

            int numRows = SlotCount(db, page);
            for (int r = 0; r < numRows; r++)
            {
                int raw = Slot(db, page, r);
                if ((raw & DeletedFlag) != 0)
                {
                    continue;
                }

                (byte[] RowPage, int Start, int Size)? row = (raw & OverflowFlag) == 0
                    ? (page, raw & OffsetMask, RowSize(db, page, raw & OffsetMask))
                    : await FollowAsync(harness, page, raw & OffsetMask, cancellationToken);
                if (row is not { } found || found.Size <= 0)
                {
                    continue;
                }

                string rowName = ScalarColumnReader.DecodeSimpleColumnValue(db.Format, found.RowPage, found.Start, found.Size, name);
                int rowType = int.TryParse(ScalarColumnReader.DecodeSimpleColumnValue(db.Format, found.RowPage, found.Start, found.Size, type), NumberStyles.Integer, CultureInfo.InvariantCulture, out int t) ? t : 0;
                long rowFlags = long.TryParse(ScalarColumnReader.DecodeSimpleColumnValue(db.Format, found.RowPage, found.Start, found.Size, flags), NumberStyles.Integer, CultureInfo.InvariantCulture, out long f) ? f : 0;
                long rowId = long.TryParse(ScalarColumnReader.DecodeSimpleColumnValue(db.Format, found.RowPage, found.Start, found.Size, id), NumberStyles.Integer, CultureInfo.InvariantCulture, out long i) ? i : 0;
                if (rowType == 1 && (unchecked((uint)rowFlags) & SystemObjectFlags) == 0 && rowName.Length > 0 && (rowId & 0xFFFFFF) > 0)
                {
                    _ = names.Add(rowName);
                }
            }
        }

        return names;
    }

    private static int SlotCount(DatabaseFile db, byte[] page)
        => Math.Min(BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(db.Format.DataPage.NumRows)), (db.Format.PageSize - db.Format.DataPage.RowsStart) / 2);

    private static int Slot(DatabaseFile db, byte[] page, int rowIndex)
        => BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(db.Format.DataPage.RowsStart + (rowIndex * 2)));

    /// <summary>Returns the bytes from <paramref name="start"/> to the next greater offset of any slot.</summary>
    /// <param name="db">The open database, which supplies the page layout.</param>
    /// <param name="page">The data page.</param>
    /// <param name="start">The row's start offset.</param>
    private static int RowSize(DatabaseFile db, byte[] page, int start)
    {
        int end = db.Format.PageSize;
        int numRows = SlotCount(db, page);
        for (int r = 0; r < numRows; r++)
        {
            int offset = Slot(db, page, r) & OffsetMask;
            if (offset > start && offset < end)
            {
                end = offset;
            }
        }

        return end - start;
    }

    private static async ValueTask<(byte[] RowPage, int Start, int Size)?> FollowAsync(ReaderHarness harness, byte[] page, int pointerStart, CancellationToken cancellationToken)
    {
        DatabaseFile db = harness.Database;
        for (int hop = 0; hop < 8; hop++)
        {
            if (RowSize(db, page, pointerStart) < 4)
            {
                return null;
            }

            int targetRow = page[pointerStart];
            long targetPage = page[pointerStart + 1] | (page[pointerStart + 2] << 8) | (page[pointerStart + 3] << 16);
            if (targetPage <= 0 || targetPage >= db.Pages.PageCount)
            {
                return null;
            }

            page = await harness.ReadPageCopyAsync(targetPage, cancellationToken);
            if (page[0] != 0x01 || targetRow >= SlotCount(db, page))
            {
                return null;
            }

            int raw = Slot(db, page, targetRow);
            int start = raw & OffsetMask;
            if ((raw & OverflowFlag) == 0)
            {
                return (page, start, RowSize(db, page, start));
            }

            pointerStart = start;
        }

        return null;
    }
}
