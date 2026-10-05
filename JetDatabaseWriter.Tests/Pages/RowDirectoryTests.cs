namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins how a data page's row-offset table becomes row bounds. A row runs from
/// its start offset up to the next greater offset of any slot on the page, or
/// to the end of the page. Access leaves deleted slots pointing at the same
/// offset as a live row (indexTestV1997.mdb page 18 has row 21 live at 756 and
/// rows 22-26 deleted at 756), so the "next" offset has to be the next
/// distinct one, not the neighbour of whichever equal entry a binary search
/// lands on.
/// </summary>
public sealed class RowDirectoryTests
{
    private const int Deleted = 0x8000;
    private const int Overflow = 0x4000;

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    public static TheoryData<DatabaseFormat> Formats =>
    [
        DatabaseFormat.Jet3Mdb,
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.AceAccdb,
    ];

    /// <summary>
    /// A live row followed in the slot table by deleted slots that share its
    /// offset keeps the bytes up to the next greater offset.
    /// </summary>
    /// <param name="format">The database format, which sets the page size and header layout.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task LiveRowFollowedByDeletedSlotsAtSameOffset_GetsFullLength(DatabaseFormat format)
    {
        await using ReaderHarness harness = await OpenEmptyAsync(format, this.ct);
        DatabaseFile db = harness.Database;
        int top = db.Format.PageSize;
        int shared = top - 300;
        byte[] page = BuildDataPage(db, top - 100, shared, shared | Deleted, shared | Deleted, shared | Deleted);

        AssertBounds(db, page, new RowBound(0, top - 100, 100), new RowBound(1, shared, 200));
    }

    /// <summary>
    /// Deleted slots listed before a live slot at the same offset do not
    /// shorten the live row either.
    /// </summary>
    /// <param name="format">The database format, which sets the page size and header layout.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task DeletedSlotsBeforeLiveSlotAtSameOffset_GetsFullLength(DatabaseFormat format)
    {
        await using ReaderHarness harness = await OpenEmptyAsync(format, this.ct);
        DatabaseFile db = harness.Database;
        int top = db.Format.PageSize;
        int shared = top - 300;
        byte[] page = BuildDataPage(db, shared | Deleted, shared | Deleted, top - 100, shared | Deleted, shared);

        AssertBounds(db, page, new RowBound(2, top - 100, 100), new RowBound(4, shared, 200));
    }

    /// <summary>
    /// Offsets need not descend in slot order: each row still ends at the next
    /// greater offset, and the highest row ends at the end of the page.
    /// </summary>
    /// <param name="format">The database format, which sets the page size and header layout.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task OffsetsNotDescending_EndAtNextGreaterOffset(DatabaseFormat format)
    {
        await using ReaderHarness harness = await OpenEmptyAsync(format, this.ct);
        DatabaseFile db = harness.Database;
        int top = db.Format.PageSize;
        byte[] page = BuildDataPage(db, top - 600, top - 100, top - 300, (top - 450) | Deleted);

        AssertBounds(
            db,
            page,
            new RowBound(0, top - 600, 150),
            new RowBound(1, top - 100, 100),
            new RowBound(2, top - 300, 200));
    }

    /// <summary>
    /// The row directory keeps an overflow row's header (flagged <c>0x4000</c>) as a
    /// pointer entry and leaves out deleted slots, including a deleted header
    /// (<c>0xC000</c>) and the moved bytes Access flags deleted. The live-row
    /// enumeration leaves the header out too.
    /// </summary>
    /// <param name="format">The database format, which sets the page size and header layout.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task RowDirectory_FlagsOverflowHeadersAndLeavesOutDeletedSlots(DatabaseFormat format)
    {
        await using ReaderHarness harness = await OpenEmptyAsync(format, this.ct);
        DatabaseFile db = harness.Database;
        int top = db.Format.PageSize;
        byte[] page = BuildDataPage(db, top - 100, (top - 200) | Overflow, (top - 300) | Deleted, (top - 400) | Deleted | Overflow);

        Assert.Equal([new RowBound(0, top - 100, 100), new RowBound(1, top - 200, 100, IsOverflowPointer: true)], DataPageRows.ComputeRowDirectory(db.Format, page));
        Assert.Equal([new RowBound(0, top - 100, 100)], DataPageRows.EnumerateLiveRowBounds(db.Format, page).ToArray());
    }

    private static void AssertBounds(DatabaseFile db, byte[] page, params RowBound[] expected)
    {
        Assert.Equal(expected, DataPageRows.EnumerateLiveRowBounds(db.Format, page).ToArray());
        Assert.Equal(expected, DataPageRows.ComputeRowDirectory(db.Format, page));
    }

    /// <summary>
    /// Builds a data page owned by TDEF page 2 whose row-offset table holds
    /// <paramref name="slots"/> as raw 16-bit entries (offset plus flag bits).
    /// </summary>
    /// <param name="db">The open database, which supplies the page size and header layout.</param>
    /// <param name="slots">The raw row-offset entries, in slot order.</param>
    private static byte[] BuildDataPage(DatabaseFile db, params int[] slots)
    {
        byte[] page = new byte[db.Format.PageSize];
        page[0] = 0x01;
        page[1] = 0x01;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(db.Format.DataPage.TDefOff), 2);
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(db.Format.DataPage.NumRows), checked((ushort)slots.Length));
        for (int i = 0; i < slots.Length; i++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(db.Format.DataPage.RowsStart + (i * 2)), checked((ushort)slots[i]));
        }

        return page;
    }

    private static async ValueTask<ReaderHarness> OpenEmptyAsync(DatabaseFormat format, CancellationToken cancellationToken)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], cancellationToken);
        }

        ms.Position = 0;
        return await ReaderHarness.OpenAsync(ms, leaveOpen: false, cancellationToken: cancellationToken);
    }
}
