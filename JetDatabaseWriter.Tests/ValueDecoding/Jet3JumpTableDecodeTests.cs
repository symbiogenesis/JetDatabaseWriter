namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>
/// Reads Jet3 (Access 97) rows whose variable data crosses offset 256. A Jet3
/// row stores the EOD and each variable-column offset in one byte, and a row
/// longer than 256 bytes carries a jump table of <c>(rowLength - 1) / 256</c>
/// entries that add 256 to every offset at or past each entry's index (the
/// mdbtools and Jackcess readers; Access 97 wrote the one jump-table row in
/// the fixtures, MSP_PROJECTS in test2V1997.mdb). The reader used to skip
/// <c>rowLength / 256</c> bytes and never read them, so such rows decoded
/// truncated, empty or shifted, and a row of exactly 256 bytes was misparsed.
/// </summary>
public sealed class Jet3JumpTableDecodeTests
{
    private const int Jet3PageSize = 2048;

    /// <summary>
    /// A hand-built row in Access's layout: <paramref name="fixedLongCount"/>
    /// Long columns, then variable columns of <paramref name="varLengths"/>
    /// bytes (-1 for null), with the jump bytes in stored order.
    /// </summary>
    /// <param name="fixedLongCount">The number of fixed Long columns.</param>
    /// <param name="varLengths">The variable-column lengths, comma-separated; -1 is null.</param>
    /// <param name="jumpHex">The jump-table bytes in stored order, as hex.</param>
    /// <param name="rowLength">The expected row length.</param>
    [Theory]
    [InlineData(1, "200,200", "02", 411)] // only the EOD (405) crosses 256
    [InlineData(1, "100,300", "02", 411)] // the second value straddles 256
    [InlineData(1, "255,100", "01", 366)] // the second value starts at 260
    [InlineData(75, "10", "00", 325)] // a 300-byte fixed area: the first offset is past 256
    [InlineData(1, "255,255,100", "0201", 623)] // the EOD (615) is past 512
    [InlineData(1, "10,600,10", "0202", 633)] // one value spans both boundaries
    [InlineData(1, "255,247", "FF01", 514)] // a real entry and a trailing dummy
    [InlineData(1, "-1,-1,255", "03", 267)] // nulls before a value: only the EOD (260) crosses 256
    public void HandBuiltJet3Row_WithJumpEntries_ResolvesEverySlice(int fixedLongCount, string varLengths, string jumpHex, int rowLength)
    {
        Assert.NotNull(varLengths);
        int[] lengths = [.. varLengths.Split(',').Select(s => int.Parse(s, CultureInfo.InvariantCulture))];
        AssertRowResolves(fixedLongCount, lengths, Convert.FromHexString(jumpHex), rowLength);
    }

    /// <summary>1 + 4 + 247 + EOD + one offset + var_len + mask = 256 bytes: (256 - 1) / 256 = 0 jump entries.</summary>
    [Fact]
    public void HandBuiltJet3Row_Exactly256Bytes_HasNoJumpTable()
        => AssertRowResolves(fixedLongCount: 1, [247], [], rowLength: 256);

    /// <summary>EOD 255 leaves the one entry's boundary unreached; Access writes 0xFF there.</summary>
    [Fact]
    public void HandBuiltJet3Row_TrailingDummyEntry0xFF_IsIgnored()
        => AssertRowResolves(fixedLongCount: 1, [250], [0xFF], rowLength: 260);

    /// <summary>
    /// Earlier builds wrote rows up to EOD 255 and left their one jump byte 0.
    /// The entry's boundary is beyond the EOD, so it is a dummy whatever it holds.
    /// </summary>
    [Fact]
    public void Jet3RowFromEarlierBuild_ZeroJumpByte_DecodesUnchanged()
        => AssertRowResolves(fixedLongCount: 1, [250], [0x00], rowLength: 260);

    /// <summary>
    /// MSP_PROJECTS in test2V1997.mdb holds the one Access 97 row in the
    /// fixtures with a jump table: 290 bytes, EOD 255 and a single 0xFF dummy.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AccessAuthoredJet3Row_WithDummyJump_DecodesUnchanged()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.Test2V1997))
        {
            Assert.Skip("test2V1997.mdb is unavailable on this machine.");
        }

        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(TestDatabases.Test2V1997, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("MSP_PROJECTS", ct));
            TableDef tableDef = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            DatabaseFile db = harness.Database;
            int checkedRows = 0;
            await db.ForEachLiveTableRowAsync(
                entry.TDefPage,
                (row, _) =>
                {
                    RowLocation location = row.Location;
                    ReadOnlySpan<byte> rowBytes = row.Page.AsSpan(location.RowStart, location.RowSize);
                    int nullMaskSize = (rowBytes[0] + 7) / 8;
                    Assert.Equal(290, location.RowSize);
                    Assert.Equal([0xFF], Jet3RowTrailerReference.JumpBytes(rowBytes, nullMaskSize));

                    int[] offsets = Jet3RowTrailerReference.DecodeVarOffsets(rowBytes, nullMaskSize);
                    Assert.Equal(255, offsets[^1]);
                    Assert.True(db.TryParseRowLayout(row.Page, location.RowStart, location.RowSize, hasVarColumns: true, out RowLayout layout));
                    Assert.Equal(255, layout.Eod);
                    foreach (ColumnInfo column in tableDef.Columns.Where(c => !c.IsFixed))
                    {
                        ColumnSlice slice = db.ResolveColumnSlice(row.Page, location.RowStart, location.RowSize, layout, column);
                        if (slice.Kind == ColumnSliceKind.Var)
                        {
                            Assert.Equal(offsets[column.VarIdx], slice.DataStart);
                            Assert.Equal(offsets[column.VarIdx + 1] - offsets[column.VarIdx], slice.DataLen);
                        }
                    }

                    checkedRows++;
                    return new ValueTask<bool>(true);
                },
                ct);
            Assert.Equal(1, checkedRows);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(TestDatabases.Test2V1997, new AccessReaderOptions { UseLockFile = false }, ct);
        using DataTable table = await reader.ReadDataTableAsync("MSP_PROJECTS", cancellationToken: ct);
        DataRow project = Assert.Single(table.AsEnumerable());
        Assert.Equal("Project1", project["PROJ_NAME"]);
        Assert.Equal("Standard", project["PROJ_INFO_CAL_NAME"]);
        Assert.Equal(@"Z:\temp\Project1.mpd", project["PROJ_DATA_SOURCE"]);
    }

    /// <summary>
    /// The public read APIs on a row with real jump entries: the writer stores
    /// a short row, the test replaces it on its data page with a hand-built
    /// 623-byte row whose second value starts at 260 and whose EOD is at 615,
    /// and every read path must return the hand-built values.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AccessReader_ReadsHandPlacedJet3RowWithJumpTable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string a = Pattern('a', 255);
        string b = Pattern('k', 100);
        string c = Pattern('p', 255);

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("A", typeof(string), maxLength: 255),
                    new ColumnDefinition("B", typeof(string), maxLength: 255),
                    new ColumnDefinition("C", typeof(string), maxLength: 255),
                ],
                ct);
            await writer.InsertRowAsync("T", [1, "a", "b", "c"], ct);
        }

        RowLocation location;
        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("T", ct));
            location = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, ct));
        }

        byte[] row = Jet3RowTrailerReference.BuildRow(
            BitConverter.GetBytes(7),
            fixedColumnCount: 1,
            [Encoding.ASCII.GetBytes(a), Encoding.ASCII.GetBytes(b), Encoding.ASCII.GetBytes(c)],
            [0x03, 0x01]);
        Assert.Equal(623, row.Length);
        Assert.Equal([5, 260, 360, 615], Jet3RowTrailerReference.DecodeVarOffsets(row, nullMaskSize: 1));
        PlaceOnlyRow(ms, location, row);

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        using DataTable table = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        DataRow typed = Assert.Single(table.AsEnumerable());
        Assert.Equal(7, typed["Id"]);
        Assert.Equal(a, typed["A"]);
        Assert.Equal(b, typed["B"]);
        Assert.Equal(c, typed["C"]);

        object[] streamed = Assert.Single(await reader.Rows("T", cancellationToken: ct).ToListAsync(ct));
        Assert.Equal<object>([7, a, b, c], streamed);

        string[] strings = Assert.Single(await reader.RowsAsStrings("T", cancellationToken: ct).ToListAsync(ct));
        Assert.Equal(["7", a, b, c], strings);

        LongRow mapped = Assert.Single(await reader.Rows<LongRow>("T", cancellationToken: ct).ToListAsync(ct));
        Assert.Equal(7, mapped.Id);
        Assert.Equal(a, mapped.A);
        Assert.Equal(b, mapped.B);
        Assert.Equal(c, mapped.C);
    }

    private static void AssertRowResolves(int fixedLongCount, int[] varLengths, byte[] jumpBytes, int rowLength)
    {
        byte[] fixedArea = new byte[fixedLongCount * 4];
        var columns = new List<ColumnInfo>();
        for (int i = 0; i < fixedLongCount; i++)
        {
            BitConverter.GetBytes(i + 1).CopyTo(fixedArea, i * 4);
            columns.Add(new ColumnInfo
            {
                Type = ColumnType.LongIntegerType,
                ColNum = i,
                FixedOff = i * 4,
                Size = 4,
                Flags = Constants.ColumnDescriptorFlags.Fixed,
                Name = "F" + i.ToString(CultureInfo.InvariantCulture),
            });
        }

        var values = new List<byte[]?>();
        int[] expectedOffsets = new int[varLengths.Length + 1];
        int offset = 1 + fixedArea.Length;
        for (int i = 0; i < varLengths.Length; i++)
        {
            expectedOffsets[i] = offset;
            byte[]? value = varLengths[i] < 0 ? null : Enumerable.Range(0, varLengths[i]).Select(k => unchecked((byte)(k + (i * 31)))).ToArray();
            values.Add(value);
            offset += value?.Length ?? 0;
            columns.Add(new ColumnInfo
            {
                Type = ColumnType.BinaryType,
                ColNum = fixedLongCount + i,
                VarIdx = i,
                Name = "V" + i.ToString(CultureInfo.InvariantCulture),
            });
        }

        expectedOffsets[^1] = offset;
        byte[] row = Jet3RowTrailerReference.BuildRow(fixedArea, fixedLongCount, values, jumpBytes);
        Assert.Equal(rowLength, row.Length);
        int nullMaskSize = (columns.Count + 7) / 8;
        Assert.Equal(expectedOffsets, Jet3RowTrailerReference.DecodeVarOffsets(row, nullMaskSize));

        // Place the row at the end of a page, as the writer does.
        byte[] page = new byte[Jet3PageSize];
        int rowStart = Jet3PageSize - row.Length;
        row.CopyTo(page, rowStart);

        var rowFields = RowFieldSizes.For(DatabaseFormat.Jet3Mdb);
        Assert.True(RowDecodePlan.TryParseRowLayout(DatabaseFormat.Jet3Mdb, rowFields, page, rowStart, row.Length, hasVarColumns: true, out RowLayout layout));
        Assert.Equal(varLengths.Length, layout.VarLen);
        Assert.Equal(expectedOffsets[^1], layout.Eod);

        for (int i = 0; i < fixedLongCount; i++)
        {
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(rowFields, page, rowStart, row.Length, layout, columns[i]);
            Assert.Equal(ColumnSliceKind.Fixed, slice.Kind);
            Assert.Equal(i + 1, BitConverter.ToInt32(page, rowStart + slice.DataStart));
        }

        for (int i = 0; i < varLengths.Length; i++)
        {
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(rowFields, page, rowStart, row.Length, layout, columns[fixedLongCount + i]);
            if (values[i] is not byte[] value)
            {
                Assert.Equal(ColumnSliceKind.Null, slice.Kind);
                continue;
            }

            Assert.Equal(ColumnSliceKind.Var, slice.Kind);
            Assert.Equal(expectedOffsets[i], slice.DataStart);
            Assert.Equal(value.Length, slice.DataLen);
            Assert.Equal(value, page.AsSpan(rowStart + slice.DataStart, slice.DataLen).ToArray());
        }
    }

    /// <summary>
    /// Replaces the only row on its Jet3 data page with <paramref name="row"/>,
    /// packed at the end of the page, and fixes the row-offset slot and the
    /// free-space field.
    /// </summary>
    /// <param name="ms">The database bytes.</param>
    /// <param name="location">The row's location.</param>
    /// <param name="row">The new row bytes.</param>
    private static void PlaceOnlyRow(MemoryStream ms, RowLocation location, byte[] row)
    {
        byte[] file = ms.GetBuffer();
        int pageStart = checked((int)location.PageNumber * Jet3PageSize);
        Assert.Equal(0x01, file[pageStart]);
        Assert.Equal(1, BitConverter.ToUInt16(file, pageStart + 8));
        Assert.Equal(0, location.RowIndex);

        int rowStart = Jet3PageSize - row.Length;
        Array.Clear(file, pageStart + 12, Jet3PageSize - 12);
        row.CopyTo(file, pageStart + rowStart);
        BitConverter.GetBytes(checked((ushort)rowStart)).CopyTo(file, pageStart + 10);
        BitConverter.GetBytes(checked((ushort)(rowStart - 12))).CopyTo(file, pageStart + 2);
    }

    private static string Pattern(char first, int length)
    {
        var text = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            text.Append((char)(first + (i % 10)));
        }

        return text.ToString();
    }

    private sealed class LongRow
    {
        public int Id { get; set; }

        public string? A { get; set; }

        public string? B { get; set; }

        public string? C { get; set; }
    }
}
