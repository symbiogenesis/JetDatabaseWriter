namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the per-format TDEF header offsets the writer stamps and the reader
/// trusts. The expected offsets are spelled out here (mdbtools HACKING.md /
/// Jackcess <c>JetFormat</c>) rather than read from the library's layout
/// struct, so a layout that applies the Jet4 offsets to Jet3 fails:
/// <list type="bullet">
///   <item>Jet3: <c>num_rows</c> 12, <c>autonumber</c> 16, <c>table_type</c> 20,
///   <c>max_cols</c> 21, <c>num_var_cols</c> 23, <c>num_cols</c> 25.</item>
///   <item>Jet4 / ACE: <c>num_rows</c> 16, <c>autonumber</c> 20, <c>table_type</c> 40,
///   <c>max_cols</c> 41, <c>num_var_cols</c> 43, <c>num_cols</c> 45.</item>
/// </list>
/// </summary>
public sealed class TdefHeaderOffsetTests
{
    private const byte UserTableType = 0x4E;

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    /// <summary>
    /// A fixed-only AutoNumber table takes enough inserts for the counter to
    /// pass the uint32 formed by the Jet3 <c>table_type</c>, <c>max_cols</c>
    /// and <c>num_var_cols</c> bytes (590 for this shape). The row count and
    /// the counter must land in their own slots and leave the column-count
    /// fields alone, and the reader must report the row count it wrote.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="numRows">The expected <c>num_rows</c> offset.</param>
    /// <param name="autoNumber">The expected <c>autonumber</c> offset.</param>
    /// <param name="tableType">The expected <c>table_type</c> offset.</param>
    /// <param name="maxCols">The expected <c>max_cols</c> offset.</param>
    /// <param name="numVarCols">The expected <c>num_var_cols</c> offset.</param>
    /// <param name="numCols">The expected <c>num_cols</c> offset.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 12, 16, 20, 21, 23, 25)]
    [InlineData(DatabaseFormat.Jet4Mdb, 16, 20, 40, 41, 43, 45)]
    [InlineData(DatabaseFormat.AceAccdb, 16, 20, 40, 41, 43, 45)]
    public async Task AutoNumberInserts_WriteRowCountAndCounterToFormatOffsets_AndKeepTableType(
        DatabaseFormat format,
        int numRows,
        int autoNumber,
        int tableType,
        int maxCols,
        int numVarCols,
        int numCols)
    {
        const int rowCount = 700;
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, this.ct))
        {
            await writer.CreateTableAsync(
                "Fixed",
                [
                    new ColumnDefinition("ZqAutoFx", typeof(int)) { IsAutoIncrement = true },
                    new ColumnDefinition("ZqVal", typeof(int)),
                ],
                this.ct);
        }

        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, this.ct))
        {
            var rows = new List<object?[]>(rowCount);
            for (int i = 0; i < rowCount; i++)
            {
                rows.Add([DBNull.Value, i]);
            }

            _ = await writer.InsertRowsAsync("Fixed", rows, this.ct);
        }

        byte[] bytes = ms.ToArray();
        int o = FindTdefOffset(bytes, format, "ZqAutoFx");

        Assert.Equal((uint)rowCount, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(o + numRows)));
        Assert.Equal((uint)rowCount, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(o + autoNumber)));
        Assert.Equal(UserTableType, bytes[o + tableType]);
        Assert.Equal(2, BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(o + maxCols)));
        Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(o + numVarCols)));
        Assert.Equal(2, BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(o + numCols)));

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, this.ct);
        TableStat stat = (await reader.GetTableStatsAsync(this.ct)).Single(s => s.Name == "Fixed");
        Assert.Equal(rowCount, stat.RowCount);
        Assert.Equal(2, stat.ColumnCount);

        var ids = new List<int>(rowCount);
        await foreach (object[] row in reader.Rows("Fixed", cancellationToken: this.ct))
        {
            ids.Add((int)row[0]);
        }

        Assert.Equal(Enumerable.Range(1, rowCount), ids.Order());
    }

    /// <summary>
    /// Deletes move the declared row count at the format's <c>num_rows</c>
    /// offset, and <see cref="TableStat.RowCount"/> agrees with the real count.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="numRows">The expected <c>num_rows</c> offset.</param>
    /// <param name="autoNumber">The expected <c>autonumber</c> offset.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 12, 16)]
    [InlineData(DatabaseFormat.Jet4Mdb, 16, 20)]
    [InlineData(DatabaseFormat.AceAccdb, 16, 20)]
    public async Task Delete_DecrementsRowCountAtFormatOffset_AndLeavesCounter(DatabaseFormat format, int numRows, int autoNumber)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, this.ct))
        {
            await writer.CreateTableAsync(
                "People",
                [
                    new ColumnDefinition("ZqPersonId", typeof(int)) { IsAutoIncrement = true },
                    new ColumnDefinition("Name", typeof(string), 20),
                ],
                this.ct);

            _ = await writer.InsertRowsAsync(
                "People",
                [[DBNull.Value, "a"], [DBNull.Value, "b"], [DBNull.Value, "c"], [DBNull.Value, "d"]],
                this.ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("People", "Name", "d", this.ct));
        }

        byte[] bytes = ms.ToArray();
        int o = FindTdefOffset(bytes, format, "ZqPersonId");
        Assert.Equal(3u, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(o + numRows)));
        Assert.Equal(4u, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(o + autoNumber)));

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, this.ct);
        TableStat stat = (await reader.GetTableStatsAsync(this.ct)).Single(s => s.Name == "People");
        Assert.Equal(3, stat.RowCount);
        Assert.Equal(3, await reader.GetRealRowCountAsync("People", this.ct));
    }

    /// <summary>
    /// Access stores a Jet3 table's row count at TDEF offset 12; offset 16 is
    /// the AutoNumber counter, which Access leaves at 0 for <c>MSysObjects</c>.
    /// The parsed row count of the Access-authored Jet3 fixture's catalog and
    /// user tables must match their live rows.
    /// </summary>
    [Fact]
    public async Task Jet3AccessAuthoredFixture_DeclaredRowCount_MatchesLiveRows()
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(TestDatabases.Jet3Test, cancellationToken: this.ct);
        Assert.Equal(DatabaseFormat.Jet3Mdb, harness.Database.Format.Kind);

        var tdefPages = new List<long> { 2 };
        await using (AccessReader reader = await AccessReader.OpenAsync(
            TestDatabases.Jet3Test, new AccessReaderOptions { UseLockFile = false }, this.ct))
        {
            foreach (string table in await reader.ListTablesAsync(this.ct))
            {
                CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(table, this.ct));
                tdefPages.Add(entry.TDefPage);
            }
        }

        Assert.True(tdefPages.Count > 1);
        foreach (long tdefPage in tdefPages)
        {
            TableDef tableDef = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(tdefPage, this.ct));
            List<RowLocation> live = await harness.Database.GetLiveRowLocationsAsync(tdefPage, this.ct);
            Assert.True(live.Count > 0, $"TDEF page {tdefPage} has no live rows.");
            Assert.Equal(live.Count, tableDef.RowCount);
        }
    }

    private static int FindTdefOffset(byte[] bytes, DatabaseFormat format, string columnName)
    {
        int pageSize = format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;
        byte[] needle = format == DatabaseFormat.Jet3Mdb
            ? Encoding.ASCII.GetBytes(columnName)
            : Encoding.Unicode.GetBytes(columnName);
        for (int page = 3; page < bytes.Length / pageSize; page++)
        {
            int o = page * pageSize;
            if (bytes[o] == Constants.PageTypes.TableDefinition && bytes.AsSpan(o, pageSize).IndexOf(needle) >= 0)
            {
                return o;
            }
        }

        throw new InvalidOperationException($"No TDEF page names column '{columnName}'.");
    }
}
