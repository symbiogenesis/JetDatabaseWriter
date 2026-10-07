namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using JetDatabaseWriter.ValueEncoding.Models;
using Xunit;

/// <summary>
/// A row must fit on one data page: the page size less the data-page header
/// and one row-offset slot, 2,036 bytes on Jet3 and 4,080 on Jet4/ACE. Long
/// values over the 64-byte inline limit move to LVAL
/// pages first, and while the row is still too long the largest MEMO and
/// byte-array OLE values left in it follow (Access also moves long values out
/// of a full row), so only the other columns can push a row past a page.
/// Calculated columns with a Memo result take part. The writer used to throw
/// for a row of a few MEMO values under the cap, and before that it appended
/// an empty data page for a row too long and then threw
/// <see cref="ArgumentOutOfRangeException"/> from the page copy (or, on Jet3
/// before the jump table was written, <see cref="OverflowException"/>).
/// </summary>
public sealed class RowSizeLimitTests
{
    private const string TableName = "T";

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes()
        => new()
        {
            { DatabaseFormat.Jet3Mdb, WriteMode.Direct },
            { DatabaseFormat.Jet3Mdb, WriteMode.AutoCommit },
            { DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit },
            { DatabaseFormat.Jet4Mdb, WriteMode.Direct },
            { DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit },
            { DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit },
            { DatabaseFormat.AceAccdb, WriteMode.Direct },
            { DatabaseFormat.AceAccdb, WriteMode.AutoCommit },
            { DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit },
        };

    /// <summary>Gets every write mode.</summary>
    /// <returns>The write modes.</returns>
    public static TheoryData<WriteMode> Modes() => [.. new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit }];

    /// <summary>
    /// Multiple MEMO values exceed the native inline limit and their combined
    /// payload exceeds a data page. External storage keeps the row within
    /// capacity and every value survives reopening.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the insert.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertRowWithLargeMemoValues_StoresThemOnLvalPages(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int memoColumns = format == DatabaseFormat.Jet3Mdb ? 3 : 5;
        await using MemoryStream ms = await CreateDatabaseAsync(format, binaryColumns: 0, oleColumns: 0, memoColumns, ct);
        object[] row = [1, .. Enumerable.Range(0, memoColumns).Select(i => (object)new string((char)('a' + i), 1_000))];

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteModes.WriterOptions(mode), ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.InsertRowAsync(TableName, row, ct), ct);
        }

        object[][] expected = [row];
        Assert.Equal(expected, await ReadRowsByIdAsync(ms, ct));
    }

    /// <summary>
    /// A row mixing MEMO and OLE payloads over the native inline limit stores
    /// them externally and preserves every byte through reopening.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the insert.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertRowMixingMemoAndOleValuesPastAPage_MovesThemToLvalPages(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int oleColumns = format == DatabaseFormat.Jet3Mdb ? 8 : 16;
        await using MemoryStream ms = await CreateDatabaseAsync(format, binaryColumns: 0, oleColumns, memoColumns: 1, ct);
        object[] row = [1, .. Enumerable.Range(0, oleColumns).Select(i => (object)Bytes(250, i)), new string('m', 300)];

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteModes.WriterOptions(mode), ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.InsertRowAsync(TableName, row, ct), ct);
        }

        object[][] expected = [row];
        Assert.Equal(expected, await ReadRowsByIdAsync(ms, ct));
    }

    /// <summary>
    /// A calculated Memo result over the native inline limit uses LVAL pages
    /// alongside thirteen fixed 250-byte Binary values. The cached result
    /// and fixed fields survive reopening.
    /// </summary>
    /// <param name="mode">How the writer runs the insert.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task InsertRowWithALargeCalculatedMemo_StoresItOnLvalPages(WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int binaryColumns = 13;
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Id", typeof(int)),
                    .. Enumerable.Range(0, binaryColumns).Select(i => new ColumnDefinition($"Bin{i}", typeof(byte[]), maxLength: 250)),
                    new("Calc", typeof(string)) { IsCalculated = true, CalculationExpression = "[Id] & \" memo\"" },
                ],
                ct);
        }

        object[] row = [1, .. Enumerable.Range(0, binaryColumns).Select(i => (object)Bytes(250, i)), new string('c', 490)];
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteModes.WriterOptions(mode), ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.InsertRowAsync(TableName, row, ct), ct);
        }

        object[][] expected = [row];
        Assert.Equal(expected, await ReadRowsByIdAsync(ms, ct));
    }

    /// <summary>
    /// The no-I/O encoding step that inserts, updates and cascades share
    /// (<see cref="TableRowStore.EncodeRow"/>) moves the largest inline long
    /// value first, the first of equal ones in column order, and only as many
    /// as the row needs. Fixed Binary values occupy most of the page; every
    /// MEMO individually fits the native inline limit. Moving the first
    /// largest value (<c>Memo1</c>) makes the combined row fit. A shorter row
    /// comes back as the same array, with nothing moved.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task EncodeRow_MovesTheLargestInlineLongValueFirst_AndOnlyAsManyAsTheRowNeeds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Id", typeof(int)),
                    .. Enumerable.Range(0, 15).Select(i => new ColumnDefinition($"Bin{i}", typeof(byte[]), maxLength: 250)),
                    .. Enumerable.Range(0, 5).Select(i => new ColumnDefinition($"Memo{i}", typeof(string))),
                ],
                ct);
        }

        ms.Position = 0;
        var options = new AccessWriterOptions { UseLockFile = false };
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, options, cancellationToken: ct);
        DatabaseFile db = harness.Database;
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync(TableName, ct));
        TableDef tableDef = await db.TableDefs.ReadRequiredTableDefAsync(entry.TDefPage, TableName, ct);
        var store = new TableRowStore(
            db.Format,
            db.OwnedPages,
            harness.Pager,
            options,
            new LongValueEncoder(db.Format, harness.Pager, harness.Services.PageAllocator, options),
            new RowEncoder(db.Format),
            new DataPageInserter(db.Format, db.OwnedPages, harness.Pager, harness.Services.PageAllocator, harness.Services.OwnedMaps, new UsageMapEditor(db.Format, harness.Pager, harness.Services.PageAllocator)),
            new TDefPageBuilder(db.Format, harness.Pager));

        int[] lengths = [58, 62, 62, 56, 62];
        object[] values = Row(tableDef, lengths);
        object[] original = (object[])values.Clone();
        (object[] prepared, byte[] rowBytes) = store.EncodeRow(tableDef, values);

        Assert.Equal(original, values);
        Assert.True(rowBytes.Length <= 4_080, $"The row is {rowBytes.Length} bytes.");
        int moved = tableDef.FindColumnIndex(column => column.Name == "Memo1");
        for (int i = 0; i < prepared.Length; i++)
        {
            if (i == moved)
            {
                PreEncodedLongValue pending = Assert.IsType<PreEncodedLongValue>(prepared[i]);
                Assert.NotNull(pending.PendingPayload);
            }
            else
            {
                Assert.Same(values[i], prepared[i]);
            }
        }

        int[] shorter = [40, 40, 40, 40, 40];
        object[] fits = Row(tableDef, shorter);
        (object[] unchanged, _) = store.EncodeRow(tableDef, fits);
        Assert.Same(fits, unchanged);
    }

    /// <summary>
    /// Inserts a row of <c>Id</c> plus 250-byte Binary values that alone add
    /// up to more than a page, with two OLE values and a MEMO
    /// value. The long values move to LVAL pages and the row is still too
    /// long, so the insert throws before anything is written, LVAL pages
    /// included: the file is byte-for-byte what it was, in every write mode
    /// (an explicit transaction is committed after the failure).
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">WriteMode.Direct, WriteMode.AutoCommit or WriteMode.ExplicitCommit.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit)]
    public async Task RowLargerThanAPage_ThrowsJetLimitationException_AndWritesNothing(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int binaryColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, binaryColumns, oleColumns: 2, memoColumns: 1, ct);
        object[] row = [1, .. Enumerable.Range(0, binaryColumns).Select(i => (object)Bytes(250, i)), Bytes(250, 100), Bytes(250, 101), new string('m', 900)];

        await AssertOversizedInsertWritesNothingAsync(ms, format, mode, row, ct);
    }

    /// <summary>
    /// The same oversized row with a MEMO value too long to stay inline, so it
    /// is bound for LVAL pages. The writer used to write those pages before it
    /// measured the row, and the failed insert left them behind with nothing
    /// pointing at them: appended to the file, or, once a dropped table had
    /// freed pages, written over the free pages and marked used, with the file
    /// length unchanged. The row is now measured first, with a 12-byte
    /// placeholder for the value's LVAL header.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">WriteMode.Direct, WriteMode.AutoCommit or WriteMode.ExplicitCommit.</param>
    /// <param name="freePages">Whether a dropped table has left free pages for the LVAL pages to reuse.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.Direct, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.Direct, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit, false)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct, false)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct, true)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit, false)]
    public async Task RowLargerThanAPage_WithAMemoBoundForLvalPages_WritesNothing(DatabaseFormat format, WriteMode mode, bool freePages)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int binaryColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, binaryColumns, oleColumns: 0, memoColumns: 1, ct);
        if (freePages)
        {
            await using AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct);
            await writer.CreateTableAsync("Scratch", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Notes", typeof(string))], ct);
            await writer.InsertRowAsync("Scratch", [1, new string('s', 20_000)], ct);
            await writer.DropTableAsync("Scratch", ct);
        }

        object[] row = [1, .. Enumerable.Range(0, binaryColumns).Select(i => (object)Bytes(250, i)), new string('m', 5_000)];

        await AssertOversizedInsertWritesNothingAsync(ms, format, mode, row, ct);
    }

    /// <summary>
    /// A row of exactly the page's capacity (Id plus Binary values) is
    /// written and reads back; one byte more is refused.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RowAtExactPageCapacity_IsWritten(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Jet3: 1 + 4 + 8 values + EOD + 8 offsets + 7 jump entries + var_len + 2 mask bytes = 2,036 with 2,012 bytes of values.
        // Jet4/ACE: 2 + 4 + 16 values + 2 + 32 + 2 + 3 mask bytes = 4,080 with 4,035 bytes of values.
        // The first value is the short one, so one byte more stays under the 255-byte column size.
        int[] lengths = format == DatabaseFormat.Jet3Mdb
            ? [227, .. Enumerable.Repeat(255, 7)]
            : [210, .. Enumerable.Repeat(255, 15)];
        await using MemoryStream ms = await CreateDatabaseAsync(format, lengths.Length, oleColumns: 0, memoColumns: 0, ct);

        object[] fits = [1, .. lengths.Select((length, i) => (object)Bytes(length, i))];
        object[] tooLong = [2, .. lengths.Select((length, i) => (object)Bytes(i == 0 ? length + 1 : length, i))];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.InsertRowAsync(TableName, fits, ct);
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync(TableName, tooLong, ct));
            Assert.Contains($"{MaxRowLength(format) + 1} bytes", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        object[] row = Assert.Single(await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct));
        Assert.Equal(fits, row);
    }

    /// <summary>
    /// An update that grows a row past a page with values that cannot leave
    /// the row (Binary) throws the row-size exception. The update rewrites a
    /// row by deleting it and inserting the new version, and it used to
    /// encode the new version only after the delete, so without a transaction
    /// the row was lost. Every new version is now encoded and measured first:
    /// the row keeps its old values and the file is byte-for-byte what it
    /// was, in every write mode (an explicit transaction is committed after
    /// the failure).
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">WriteMode.Direct, WriteMode.AutoCommit or WriteMode.ExplicitCommit.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit)]
    public async Task UpdateGrowingRowPastAPage_ThrowsJetLimitationException_AndKeepsTheRow(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int binaryColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, binaryColumns, oleColumns: 0, memoColumns: 0, ct);
        object[] original = [1, .. Enumerable.Range(0, binaryColumns).Select(i => (object)Bytes(20, i))];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.InsertRowAsync(TableName, original, ct);
        }

        var changes = new RowValues();
        for (int i = 0; i < binaryColumns; i++)
        {
            changes[$"Bin{i}"] = Bytes(250, i);
        }

        await AssertOversizedUpdateWritesNothingAsync(ms, format, mode, RowCriteria.Where("Id", 1), changes, [original], ct);
    }

    /// <summary>
    /// A multi-row update whose later row grows past a page changes no row,
    /// not even the earlier one, whose new version fits. The second row holds
    /// 250-byte Binary values in all but its first Binary column (2,025 bytes
    /// on Jet3, 4,047 on Jet4 and ACCDB), and the update sets that column to
    /// 250 bytes too.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">WriteMode.Direct, WriteMode.AutoCommit or WriteMode.ExplicitCommit.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit)]
    public async Task UpdateGrowingALaterRowPastAPage_ChangesNoRow(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int binaryColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, binaryColumns, oleColumns: 0, memoColumns: 0, ct);
        object[] fits = [1, .. Enumerable.Repeat<object>(DBNull.Value, binaryColumns)];
        object[] grows = [2, DBNull.Value, .. Enumerable.Range(1, binaryColumns - 1).Select(i => (object)Bytes(250, i))];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            Assert.Equal(2, await writer.InsertRowsAsync(TableName, [fits, grows], ct));
        }

        await AssertOversizedUpdateWritesNothingAsync(
            ms,
            format,
            mode,
            RowCriteria.All(),
            new RowValues { ["Bin0"] = Bytes(250, 0) },
            [fits, grows],
            ct);
    }

    /// <summary>
    /// A multi-row update adds large MEMO values to rows with different
    /// existing external values. Both rows retain their other values and
    /// read back with the new value.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task UpdateRowsWithLargeMemoValues_PreservesExternalValues(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int memoColumns = format == DatabaseFormat.Jet3Mdb ? 3 : 5;
        await using MemoryStream ms = await CreateDatabaseAsync(format, binaryColumns: 0, oleColumns: 0, memoColumns, ct);
        object[] small = [1, .. Enumerable.Repeat<object>(DBNull.Value, memoColumns)];
        object[] grows = [2, DBNull.Value, .. Enumerable.Repeat<object>(new string('b', 900), memoColumns - 1)];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            Assert.Equal(2, await writer.InsertRowsAsync(TableName, [small, grows], ct));
        }

        string newValue = new('a', 1_000);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteModes.WriterOptions(mode), ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () => Assert.Equal(2, await writer.UpdateRowsAsync(TableName, RowCriteria.All(), new RowValues { ["Memo0"] = newValue }, ct)),
                ct);
        }

        small[1] = newValue;
        grows[1] = newValue;
        object[][] expected = [small, grows];
        Assert.Equal(expected, await ReadRowsByIdAsync(ms, ct));
    }

    private static int MaxRowLength(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb ? 2036 : 4080;

    /// <summary>
    /// Builds a row of the table <see cref="EncodeRow_MovesTheLargestInlineLongValueFirst_AndOnlyAsManyAsTheRowNeeds"/>
    /// reads: <c>Id</c> 1 and, in <c>Memo0</c> to <c>Memo4</c>, strings of
    /// <paramref name="lengths"/> characters, each of its own letter.
    /// </summary>
    /// <param name="tableDef">The table.</param>
    /// <param name="lengths">The length of each MEMO value.</param>
    /// <returns>The row, in the table's column order.</returns>
    private static object[] Row(TableDef tableDef, int[] lengths)
    {
        object[] values = new object[tableDef.Columns.Count];
        for (int i = 0; i < values.Length; i++)
        {
            string name = tableDef.Columns[i].Name;
            if (name == "Id")
            {
                values[i] = 1;
            }
            else if (tableDef.Columns[i].Type == ColumnType.BinaryType)
            {
                values[i] = Bytes(tableDef.Columns[i].Size, i);
            }
            else
            {
                values[i] = new string((char)('a' + (name[^1] - '0')), lengths[name[^1] - '0']);
            }
        }

        return values;
    }

    /// <summary>Reads every row of the table through a reopened reader, ordered by <c>Id</c>.</summary>
    /// <param name="ms">The database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The rows.</returns>
    private static async Task<List<object[]>> ReadRowsByIdAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        await using AccessReader reader = await OpenReaderAsync(ms, cancellationToken);
        List<object[]> rows = await reader.Rows(TableName, cancellationToken: cancellationToken).ToListAsync(cancellationToken);
        return [.. rows.OrderBy(row => (int)row[0])];
    }

    /// <summary>
    /// Runs <paramref name="changes"/> on the rows <paramref name="criteria"/>
    /// matches in <paramref name="mode"/>, expects the row-size
    /// <see cref="JetLimitationException"/>, and checks that the file is
    /// byte-for-byte unchanged, the table holds <paramref name="expectedRows"/>
    /// and its stored row count is unchanged.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="format">The database format.</param>
    /// <param name="mode">WriteMode.Direct, WriteMode.AutoCommit or WriteMode.ExplicitCommit.</param>
    /// <param name="criteria">The rows to update.</param>
    /// <param name="changes">Changes that grow a matching row past a page.</param>
    /// <param name="expectedRows">The table's rows, in table order.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task AssertOversizedUpdateWritesNothingAsync(
        MemoryStream ms,
        DatabaseFormat format,
        WriteMode mode,
        RowCriteria criteria,
        RowValues changes,
        object[][] expectedRows,
        CancellationToken cancellationToken)
    {
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit }, cancellationToken))
        {
            JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(cancellationToken) : null;
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(TableName, criteria, changes, cancellationToken));
            Assert.Contains($"{MaxRowLength(format)}-byte maximum", ex.Message, StringComparison.Ordinal);

            if (transaction is not null)
            {
                await transaction.CommitAsync(cancellationToken);
                await transaction.DisposeAsync();
            }
        }

        Assert.Equal(before, ms.ToArray());
        await using AccessReader reader = await OpenReaderAsync(ms, cancellationToken);
        Assert.Equal(expectedRows, await reader.Rows(TableName, cancellationToken: cancellationToken).ToListAsync(cancellationToken));
        TableStat stat = Assert.Single(await reader.GetTableStatsAsync(cancellationToken), s => s.Name == TableName);
        Assert.Equal(expectedRows.Length, stat.RowCount);
    }

    /// <summary>
    /// Inserts <paramref name="row"/> in <paramref name="mode"/>, expects the
    /// row-size <see cref="JetLimitationException"/>, and checks that the file
    /// is byte-for-byte unchanged and the table still empty.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="format">The database format.</param>
    /// <param name="mode">WriteMode.Direct, WriteMode.AutoCommit or WriteMode.ExplicitCommit.</param>
    /// <param name="row">A row larger than a page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task AssertOversizedInsertWritesNothingAsync(MemoryStream ms, DatabaseFormat format, WriteMode mode, object[] row, CancellationToken cancellationToken)
    {
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit }, cancellationToken))
        {
            JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(cancellationToken) : null;
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync(TableName, row, cancellationToken));
            Assert.Contains($"{MaxRowLength(format)}-byte maximum", ex.Message, StringComparison.Ordinal);

            if (transaction is not null)
            {
                await transaction.CommitAsync(cancellationToken);
                await transaction.DisposeAsync();
            }
        }

        Assert.Equal(before.Length, ms.Length);
        Assert.Equal(before, ms.ToArray());
        await using AccessReader reader = await OpenReaderAsync(ms, cancellationToken);
        Assert.Equal(0, await reader.GetRealRowCountAsync(TableName, cancellationToken));
        using DataTable table = await reader.ReadTableAsync(TableName, cancellationToken: cancellationToken);
        Assert.Empty(table.Rows);
    }

    /// <summary>
    /// Binary or OLE bytes that start <c>11 22</c> and carry no file
    /// signature, so the read API returns them unchanged.
    /// </summary>
    /// <param name="length">The length.</param>
    /// <param name="salt">Varies the bytes per column.</param>
    /// <returns>The bytes.</returns>
    private static byte[] Bytes(int length, int salt)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = unchecked((byte)((i * 7) + salt));
        }

        bytes[0] = 0x11;
        bytes[1] = 0x22;
        return bytes;
    }

    /// <summary>
    /// Creates a database whose table <see cref="TableName"/> has <c>Id</c>,
    /// then <paramref name="binaryColumns"/> Binary(255) columns <c>Bin0</c>…,
    /// <paramref name="oleColumns"/> OLE columns <c>Blob0</c>… and
    /// <paramref name="memoColumns"/> MEMO columns <c>Memo0</c>….
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="binaryColumns">The number of Binary columns.</param>
    /// <param name="oleColumns">The number of OLE columns.</param>
    /// <param name="memoColumns">The number of MEMO columns.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The database, in memory.</returns>
    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, int binaryColumns, int oleColumns, int memoColumns, CancellationToken cancellationToken)
    {
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) };
        for (int i = 0; i < binaryColumns; i++)
        {
            // Growth tests require native variable BINARY storage; ordinary BINARY is fixed-width.
            columns.Add(new ColumnDefinition($"Bin{i}", typeof(byte[]), maxLength: 255) { DescriptorFlagsOverride = 0x02 });
        }

        for (int i = 0; i < oleColumns; i++)
        {
            columns.Add(new ColumnDefinition($"Blob{i}", typeof(byte[])));
        }

        for (int i = 0; i < memoColumns; i++)
        {
            columns.Add(new ColumnDefinition($"Memo{i}", typeof(string)));
        }

        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync(TableName, columns, cancellationToken);
        }

        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream ms, AccessWriterOptions options, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessWriter.OpenAsync(ms, options, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }
}
