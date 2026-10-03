namespace JetDatabaseWriter.Tests.Writer;

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
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

#pragma warning disable CA5394 // Seeded test data; nothing here needs a secure generator.

/// <summary>
/// Jet3 (Access 97) rows longer than 255 bytes. A Jet3 row stores the EOD and
/// each variable-column offset in one byte and takes their high parts from a
/// jump table of <c>(rowLength - 1) / 256</c> entries, each the index of the
/// first offset at or past its 256-byte boundary, with <c>0xFF</c> for a
/// boundary no offset reaches (<see cref="Jet3JumpTable"/>). The encoder wrote
/// the EOD and offsets with a checked one-byte cast, so any row whose
/// variable data or fixed area reached offset 256 threw
/// <see cref="OverflowException"/>; it also sized the jump table by
/// <c>rowLength / 256</c> and left it zero. Rows are checked here through the
/// public read APIs and byte by byte against
/// <see cref="Jet3RowTrailerReference"/>, a port of Jackcess's reader.
/// </summary>
public sealed class Jet3LongRowTests
{
    private const string TableName = "T";
    private const int Jet3PageSize = 2048;

    /// <summary>
    /// Text lengths of T1..T4 (0 is null) for the row under test, the jump
    /// bytes in stored order and the row length. The row is
    /// <c>num_cols</c>, a 4-byte Id, the text, the EOD, four offsets, the
    /// jump table, <c>var_len</c> and one null-mask byte: 12 bytes plus the
    /// text plus the jump table, with the text starting at offset 5.
    /// </summary>
    private static readonly (string Lengths, string JumpHex, int RowLength)[] BoundaryCases =
    [
        ("244,0,0,0", string.Empty, 256), // exactly 256 bytes: no jump table
        ("250,0,0,0", "FF", 263), // EOD 255: the one entry is a dummy
        ("251,0,0,0", "01", 264), // EOD 256: the null T2's offset is the first at 256
        ("251,1,0,0", "01", 265), // T2 starts at 256, low byte 00
        ("200,200,0,0", "02", 413), // only T3's offset and the EOD (405) cross 256
        ("100,255,0,0", "02", 368), // T2 straddles 256
        ("255,244,0,0", "01", 512), // a 512-byte row has one entry
        ("255,245,0,0", "FF01", 514), // two entries, the second a dummy
        ("255,251,0,0", "FF01", 520), // EOD 511
        ("255,252,0,0", "0201", 521), // EOD 512
        ("255,255,2,0", "0201", 526), // T3 starts at 515
        ("255,255,255,255", "04030201", 1036), // EOD 1025, four entries
        ("0,0,0,255", "04", 268), // nulls before a long value: only the EOD crosses 256
    ];

    private enum WriteMode
    {
        /// <summary>Each write commits on its own.</summary>
        AutoCommit = 0,

        /// <summary>The writer journals each operation (<see cref="AccessWriterOptions.UseTransactionalWrites"/>).</summary>
        TransactionalWrites = 1,

        /// <summary>One explicit transaction holds every write, so the update reads the long row back from the journal.</summary>
        ExplicitTransaction = 2,
    }

    /// <summary>Gets every boundary case in every write mode.</summary>
    public static TheoryData<string, string, int, string> BoundaryRows
    {
        get
        {
            var data = new TheoryData<string, string, int, string>();
            foreach ((string lengths, string jumpHex, int rowLength) in BoundaryCases)
            {
                foreach (string mode in Enum.GetNames<WriteMode>())
                {
                    data.Add(lengths, jumpHex, rowLength, mode);
                }
            }

            return data;
        }
    }

    /// <summary>Gets the write modes.</summary>
    public static TheoryData<string> WriteModes => [.. Enum.GetNames<WriteMode>()];

    /// <summary>
    /// Inserts a row of Id + four Text(255) values at a jump-table boundary,
    /// inserts a short row and updates it to 517 bytes of text. Every value
    /// must read back, and every stored row must carry the trailer Jet writes.
    /// </summary>
    /// <param name="lengths">The T1..T4 lengths, comma-separated; 0 is null.</param>
    /// <param name="jumpHex">The boundary row's jump bytes in stored order, as hex.</param>
    /// <param name="rowLength">The boundary row's length.</param>
    /// <param name="mode">The <see cref="WriteMode"/>.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(BoundaryRows))]
    public async Task TextRow_AtJumpBoundaries_RoundTripsAndMatchesJetLayout(string lengths, string jumpHex, int rowLength, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        Assert.NotNull(lengths);
        string?[] boundary = [.. lengths.Split(',').Select((s, i) => Text((char)('a' + i), int.Parse(s, CultureInfo.InvariantCulture)))];
        string?[] updated = ["x", "y", Text('p', 255), Text('q', 255)];

        await using MemoryStream ms = await CreateDatabaseAsync(
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("T1", typeof(string), maxLength: 255),
                new ColumnDefinition("T2", typeof(string), maxLength: 255),
                new ColumnDefinition("T3", typeof(string), maxLength: 255),
                new ColumnDefinition("T4", typeof(string), maxLength: 255),
            ],
            [new IndexDefinition("IX_T2", "T2")],
            ct);

        await WriteInModeAsync(
            ms,
            mode,
            async writer =>
            {
                await writer.InsertRowAsync(TableName, [1, .. boundary.Select(Cell)], ct);
                await writer.InsertRowAsync(TableName, [2, "x", "y", DBNull.Value, DBNull.Value], ct);
                int changed = await writer.UpdateRowsAsync(TableName, "Id", 2, new Dictionary<string, object?> { ["T3"] = updated[2], ["T4"] = updated[3] }, ct);
                Assert.Equal(1, changed);
            },
            ct);

        var expected = new Dictionary<int, string?[]> { [1] = boundary, [2] = updated };
        await using (AccessReader reader = await OpenReaderAsync(ms, ct))
        {
            using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
            Assert.Equal(2, table.Rows.Count);
            foreach (DataRow row in table.Rows)
            {
                string?[] values = expected[(int)row["Id"]];
                for (int i = 0; i < 4; i++)
                {
                    Assert.Equal(Cell(values[i]), row[i + 1]);
                }
            }

            List<string[]> strings = await reader.RowsAsStrings(TableName, cancellationToken: ct).ToListAsync(ct);
            Assert.Equal(2, strings.Count);
            foreach (string[] row in strings)
            {
                string?[] values = expected[int.Parse(row[0], CultureInfo.InvariantCulture)];
                Assert.Equal(values.Select(v => v ?? string.Empty), row.Skip(1));
            }
        }

        Dictionary<int, byte[]> stored = await ReadStoredRowsAsync(ms, TableName, keyed: true, ct);
        Assert.Equal(2, stored.Count);
        Assert.Equal(rowLength, stored[1].Length);
        Assert.Equal(Convert.FromHexString(jumpHex), Jet3RowTrailerReference.JumpBytes(stored[1], nullMaskSize: 1));
        foreach ((int id, byte[] row) in stored)
        {
            AssertJetTrailer(row, nullMaskSize: 1, ExpectedOffsets(5, expected[id].Select(v => v?.Length ?? 0)));
        }
    }

    /// <summary>
    /// A table of 200 Long columns has no variable columns but an 801-byte
    /// fixed area, so its EOD byte overflowed. The row keeps the writer's
    /// trailer (EOD low byte, three jump entries naming the EOD, var_len 0).
    /// </summary>
    /// <param name="mode">The <see cref="WriteMode"/>.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task FixedOnlyRow_200LongColumns_RoundTrips(string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int columnCount = 200;
        await using MemoryStream ms = await CreateDatabaseAsync(
            [.. Enumerable.Range(0, columnCount).Select(i => new ColumnDefinition(ColumnName(i), typeof(int)))],
            [new IndexDefinition("UX_C000", "C000") { IsUnique = true }],
            ct);

        await WriteInModeAsync(
            ms,
            mode,
            async writer =>
            {
                _ = await writer.InsertRowsAsync(TableName, [.. Enumerable.Range(1, 5).Select(LongRow)], ct);
                Assert.Equal(1, await writer.UpdateRowsAsync(TableName, "C000", 3, new Dictionary<string, object?> { ["C000"] = 33, ["C199"] = -1 }, ct));
                Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 4, ct));
                _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync(TableName, LongRow(5), ct));
            },
            ct);

        await using (AccessReader reader = await OpenReaderAsync(ms, ct))
        {
            using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
            Assert.Equal([1, 2, 5, 33], table.AsEnumerable().Select(r => (int)r["C000"]).Order());
            foreach (DataRow row in table.Rows)
            {
                int key = (int)row["C000"];
                object[] expected = key == 33 ? LongRow(3) : LongRow(key);
                if (key == 33)
                {
                    expected[0] = 33;
                    expected[columnCount - 1] = -1;
                }

                Assert.Equal<object?>(expected, row.ItemArray);
            }
        }

        Dictionary<int, byte[]> stored = await ReadStoredRowsAsync(ms, TableName, keyed: true, ct);
        Assert.Equal(4, stored.Count);
        foreach (byte[] row in stored.Values)
        {
            // 1 + 800 + EOD + var_len + 25 mask bytes = 828, plus three jump entries.
            Assert.Equal(831, row.Length);
            Assert.Equal(0x21, row[801]);
            Assert.Equal([0, 0, 0], Jet3RowTrailerReference.JumpBytes(row, nullMaskSize: 25));
            Assert.Equal(0, row[805]);
        }

        static object[] LongRow(int key) => [key, .. Enumerable.Range(1, columnCount - 1).Select(i => (object)((key * 1000) + i))];
    }

    /// <summary>
    /// Rebuilding the trailer of the Access 97 row in test2V1997.mdb
    /// (MSP_PROJECTS: 290 bytes, 22 variable columns, EOD 255) through
    /// <see cref="Jet3JumpTable"/> gives Access's bytes, including the
    /// <c>0xFF</c> dummy.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task Trailer_MatchesAccessAuthoredRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (!File.Exists(TestDatabases.Test2V1997))
        {
            Assert.Skip("test2V1997.mdb is unavailable on this machine.");
        }

        byte[] row;
        await using (var copy = new MemoryStream(await File.ReadAllBytesAsync(TestDatabases.Test2V1997, ct)))
        {
            row = Assert.Single((await ReadStoredRowsAsync(copy, "MSP_PROJECTS", keyed: false, ct)).Values);
        }

        int nullMaskSize = (row[0] + 7) / 8;
        int[] offsets = Jet3RowTrailerReference.DecodeVarOffsets(row, nullMaskSize);
        int varLen = offsets.Length - 1;
        int eod = offsets[^1];
        Assert.Equal(22, varLen);
        Assert.Equal(255, eod);

        int jumpCount = Jet3JumpTable.CountForLength(eod + 1 + varLen + 1 + nullMaskSize);
        Assert.Equal(row.Length, eod + 1 + varLen + jumpCount + 1 + nullMaskSize);
        byte[] jumps = new byte[jumpCount];
        Jet3JumpTable.Write(jumps, offsets.AsSpan(0, varLen), eod);

        var trailer = new List<byte> { unchecked((byte)eod) };
        for (int i = varLen - 1; i >= 0; i--)
        {
            trailer.Add(unchecked((byte)offsets[i]));
        }

        trailer.AddRange(jumps);
        trailer.Add((byte)varLen);
        trailer.AddRange(row.AsSpan(row.Length - nullMaskSize).ToArray());
        Assert.Equal(row.AsSpan(eod).ToArray(), trailer.ToArray());
        Assert.Equal("FFF3F3F3F2F1F0EFDBDBDBDBC7C7B3B3B3B3B2A5A59991FF16", Convert.ToHexString(row, eod, 1 + varLen + jumpCount + 1));
    }

    /// <summary>The jump table holds (rowLength - 1) / 256 entries, so a row's entry count depends on its own length.</summary>
    /// <param name="lengthWithoutJumps">The row length before the jump table.</param>
    /// <param name="expected">The jump-entry count.</param>
    [Theory]
    [InlineData(1, 0)]
    [InlineData(254, 0)]
    [InlineData(255, 0)]
    [InlineData(256, 0)]
    [InlineData(257, 1)]
    [InlineData(510, 1)]
    [InlineData(511, 1)]
    [InlineData(512, 2)]
    [InlineData(766, 2)]
    [InlineData(767, 3)]
    [InlineData(2028, 7)]
    public void CountForLength_FollowsRowLengthMinusOneRule(int lengthWithoutJumps, int expected)
    {
        int count = Jet3JumpTable.CountForLength(lengthWithoutJumps);
        Assert.Equal(expected, count);
        Assert.Equal(count, Jet3JumpTable.EntryCount(lengthWithoutJumps + count));
    }

    /// <summary>
    /// Random rows of Long and Binary columns, a quarter of them steered so
    /// the EOD lands within two bytes of a 256-byte boundary, serialized by
    /// <see cref="RowEncoder.SerializeRow"/>: Jackcess's reader and the
    /// library's reader must both find every value where it was put.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task RandomRows_EncoderAndReaderAgree()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(null, [], ct);
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: ct);
        var encoder = new RowEncoder(harness.Database);
        var random = new Random(20261003);
        int withJumps = 0;
        for (int iteration = 0; iteration < 20_000; iteration++)
        {
            int fixedCount = random.Next(0, 61);
            int varCount = random.Next(1, 41);
            int budget = random.Next(0, 1701);
            int[] lengths = new int[varCount];
            for (int i = 0; i < varCount; i++)
            {
                lengths[i] = random.Next(5) == 0 ? -1 : random.Next(0, (budget / varCount * 2) + 1);
            }

            int dataStart = 1 + (fixedCount * 4);
            if (iteration % 4 == 0)
            {
                int target = (256 * random.Next(1, 7)) + random.Next(-2, 3);
                int last = varCount - 1;
                int adjusted = Math.Max(lengths[last], 0) + target - (dataStart + lengths.Sum(l => Math.Max(l, 0)));
                if (adjusted >= 0)
                {
                    lengths[last] = adjusted;
                }
            }

            if (dataStart + lengths.Sum(l => Math.Max(l, 0)) > 1950)
            {
                continue;
            }

            (TableDef tableDef, object[] values) = BuildRow(random, fixedCount, lengths);
            byte[] row = encoder.SerializeRow(tableDef, values);
            int nullMaskSize = (tableDef.Columns.Count + 7) / 8;
            int[] expectedOffsets = ExpectedOffsets(dataStart, lengths.Select(l => Math.Max(l, 0)));
            AssertJetTrailer(row, nullMaskSize, expectedOffsets);
            AssertLibraryReads(row, tableDef, values);
            if (row.Length > 256)
            {
                withJumps++;
            }
        }

        Assert.True(withJumps > 5_000, $"Only {withJumps} rows carried a jump table.");
    }

    /// <summary>
    /// A Jet3 row stores <c>num_cols</c> in one byte, and Access allows 255
    /// fields per table. CreateTableAsync and AddColumnAsync reject a 256th
    /// column before writing anything, and a row of a wider table (from a
    /// file written elsewhere) fails to encode with the same exception. A
    /// 255-column row of Longs round-trips.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task Jet3_MoreThan255Columns_ThrowsJetLimitationException()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        List<ColumnDefinition> columns = [.. Enumerable.Range(0, 255).Select(i => new ColumnDefinition(ColumnName(i), typeof(int)))];
        await using MemoryStream ms = await CreateDatabaseAsync(columns, [], ct);
        long lengthBefore = ms.Length;

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            JetLimitationException create = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.CreateTableAsync("Wide", [.. columns, new ColumnDefinition("C255", typeof(int))], ct));
            Assert.Contains("255", create.Message, StringComparison.Ordinal);

            JetLimitationException add = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.AddColumnAsync(TableName, new ColumnDefinition("C255", typeof(int)), ct));
            Assert.Contains("255", add.Message, StringComparison.Ordinal);
        }

        Assert.Equal(lengthBefore, ms.Length);
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.InsertRowAsync(TableName, [.. Enumerable.Range(0, 255).Select(i => (object)(i - 100))], ct);
        }

        await using (AccessReader reader = await OpenReaderAsync(ms, ct))
        {
            Assert.DoesNotContain("Wide", await reader.ListTablesAsync(ct));
            Assert.Equal(255, (await reader.GetColumnMetadataAsync(TableName, ct)).Count);
            object[] row = Assert.Single(await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct));
            Assert.Equal(Enumerable.Range(0, 255).Select(i => (object)(i - 100)), row);
        }

        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: ct);
        var wide = new TableDef
        {
            Columns = [.. Enumerable.Range(0, 256).Select(FixedLong)],
        };
        wide.InitializeColumnMetadata();
        JetLimitationException encode = Assert.Throws<JetLimitationException>(() =>
            new RowEncoder(harness.Database).SerializeRow(wide, [.. Enumerable.Range(0, 256).Select(i => (object)i)]));
        Assert.Contains("255", encode.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// With 255 variable columns the EOD's index is 255, the same byte as a
    /// dummy entry. One dummy is the last entry, which readers drop; a row
    /// that needs two cannot be read back correctly, so it is refused.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task Jet3Row_255VariableColumnsNeedingTwoDummyEntries_ThrowsJetLimitationException()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(null, [], ct);
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: ct);
        var encoder = new RowEncoder(harness.Database);
        int[] lengths = [.. Enumerable.Repeat(-1, 255)];
        (TableDef tableDef, object[] values) = BuildRow(new Random(1), fixedCount: 0, lengths);

        // EOD 300: 1 + 299 + EOD + 255 offsets + 2 entries + var_len + 32 mask bytes = 591; the 512 entry is the one dummy.
        values[0] = new byte[299];
        byte[] row = encoder.SerializeRow(tableDef, values);
        Assert.Equal(591, row.Length);
        Assert.Equal([0xFF, 0x01], Jet3RowTrailerReference.JumpBytes(row, nullMaskSize: 32));
        AssertJetTrailer(row, nullMaskSize: 32, ExpectedOffsets(1, [299, .. Enumerable.Repeat(0, 254)]));
        AssertLibraryReads(row, tableDef, values);

        // EOD 250: the 256 and 512 entries are both dummies.
        values[0] = new byte[249];
        JetLimitationException ex = Assert.Throws<JetLimitationException>(() => encoder.SerializeRow(tableDef, values));
        Assert.Contains("255 variable", ex.Message, StringComparison.Ordinal);
    }

    private static string ColumnName(int i) => "C" + i.ToString("D3", CultureInfo.InvariantCulture);

    private static object Cell(string? value) => value ?? (object)DBNull.Value;

    private static string? Text(char first, int length)
    {
        if (length == 0)
        {
            return null;
        }

        var text = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            text.Append((char)(first + (i % 10)));
        }

        return text.ToString();
    }

    private static int[] ExpectedOffsets(int dataStart, IEnumerable<int> lengths)
    {
        var offsets = new List<int> { dataStart };
        foreach (int length in lengths)
        {
            offsets.Add(offsets[^1] + length);
        }

        return [.. offsets];
    }

    /// <summary>
    /// Checks a Jet3 row's trailer against Jackcess's reading of it: the
    /// offsets and EOD are <paramref name="expectedOffsets"/>, the EOD byte
    /// sits at the EOD, and the jump table has <c>(rowLength - 1) / 256</c>
    /// entries naming the first offset at or past each boundary, with
    /// <c>0xFF</c> for a boundary none reaches.
    /// </summary>
    /// <param name="row">The row bytes.</param>
    /// <param name="nullMaskSize">The null-mask size.</param>
    /// <param name="expectedOffsets">The variable-column offsets, then the EOD.</param>
    private static void AssertJetTrailer(byte[] row, int nullMaskSize, int[] expectedOffsets)
    {
        int varLen = expectedOffsets.Length - 1;
        int jumpCount = (row.Length - 1) / 256;
        Assert.Equal(row.Length, expectedOffsets[^1] + 1 + varLen + jumpCount + 1 + nullMaskSize);
        Assert.Equal(varLen, row[row.Length - nullMaskSize - 1]);
        Assert.Equal(expectedOffsets, Jet3RowTrailerReference.DecodeVarOffsets(row, nullMaskSize));
        Assert.Equal(Jet3RowTrailerReference.ExpectedJumpBytes(expectedOffsets, jumpCount), Jet3RowTrailerReference.JumpBytes(row, nullMaskSize));
    }

    /// <summary>Places <paramref name="row"/> at the end of a Jet3 page and reads every column through the library's row parser.</summary>
    /// <param name="row">The row bytes.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="values">The values the row was built from.</param>
    private static void AssertLibraryReads(byte[] row, TableDef tableDef, object[] values)
    {
        byte[] page = new byte[Jet3PageSize];
        int rowStart = Jet3PageSize - row.Length;
        row.CopyTo(page, rowStart);
        var rowFields = RowFieldSizes.For(DatabaseFormat.Jet3Mdb);
        Assert.True(RowDecodePlan.TryParseRowLayout(DatabaseFormat.Jet3Mdb, rowFields, page, rowStart, row.Length, tableDef.HasVarColumns, out RowLayout layout));
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(rowFields, page, rowStart, row.Length, layout, tableDef.Columns[i]);
            if (values[i] is DBNull)
            {
                Assert.Equal(ColumnSliceKind.Null, slice.Kind);
                continue;
            }

            byte[] expected = values[i] as byte[] ?? BitConverter.GetBytes((int)values[i]);
            Assert.Equal(expected, page.AsSpan(rowStart + slice.DataStart, slice.DataLen).ToArray());
        }
    }

    private static ColumnInfo FixedLong(int colNum) => new()
    {
        Type = ColumnType.LongIntegerType,
        ColNum = colNum,
        FixedOff = colNum * 4,
        Size = 4,
        Flags = Constants.ColumnDescriptorFlags.Fixed,
        Name = ColumnName(colNum),
    };

    /// <summary>
    /// Builds a table of <paramref name="fixedCount"/> Long columns then one
    /// Binary column per entry of <paramref name="lengths"/>, and a row with a
    /// random value in each (-1 is null).
    /// </summary>
    /// <param name="random">The random source.</param>
    /// <param name="fixedCount">The number of Long columns.</param>
    /// <param name="lengths">The Binary value lengths; -1 is null.</param>
    /// <returns>The table definition and the row values.</returns>
    private static (TableDef TableDef, object[] Values) BuildRow(Random random, int fixedCount, int[] lengths)
    {
        var tableDef = new TableDef();
        var values = new List<object>();
        for (int i = 0; i < fixedCount; i++)
        {
            tableDef.Columns.Add(FixedLong(i));
            values.Add(random.Next());
        }

        for (int i = 0; i < lengths.Length; i++)
        {
            tableDef.Columns.Add(new ColumnInfo
            {
                Type = ColumnType.BinaryType,
                ColNum = fixedCount + i,
                VarIdx = i,
                Name = "V" + i.ToString(CultureInfo.InvariantCulture),
            });

            if (lengths[i] < 0)
            {
                values.Add(DBNull.Value);
                continue;
            }

            byte[] value = new byte[lengths[i]];
            random.NextBytes(value);
            values.Add(value);
        }

        tableDef.InitializeColumnMetadata();
        return (tableDef, [.. values]);
    }

    /// <summary>
    /// Returns the stored bytes of every live row of <paramref name="table"/>,
    /// keyed by the Long at offset 1 (the first fixed column) or, when
    /// <paramref name="keyed"/> is false, by row order.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="table">The table name.</param>
    /// <param name="keyed">Whether to key rows by their first fixed Long.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The row bytes.</returns>
    private static async Task<Dictionary<int, byte[]>> ReadStoredRowsAsync(MemoryStream ms, string table, bool keyed, CancellationToken cancellationToken)
    {
        var rows = new Dictionary<int, byte[]>();
        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(table, cancellationToken));
        await harness.Database.ForEachLiveTableRowAsync(
            entry.TDefPage,
            (row, _) =>
            {
                byte[] bytes = row.Page.AsSpan(row.Location.RowStart, row.Location.RowSize).ToArray();
                rows.Add(keyed ? BitConverter.ToInt32(bytes, 1) : rows.Count, bytes);
                return new ValueTask<bool>(true);
            },
            cancellationToken);
        return rows;
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(IReadOnlyList<ColumnDefinition>? columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            if (columns is not null)
            {
                await writer.CreateTableAsync(TableName, columns, indexes, cancellationToken);
            }
        }

        return ms;
    }

    private static async Task WriteInModeAsync(MemoryStream ms, string mode, Func<AccessWriter, Task> work, CancellationToken cancellationToken)
    {
        WriteMode writeMode = Enum.Parse<WriteMode>(mode);
        var options = new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = writeMode == WriteMode.TransactionalWrites };
        await using AccessWriter writer = await OpenWriterAsync(ms, options, cancellationToken);
        if (writeMode == WriteMode.ExplicitTransaction)
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(cancellationToken);
            await work(writer);
            await transaction.CommitAsync(cancellationToken);
        }
        else
        {
            await work(writer);
        }
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
