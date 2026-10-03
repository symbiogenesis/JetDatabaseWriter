namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Regression tests for complex-column cells on the row read paths
/// (<c>Rows()</c>, <c>ReadTableAsync</c>, <c>Rows&lt;T&gt;</c>,
/// <c>RowsAsStrings</c>, <c>ReadTableAsStringsAsync</c>). Every attachment and
/// multi-value item of a parent row must reach its cell, decoded the same way
/// <see cref="AccessReader.GetAttachmentsAsync"/> and
/// <see cref="AccessReader.GetMultiValueItemsAsync"/> decode it.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class ComplexColumnsRowReadTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string DataUriPrefix = "data:application/octet-stream;base64,";

    private static readonly byte[] FirstContent = Encoding.UTF8.GetBytes("FIRST-CONTENT FIRST-CONTENT FIRST-CONTENT");

    private static readonly byte[] SecondContent = Encoding.UTF8.GetBytes("SECOND-CONTENT");

    /// <summary>
    /// A raw-stored (jpg) payload with a JPEG signature after two leading bytes.
    /// The OLE signature sniffing on the typed path used to slice it at FF D8 FF.
    /// </summary>
    private static readonly byte[] PhotoContent = [0x01, 0x02, 0xFF, 0xD8, 0xFF, 0xE0, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00];

    private static readonly byte[] ThirdContent = Encoding.UTF8.GetBytes("THIRD");

    [Fact]
    public async Task Rows_ParentWithSeveralAttachments_CellHoldsEveryAttachmentDecoded()
    {
        await using MemoryStream ms = await CreateFixtureAsync();
        await using AccessReader reader = await OpenAsync(ms);

        Dictionary<int, object> cells = await ReadCellsAsync(reader, "Docs");
        IReadOnlyList<AttachmentRecord> all = await reader.GetAttachmentsAsync("Docs", "Files", TestContext.Current.CancellationToken);

        IReadOnlyList<AttachmentRecord> row1 = ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(cells[1]));
        Assert.Equal(["first.txt", "photo.jpg", "second.txt"], row1.Select(a => a.FileName));
        Assert.Equal(["txt", "jpg", "txt"], row1.Select(a => a.FileType));
        Assert.Equal(FirstContent, row1[0].FileData);
        Assert.Equal(PhotoContent, row1[1].FileData);
        Assert.Equal(SecondContent, row1[2].FileData);
        AssertSameAttachments(all.Where(a => a.ConceptualTableId == row1[0].ConceptualTableId).ToList(), row1);

        Assert.IsType<DBNull>(cells[2]);

        IReadOnlyList<AttachmentRecord> row3 = ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(cells[3]));
        AttachmentRecord third = Assert.Single(row3);
        Assert.Equal("third.txt", third.FileName);
        Assert.Equal(ThirdContent, third.FileData);
        Assert.NotEqual(row1[0].ConceptualTableId, third.ConceptualTableId);
        AssertSameAttachments(all.Where(a => a.ConceptualTableId == third.ConceptualTableId).ToList(), row3);
    }

    [Fact]
    public async Task GetAttachmentsAsync_RawStoredPayloadWithFileSignature_ReturnsWholePayload()
    {
        await using MemoryStream ms = await CreateFixtureAsync();
        await using AccessReader reader = await OpenAsync(ms);

        IReadOnlyList<AttachmentRecord> all = await reader.GetAttachmentsAsync("Docs", "Files", TestContext.Current.CancellationToken);

        AttachmentRecord photo = Assert.Single(all, a => a.FileName == "photo.jpg");
        Assert.Equal(PhotoContent, photo.FileData);
    }

    [Fact]
    public async Task Rows_MultiValueCell_HoldsEveryValueOfTheParentRow()
    {
        await using MemoryStream ms = await CreateFixtureAsync();
        await using AccessReader reader = await OpenAsync(ms);

        Dictionary<int, object> cells = await ReadCellsAsync(reader, "Tags");
        IReadOnlyList<MultiValueItem> all = await reader.GetMultiValueItemsAsync("Tags", "Labels", TestContext.Current.CancellationToken);

        IReadOnlyList<MultiValueItem> row1 = ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(cells[1]));
        Assert.Equal([10, 20, 30], row1.Select(i => Assert.IsType<int>(i.Value)));
        Assert.Equal(all.Where(i => i.ConceptualTableId == row1[0].ConceptualTableId), row1);

        Assert.IsType<DBNull>(cells[2]);

        IReadOnlyList<MultiValueItem> row3 = ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(cells[3]));
        Assert.Equal(40, Assert.IsType<int>(Assert.Single(row3).Value));
        Assert.Equal(all.Where(i => i.ConceptualTableId == row3[0].ConceptualTableId), row3);
    }

    [Fact]
    public async Task Rows_TextMultiValueCell_HoldsEveryValue()
    {
        await using MemoryStream ms = await CreateFixtureAsync();
        await using AccessReader reader = await OpenAsync(ms);

        Dictionary<int, object> cells = await ReadCellsAsync(reader, "Products");

        IReadOnlyList<MultiValueItem> items = ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(cells[1]));
        Assert.Equal(["red", string.Empty, "blue"], items.Select(i => Assert.IsType<string>(i.Value)));
    }

    [Fact]
    public async Task ReadTableAsync_ComplexCells_MatchRows()
    {
        await using MemoryStream ms = await CreateFixtureAsync();
        await using AccessReader reader = await OpenAsync(ms);

        foreach (string table in new[] { "Docs", "Tags" })
        {
            Dictionary<int, object> expected = await ReadCellsAsync(reader, table);
            using DataTable dt = await reader.ReadTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(typeof(byte[]), dt.Columns[1].DataType);
            Assert.Equal(expected.Count, dt.Rows.Count);
            foreach (DataRow row in dt.Rows)
            {
                Assert.Equal(expected[(int)row[0]], row[1]);
            }
        }
    }

    [Fact]
    public async Task RowsOfT_ByteArrayProperty_BindsTheComplexCell()
    {
        await using MemoryStream ms = await CreateFixtureAsync();
        await using AccessReader reader = await OpenAsync(ms);

        Dictionary<int, object> expected = await ReadCellsAsync(reader, "Docs");
        List<DocRow> mapped = await reader.Rows<DocRow>("Docs", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(3, mapped.Count);
        foreach (DocRow row in mapped)
        {
            Assert.Equal(expected[row.Id] as byte[], row.Files);
        }

        DocRow filtered = await reader.Rows<DocRow>("Docs", d => d.Id == 1, cancellationToken: TestContext.Current.CancellationToken).SingleAsync(TestContext.Current.CancellationToken);
        Assert.Equal(3, ComplexCellValue.ReadAttachments(filtered.Files!).Count);
    }

    [Fact]
    public async Task RowsAsStrings_ComplexCells_AreDataUrisOfTheTypedCell()
    {
        await using MemoryStream ms = await CreateFixtureAsync();
        await using AccessReader reader = await OpenAsync(ms);

        foreach (string table in new[] { "Docs", "Tags" })
        {
            Dictionary<int, object> expected = await ReadCellsAsync(reader, table);
            List<string[]> streamed = await reader.RowsAsStrings(table, cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
            using DataTable dt = await reader.ReadTableAsStringsAsync(table, cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(expected.Count, streamed.Count);
            Assert.Equal(expected.Count, dt.Rows.Count);
            for (int i = 0; i < streamed.Count; i++)
            {
                int id = int.Parse(streamed[i][0], System.Globalization.CultureInfo.InvariantCulture);
                Assert.Equal(ExpectedString(expected[id]), streamed[i][1]);
                Assert.Equal(ExpectedString(expected[id]), (string)dt.Rows[i][1]);
            }
        }

        static string ExpectedString(object cell) => cell is byte[] bytes ? DataUriPrefix + Convert.ToBase64String(bytes) : string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task Rows_AccessFixture_AttachmentCellsJoinOnTheParentReference(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        Dictionary<string, object[]> rows = await ReadFixtureRowsAsync(reader);
        IReadOnlyList<AttachmentRecord> all = await reader.GetAttachmentsAsync("Table1", "attach-data", TestContext.Current.CancellationToken);
        const int attach = 5;

        Assert.IsType<DBNull>(rows["row1"][attach]);
        Assert.IsType<DBNull>(rows["row3"][attach]);

        IReadOnlyList<AttachmentRecord> row2 = ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(rows["row2"][attach]));
        Assert.Equal(["test_data.txt", "test_data2.txt"], row2.Select(a => a.FileName));
        AssertSameAttachments(all.Where(a => a.ConceptualTableId == row2[0].ConceptualTableId).ToList(), row2);

        IReadOnlyList<AttachmentRecord> row4 = ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(rows["row4"][attach]));
        Assert.Equal("test_data2.txt", Assert.Single(row4).FileName);

        // Access stores txt attachments as zlib-wrapped deflate; the payload must come back inflated.
        Assert.All(row2.Concat(row4), a => Assert.StartsWith("this is ", Encoding.ASCII.GetString(a.FileData), StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task Rows_AccessFixture_MultiValueAndVersionHistoryCellsHoldEveryValue(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        Dictionary<string, object[]> rows = await ReadFixtureRowsAsync(reader);
        const int versionHistory = 1;
        const int multiValue = 4;

        Assert.IsType<DBNull>(rows["row1"][multiValue]);
        Assert.IsType<DBNull>(rows["row4"][multiValue]);
        Assert.Equal(["value1", "value4"], ReadValues(rows["row2"][multiValue]));
        Assert.Equal(["value1", "value2", "value3", "value4"], ReadValues(rows["row3"][multiValue]));

        Assert.IsType<DBNull>(rows["row1"][versionHistory]);
        Assert.Equal(["row2-memo"], ReadValues(rows["row2"][versionHistory]));
        Assert.Equal(["row3-memo", "row3-memo-revised", "row3-memo-again"], ReadValues(rows["row3"][versionHistory]));
        Assert.Equal(["row4-memo"], ReadValues(rows["row4"][versionHistory]));

        static IEnumerable<string> ReadValues(object cell)
            => ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(cell)).Select(i => Assert.IsType<string>(i.Value));
    }

    /// <summary>
    /// A version-history cell carries each version's <c>Modified_&lt;GUID&gt;</c>
    /// timestamp from the flat table, not just its text.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task Rows_AccessFixture_VersionHistoryCellsCarryModified(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        Dictionary<string, object[]> rows = await ReadFixtureRowsAsync(reader);
        const int versionHistory = 1;

        // The flat table holds the FK, the version text (Memo), Modified_<GUID> and its AutoNumber key.
        ComplexColumnInfo column = Assert.Single(await reader.GetComplexColumnsAsync("Table1", TestContext.Current.CancellationToken), c => c.Kind == ComplexColumnKind.VersionHistory);
        List<object[]> flatRows = await reader.Rows(column.FlatTableName, cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        int text = Array.FindIndex(flatRows[0], v => v is string);
        int modified = Array.FindIndex(flatRows[0], v => v is DateTime);
        var modifiedByText = flatRows.ToDictionary(r => (string)r[text], r => (DateTime)r[modified], StringComparer.Ordinal);
        Assert.Equal(5, modifiedByText.Count);

        IReadOnlyList<MultiValueItem> row3 = ReadItems(rows["row3"][versionHistory]);
        Assert.Equal(["row3-memo", "row3-memo-revised", "row3-memo-again"], row3.Select(i => (string)i.Value!));
        Assert.All(row3, item =>
        {
            DateTime when = Assert.NotNull(item.Modified);
            Assert.Equal(new DateTime(2011, 9, 12), when.Date);
            Assert.Equal(modifiedByText[(string)item.Value!], when);
        });

        Assert.Equal(modifiedByText["row2-memo"], Assert.Single(ReadItems(rows["row2"][versionHistory])).Modified);
        Assert.Equal(modifiedByText["row4-memo"], Assert.Single(ReadItems(rows["row4"][versionHistory])).Modified);

        // Multi-value cells carry no timestamp.
        Assert.All(ReadItems(rows["row3"][4]), item => Assert.Null(item.Modified));

        static IReadOnlyList<MultiValueItem> ReadItems(object cell) => ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(cell));
    }

    [Fact]
    public void ComplexCellValue_VersionHistoryCell_RoundTrips()
    {
        MultiValueItem[] items =
        [
            new() { ConceptualTableId = 3, Value = "first", Modified = new DateTime(2011, 9, 12, 21, 21, 19) },
            new() { ConceptualTableId = 3, Value = "second", Modified = null },
            new() { ConceptualTableId = 3, Value = null, Modified = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
        ];

        byte[] cell = ComplexCellValue.EncodeVersionHistoryItems(3, items);

        Assert.Equal((byte)'V', cell[3]);
        Assert.Equal(items, ComplexCellValue.ReadMultiValueItems(cell));
        Assert.Equal(DateTimeKind.Utc, ComplexCellValue.ReadMultiValueItems(cell)[2].Modified!.Value.Kind);
    }

    [Fact]
    public async Task GetAttachmentsAsync_AccessAuthoredCompressedAttachment_IsInflated()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);

        IReadOnlyList<AttachmentRecord> all = await reader.GetAttachmentsAsync("Documents", "Attachments", TestContext.Current.CancellationToken);
        object[] first = await reader.Rows("Documents", cancellationToken: TestContext.Current.CancellationToken).FirstAsync(TestContext.Current.CancellationToken);

        AttachmentRecord hello = Assert.Single(ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(first[2])));
        Assert.Equal("fx_hello.txt", hello.FileName);
        Assert.Equal("Hello from attachment fixture!", Encoding.UTF8.GetString(hello.FileData));
        AssertSameAttachments([Assert.Single(all, a => a.FileName == "fx_hello.txt")], [hello]);
    }

    [Fact]
    public async Task Rows_AccessAuthoredLvalJpegAttachments_MatchGetAttachmentsAsync()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        IReadOnlyList<AttachmentRecord> all = await reader.GetAttachmentsAsync("ProductCategories", "ProductCategoryImage", TestContext.Current.CancellationToken);
        List<object[]> rows = await reader.Rows("ProductCategories", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);

        Assert.Equal(16, rows.Count);
        foreach (object[] row in rows)
        {
            AttachmentRecord image = Assert.Single(ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(row[4])));
            Assert.Equal("jpg", image.FileType);
            Assert.Equal([0xFF, 0xD8, 0xFF], image.FileData.Take(3));
            AssertSameAttachments([Assert.Single(all, a => a.ConceptualTableId == image.ConceptualTableId)], [image]);
        }
    }

    [Fact]
    public void ComplexCellValue_WrongKind_ThrowsFormatException()
    {
        byte[] multiValueCell = ComplexCellValue.EncodeMultiValueItems(7, [new MultiValueItem { ConceptualTableId = 7, Value = 1 }]);
        byte[] attachmentCell = ComplexCellValue.EncodeAttachments(7, [new AttachmentRecord { ConceptualTableId = 7, FileName = "a.txt" }]);
        byte[] versionHistoryCell = ComplexCellValue.EncodeVersionHistoryItems(7, [new MultiValueItem { ConceptualTableId = 7, Value = "v", Modified = DateTime.UnixEpoch }]);

        Assert.Throws<FormatException>(() => ComplexCellValue.ReadAttachments(versionHistoryCell));
        Assert.Throws<FormatException>(() => ComplexCellValue.ReadAttachments(multiValueCell));
        Assert.Throws<FormatException>(() => ComplexCellValue.ReadMultiValueItems(attachmentCell));
        Assert.Throws<FormatException>(() => ComplexCellValue.ReadAttachments([1, 2, 3]));
        Assert.Throws<FormatException>(() => ComplexCellValue.ReadAttachments(attachmentCell.AsSpan(0, attachmentCell.Length - 1).ToArray()));
    }

    [Fact]
    public void ComplexCellValue_RoundTripsEveryValueKind()
    {
        var guid = Guid.NewGuid();
        var when = new DateTime(2024, 5, 6, 7, 8, 9, DateTimeKind.Utc);
        object?[] values = [null, true, (byte)7, (short)-3, 42, 1L << 40, 1.5f, 2.25d, 12.345m, guid, when, "text", new byte[] { 1, 2, 3 }];
        MultiValueItem[] items = values.Select(v => new MultiValueItem { ConceptualTableId = 9, Value = v }).ToArray();

        IReadOnlyList<MultiValueItem> decoded = ComplexCellValue.ReadMultiValueItems(ComplexCellValue.EncodeMultiValueItems(9, items));

        Assert.Equal(values.Length, decoded.Count);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(9, decoded[i].ConceptualTableId);
            Assert.Equal(values[i], decoded[i].Value);
            Assert.Equal(values[i]?.GetType(), decoded[i].Value?.GetType());
        }

        var attachment = new AttachmentRecord
        {
            ConceptualTableId = 9,
            FileName = "naïve.txt",
            FileType = "txt",
            FileURL = "https://example.invalid/a",
            FileTimeStamp = when,
            FileData = [9, 8, 7],
        };
        AttachmentRecord roundTripped = Assert.Single(ComplexCellValue.ReadAttachments(ComplexCellValue.EncodeAttachments(9, [attachment])));
        AssertSameAttachments([attachment], [roundTripped]);
    }

    private static void AssertSameAttachments(List<AttachmentRecord> expected, IReadOnlyList<AttachmentRecord> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].ConceptualTableId, actual[i].ConceptualTableId);
            Assert.Equal(expected[i].FileName, actual[i].FileName);
            Assert.Equal(expected[i].FileType, actual[i].FileType);
            Assert.Equal(expected[i].FileURL, actual[i].FileURL);
            Assert.Equal(expected[i].FileTimeStamp, actual[i].FileTimeStamp);
            Assert.Equal(expected[i].FileData, actual[i].FileData);
        }
    }

    private static async Task<Dictionary<int, object>> ReadCellsAsync(AccessReader reader, string table)
    {
        List<object[]> rows = await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        return rows.ToDictionary(r => (int)r[0], r => r[1]);
    }

    private static async Task<Dictionary<string, object[]>> ReadFixtureRowsAsync(AccessReader reader)
    {
        List<object[]> rows = await reader.Rows("Table1", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        return rows.ToDictionary(r => (string)r[0]);
    }

    private static ValueTask<AccessReader> OpenAsync(MemoryStream ms)
        => AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

    private static async Task<MemoryStream> CreateFixtureAsync()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Docs",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value], [2, DBNull.Value], [3, DBNull.Value]], TestContext.Current.CancellationToken);
            await AddAttachmentAsync(writer, 1, "first.txt", FirstContent);
            await AddAttachmentAsync(writer, 1, "photo.jpg", PhotoContent);
            await AddAttachmentAsync(writer, 3, "third.txt", ThirdContent);
            await AddAttachmentAsync(writer, 1, "second.txt", SecondContent);

            await writer.CreateTableAsync(
                "Tags",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Labels", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) }],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync("Tags", [[1, DBNull.Value], [2, DBNull.Value], [3, DBNull.Value]], TestContext.Current.CancellationToken);
            await AddTagAsync(writer, 1, 10);
            await AddTagAsync(writer, 3, 40);
            await AddTagAsync(writer, 1, 20);
            await AddTagAsync(writer, 1, 30);

            await writer.CreateTableAsync(
                "Products",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Colors", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Products", [1, DBNull.Value], TestContext.Current.CancellationToken);
            foreach (string color in new[] { "red", string.Empty, "blue" })
            {
                await writer.AddMultiValueItemAsync("Products", "Colors", new Dictionary<string, object?> { ["Id"] = 1 }, color, TestContext.Current.CancellationToken);
            }
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask AddAttachmentAsync(AccessWriter writer, int id, string fileName, byte[] data)
        => writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = id }, new AttachmentInput(fileName, data), TestContext.Current.CancellationToken);

    private static ValueTask AddTagAsync(AccessWriter writer, int id, int value)
        => writer.AddMultiValueItemAsync("Tags", "Labels", new Dictionary<string, object?> { ["Id"] = id }, value, TestContext.Current.CancellationToken);

    private sealed class DocRow
    {
        public int Id { get; set; }

        public byte[]? Files { get; set; }
    }
}
