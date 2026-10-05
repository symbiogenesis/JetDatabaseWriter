namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Checks that the reader's table-read APIs (<c>Rows</c>, <c>Rows&lt;T&gt;</c>,
/// <c>RowsAsStrings</c>, <c>ReadTableAsync</c>, <c>ReadTableAsync&lt;T&gt;</c>,
/// <c>ReadTableAsStringsAsync</c>, <c>ReadFirstTableAsStringsAsync</c> and
/// <c>GetRealRowCountAsync</c>) agree on which rows a table holds.
/// </summary>
public sealed class ReadSurfaceConsistencyTests
{
    private const string TableName = "Items";
    private const int ItemCount = 300;

    /// <summary>The rows in the Docs table <see cref="CreateDocsDatabaseAsync"/> builds.</summary>
    private const int DocCount = 40;

    /// <summary>The size of each Docs row's one attachment.</summary>
    private const int DocAttachmentBytes = 64 * 1024;

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    /// <summary>Gets the Access-authored tables with complex columns: the fixture and the table name.</summary>
    public static TheoryData<string, string> AccessAuthoredComplexTables => new()
    {
        { TestDatabases.ComplexFields, "Documents" },
        { TestDatabases.ComplexDataTestV2007, "Table1" },
        { TestDatabases.ComplexDataTestV2010, "Table1" },
    };

    public static TheoryData<DatabaseFormat> Formats =>
    [
        DatabaseFormat.Jet3Mdb,
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.AceAccdb,
    ];

    public static TheoryData<DatabaseFormat, PageReadOptimizationMode> FormatsAndReadModes => new()
    {
        { DatabaseFormat.Jet3Mdb, PageReadOptimizationMode.Disabled },
        { DatabaseFormat.Jet3Mdb, PageReadOptimizationMode.Enabled },
        { DatabaseFormat.Jet4Mdb, PageReadOptimizationMode.Disabled },
        { DatabaseFormat.Jet4Mdb, PageReadOptimizationMode.Enabled },
        { DatabaseFormat.AceAccdb, PageReadOptimizationMode.Disabled },
        { DatabaseFormat.AceAccdb, PageReadOptimizationMode.Enabled },
    };

    // ── ReadTableAsync<T> with complex columns ────────────────────────

    [Fact]
    public async Task ReadTableGeneric_BoundAttachmentColumn_ReturnsSameRowsAsRowsGeneric()
    {
        await using MemoryStream ms = await this.CreateAttachmentDatabaseAsync();
        await using AccessReader reader = await OpenReaderAsync(ms, this.ct);

        IReadOnlyList<DocumentRow> read = await reader.ReadTableAsync<DocumentRow>("Documents", cancellationToken: this.ct);
        List<DocumentRow> streamed = await CollectAsync(reader.Rows<DocumentRow>("Documents", cancellationToken: this.ct));

        Assert.Equal([1, 2, 3], read.Select(row => row.Id));
        Assert.Equal(streamed.Select(row => row.Id), read.Select(row => row.Id));
        Assert.Equal(streamed.Select(row => row.Title), read.Select(row => row.Title));
        Assert.Equal(streamed.Select(row => row.Files), read.Select(row => row.Files));
        Assert.NotNull(read[0].Files);
    }

    [Fact]
    public async Task ReadTableGeneric_BoundAttachmentColumnWithUnboundColumns_ReturnsSameRowsAsRowsGeneric()
    {
        await using MemoryStream ms = await this.CreateAttachmentDatabaseAsync();
        await using AccessReader reader = await OpenReaderAsync(ms, this.ct);

        IReadOnlyList<DocumentFilesRow> read = await reader.ReadTableAsync<DocumentFilesRow>("Documents", cancellationToken: this.ct);
        List<DocumentFilesRow> streamed = await CollectAsync(reader.Rows<DocumentFilesRow>("Documents", cancellationToken: this.ct));

        Assert.Equal([1, 2, 3], read.Select(row => row.Id));
        Assert.Equal(streamed.Select(row => row.Files), read.Select(row => row.Files));
    }

    [Theory]
    [InlineData(1u)]
    [InlineData(2u)]
    public async Task ReadTableGeneric_BoundAttachmentColumn_HonoursMaxRows(uint maxRows)
    {
        await using MemoryStream ms = await this.CreateAttachmentDatabaseAsync();
        await using AccessReader reader = await OpenReaderAsync(ms, this.ct);

        IReadOnlyList<DocumentRow> read = await reader.ReadTableAsync<DocumentRow>("Documents", maxRows, this.ct);

        Assert.Equal(Enumerable.Range(1, (int)maxRows), read.Select(row => row.Id));
    }

    [Fact]
    public async Task ReadTableGeneric_BoundMultiValueColumn_ReturnsSameRowsAsRowsGeneric()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            this.ct))
        {
            await writer.CreateTableAsync(
                "Tags",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Labels", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
                ],
                this.ct);
            await writer.InsertRowsAsync("Tags", [[1, DBNull.Value], [2, DBNull.Value]], this.ct);
            await writer.AddMultiValueItemAsync("Tags", "Labels", new Dictionary<string, object?> { ["Id"] = 1 }, 10, this.ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, this.ct);

        IReadOnlyList<TagRow> read = await reader.ReadTableAsync<TagRow>("Tags", cancellationToken: this.ct);
        List<TagRow> streamed = await CollectAsync(reader.Rows<TagRow>("Tags", cancellationToken: this.ct));

        Assert.Equal([1, 2], read.Select(row => row.Id));
        Assert.Equal(streamed.Select(row => row.Labels), read.Select(row => row.Labels));
    }

    // ── Mapped reads skip the complex columns T does not bind ────────

    [Fact]
    public async Task ReadTableGeneric_UnboundComplexColumns_DoesNotLoadComplexData()
    {
        byte[] bytes = await this.CreateDocsDatabaseAsync();
        List<object[]> expected = await this.ReadDocsRowsAsync(bytes);
        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await OpenReaderAsync(counting, this.ct);
        counting.Reset();

        IReadOnlyList<DocSummaryRow> read = await reader.ReadTableAsync<DocSummaryRow>("Docs", cancellationToken: this.ct);

        AssertSummaries(expected, read);
        AssertSkippedAttachments(counting, "ReadTableAsync<T>");
    }

    [Fact]
    public async Task RowsGeneric_UnboundComplexColumns_DoesNotLoadComplexData()
    {
        byte[] bytes = await this.CreateDocsDatabaseAsync();
        List<object[]> expected = await this.ReadDocsRowsAsync(bytes);
        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await OpenReaderAsync(counting, this.ct);
        counting.Reset();

        List<DocSummaryRow> read = await CollectAsync(reader.Rows<DocSummaryRow>("Docs", cancellationToken: this.ct));

        AssertSummaries(expected, read);
        AssertSkippedAttachments(counting, "Rows<T>");
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(5u)]
    public async Task ReadTableGeneric_UnboundComplexColumns_HonoursMaxRows(uint maxRows)
    {
        byte[] bytes = await this.CreateDocsDatabaseAsync();
        List<object[]> expected = await this.ReadDocsRowsAsync(bytes);
        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await OpenReaderAsync(counting, this.ct);
        counting.Reset();

        IReadOnlyList<DocSummaryRow> read = await reader.ReadTableAsync<DocSummaryRow>("Docs", maxRows, this.ct);

        AssertSummaries(expected.Take((int)maxRows).ToList(), read);
        AssertSkippedAttachments(counting, $"ReadTableAsync<T> with maxRows {maxRows}");
    }

    [Fact]
    public async Task ReadTableGeneric_BindsMultiValueOnly_LoadsOnlyThatColumn()
    {
        byte[] bytes = await this.CreateDocsDatabaseAsync();
        List<object[]> expected = await this.ReadDocsRowsAsync(bytes);
        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await OpenReaderAsync(counting, this.ct);
        counting.Reset();

        IReadOnlyList<DocTagsRow> read = await reader.ReadTableAsync<DocTagsRow>("Docs", cancellationToken: this.ct);

        Assert.Equal(expected.Select(row => row[0]), read.Select(row => (object)row.Id));
        Assert.Equal(expected.Select(row => row[4]), read.Select(row => row.Tags));
        Assert.All(read, row => Assert.IsType<byte[]>(row.Tags));
        AssertSkippedAttachments(counting, "ReadTableAsync<T> binding the multi-value column");
    }

    [Fact]
    public async Task RowsWithIndexedPredicate_UnboundComplexColumns_DoesNotLoadComplexData()
    {
        byte[] bytes = await this.CreateDocsDatabaseAsync();
        List<object[]> expected = await this.ReadDocsRowsAsync(bytes);
        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await OpenReaderAsync(counting, this.ct);
        counting.Reset();

        // Id is the primary key, so the predicate is read through the index.
        List<DocSummaryRow> read = await CollectAsync(reader.Rows<DocSummaryRow>("Docs", row => row.Id == 7, cancellationToken: this.ct));

        AssertSummaries([expected.Single(row => (int)row[0] == 7)], read);
        AssertSkippedAttachments(counting, "Rows<T>(predicate)");
    }

    [Theory]
    [MemberData(nameof(AccessAuthoredComplexTables))]
    public async Task ReadTableGeneric_AccessAuthoredComplexTables_UnboundColumnsMatchRows(string fixture, string table)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(fixture, new AccessReaderOptions { UseLockFile = false }, this.ct);
        List<object[]> rows = await CollectAsync(reader.Rows(table, cancellationToken: this.ct));
        Assert.NotEmpty(rows);

        if (table == "Documents")
        {
            IReadOnlyList<ComplexFieldsDocumentRow> read = await reader.ReadTableAsync<ComplexFieldsDocumentRow>(table, cancellationToken: this.ct);
            List<ComplexFieldsDocumentRow> streamed = await CollectAsync(reader.Rows<ComplexFieldsDocumentRow>(table, cancellationToken: this.ct));
            Assert.Equal(rows.Select(row => row[0]), read.Select(row => (object)row.Id));
            Assert.Equal(rows.Select(row => row[1] as string), read.Select(row => row.Title));
            Assert.Equal(read.Select(row => (row.Id, row.Title)), streamed.Select(row => (row.Id, row.Title)));
        }
        else
        {
            IReadOnlyList<ComplexDataTestRow> read = await reader.ReadTableAsync<ComplexDataTestRow>(table, cancellationToken: this.ct);
            List<ComplexDataTestRow> streamed = await CollectAsync(reader.Rows<ComplexDataTestRow>(table, cancellationToken: this.ct));
            Assert.Equal(rows.Select(row => row[0] as string), read.Select(row => row.Id));
            Assert.Equal(rows.Select(row => row[2] as string), read.Select(row => row.Memo));
            Assert.Equal(read.Select(row => (row.Id, row.Memo)), streamed.Select(row => (row.Id, row.Memo)));
        }
    }

    // ── Drift between the read loops ──────────────────────────────────

    [Theory]
    [MemberData(nameof(FormatsAndReadModes))]
    public async Task ReadApis_RowWithZeroColumnCount_AllSkipItAndAgreeOnCount(DatabaseFormat format, PageReadOptimizationMode readMode)
    {
        byte[] bytes = await this.CreateItemsDatabaseAsync(format);
        await CorruptAsync(bytes, CorruptFirstRowColumnCount, this.ct);

        await using var ms = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false, PageReadOptimizationMode = readMode },
            leaveOpen: true,
            this.ct);

        Dictionary<string, long> counts = await this.CountThroughEveryApiAsync(reader);

        Assert.All(counts, pair => Assert.True(pair.Value == ItemCount - 1, $"{pair.Key} returned {pair.Value} rows; expected {ItemCount - 1}."));
    }

    [Theory]
    [MemberData(nameof(FormatsAndReadModes))]
    public async Task ReadApis_OverflowRow_AllFollowItAndAgreeOnCount(DatabaseFormat format, PageReadOptimizationMode readMode)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(format, TableName, ItemCount, primaryKey: false, this.ct);
        long expected;
        await using (var original = new MemoryStream(bytes, writable: false))
        await using (AccessReader reader = await OpenReaderAsync(original, this.ct))
        {
            expected = await reader.GetRealRowCountAsync(TableName, this.ct);
        }

        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, TableName, OverflowRowLayout.CrossPage, this.ct);

        await using var ms = new MemoryStream(bytes, writable: false);
        await using AccessReader overflowReader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false, PageReadOptimizationMode = readMode },
            leaveOpen: true,
            this.ct);

        Dictionary<string, long> counts = await this.CountThroughEveryApiAsync(overflowReader);

        Assert.True(expected >= ItemCount, $"The table holds {expected} rows; expected at least {ItemCount}.");
        Assert.All(counts, pair => Assert.True(pair.Value == expected, $"{pair.Key} returned {pair.Value} rows; expected {expected}."));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task ReadApis_DataPageRowCountPastPageCapacity_AgreeOnCount(DatabaseFormat format)
    {
        byte[] bytes = await this.CreateItemsDatabaseAsync(format);
        await CorruptAsync(bytes, CorruptRowCountPastCapacity, this.ct);

        await using var ms = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await OpenReaderAsync(ms, this.ct);

        long realCount = await reader.GetRealRowCountAsync(TableName, this.ct);
        List<object[]> rows = await CollectAsync(reader.Rows(TableName, cancellationToken: this.ct));
        List<string[]> stringRows = await CollectAsync(reader.RowsAsStrings(TableName, cancellationToken: this.ct));

        Assert.Equal(rows.Count, realCount);
        Assert.Equal(rows.Count, stringRows.Count);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public async Task ReadApis_MaxRowsZero_ReturnNoRows(DatabaseFormat format)
    {
        byte[] bytes = await this.CreateItemsDatabaseAsync(format);
        await using var ms = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await OpenReaderAsync(ms, this.ct);

        IReadOnlyList<ItemRow> mapped = await reader.ReadTableAsync<ItemRow>(TableName, 0, this.ct);
        IReadOnlyList<IdOnlyRow> projected = await reader.ReadTableAsync<IdOnlyRow>(TableName, 0, this.ct);
        using DataTable typed = await reader.ReadTableAsync(TableName, 0, cancellationToken: this.ct);
        using DataTable strings = await reader.ReadTableAsStringsAsync(TableName, 0, cancellationToken: this.ct);
        using DataTable first = await reader.ReadFirstTableAsStringsAsync(0, this.ct);

        Assert.Empty(mapped);
        Assert.Empty(projected);
        Assert.Equal(0, typed.Rows.Count);
        Assert.Equal(0, strings.Rows.Count);
        Assert.Equal(0, first.Rows.Count);
        Assert.Equal(2, typed.Columns.Count);
        Assert.Equal(2, strings.Columns.Count);
    }

    [Fact]
    public async Task ReadFirstTableAsStrings_CalculatedColumns_MatchesReadTableAsStrings()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            TestDatabases.CalcFieldTestV2010,
            new AccessReaderOptions { UseLockFile = false },
            this.ct);
        string firstTable = (await reader.ListTablesAsync(this.ct))[0];

        using DataTable first = await reader.ReadFirstTableAsStringsAsync(cancellationToken: this.ct);
        using DataTable named = await reader.ReadTableAsStringsAsync(firstTable, cancellationToken: this.ct);

        Assert.Equal(named.Rows.Count, first.Rows.Count);
        for (int r = 0; r < named.Rows.Count; r++)
        {
            Assert.Equal(named.Rows[r].ItemArray, first.Rows[r].ItemArray);
        }
    }

    private static async ValueTask<AccessReader> OpenReaderAsync(Stream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    /// <summary>
    /// Checks the <c>Id</c>, <c>Name</c> and <c>Size</c> of each mapped row
    /// against the matching <c>Rows()</c> row of the Docs table.
    /// </summary>
    /// <param name="expected">The <c>Rows()</c> rows, in table order.</param>
    /// <param name="actual">The mapped rows.</param>
    private static void AssertSummaries(IReadOnlyList<object[]> expected, IReadOnlyList<DocSummaryRow> actual)
    {
        Assert.Equal(expected.Select(row => row[0]), actual.Select(row => (object)row.Id));
        Assert.Equal(expected.Select(row => row[1]), actual.Select(row => (object?)row.Name));
        Assert.Equal(expected.Select(row => row[2]), actual.Select(row => (object)row.Size));
    }

    /// <summary>
    /// Checks that a read of the Docs table left the attachment data alone:
    /// it read less than a quarter of the bytes the attachments take, where
    /// loading them reads the attachment flat table and every LVAL chain.
    /// </summary>
    /// <param name="counting">The stream the reader read through, reset before the read.</param>
    /// <param name="read">The read, for the failure message.</param>
    private static void AssertSkippedAttachments(CountingStream counting, string read)
    {
        const long attachmentBytes = (long)DocCount * DocAttachmentBytes;
        Assert.True(
            counting.BytesRead < attachmentBytes / 4,
            $"{read} read {counting.BytesRead} bytes; the attachments it does not bind take {attachmentBytes}.");
    }

    private static async ValueTask<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var items = new List<T>();
        await foreach (T item in source)
        {
            items.Add(item);
        }

        return items;
    }

    /// <summary>
    /// Opens <paramref name="bytes"/> through the internal read layers, locates the
    /// second data page owned by <see cref="TableName"/>, and lets
    /// <paramref name="corrupt"/> rewrite that page in place.
    /// </summary>
    /// <param name="bytes">The database image to corrupt in place.</param>
    /// <param name="corrupt">Rewrites the page; receives the open file, the whole image and the page's byte offset.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    private static async ValueTask CorruptAsync(byte[] bytes, Action<DatabaseFile, byte[], int> corrupt, CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream(bytes, writable: true);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(TableName, cancellationToken);
        Assert.NotNull(entry);

        IReadOnlyList<long> pages = await harness.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken);
        Assert.True(pages.Count >= 2, $"Expected the table to span several data pages; it has {pages.Count}.");

        int pageStart = checked((int)(pages[1] * harness.Database.Format.PageSize));
        corrupt(harness.Database, bytes, pageStart);
    }

    private static void CorruptFirstRowColumnCount(DatabaseFile database, byte[] bytes, int pageStart)
    {
        byte[] page = bytes.AsSpan(pageStart, database.Format.PageSize).ToArray();
        RowBound row = DataPageRows.EnumerateLiveRowBounds(database.Format, page).First();
        bytes.AsSpan(pageStart + row.RowStart, database.Format.RowFields.NumCols).Clear();
    }

    private static void CorruptRowCountPastCapacity(DatabaseFile database, byte[] bytes, int pageStart)
    {
        int capacity = (database.Format.PageSize - database.Format.DataPage.RowsStart) / 2;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageStart + database.Format.DataPage.NumRows, 2), checked((ushort)(capacity + 100)));
    }

    private async ValueTask<byte[]> CreateItemsDatabaseAsync(DatabaseFormat format)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            this.ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Name", typeof(string), maxLength: 100)],
                this.ct);

            var rows = new List<object[]>(ItemCount);
            for (int i = 1; i <= ItemCount; i++)
            {
                rows.Add([i, $"Item {i:D4} " + new string('x', 40)]);
            }

            await writer.InsertRowsAsync(TableName, rows, this.ct);
        }

        return ms.ToArray();
    }

    private async ValueTask<MemoryStream> CreateAttachmentDatabaseAsync()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            this.ct))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Title", typeof(string), maxLength: 50),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                this.ct);
            await writer.InsertRowsAsync("Documents", [[1, "one", DBNull.Value], [2, "two", DBNull.Value], [3, "three", DBNull.Value]], this.ct);
            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object?> { ["Id"] = 1 },
                new AttachmentInput("first.txt", Encoding.UTF8.GetBytes("FIRST")),
                this.ct);
        }

        return ms;
    }

    /// <summary>
    /// Builds an ACCDB whose <c>Docs</c> table has <see cref="DocCount"/> rows,
    /// each with one incompressible <see cref="DocAttachmentBytes"/>-byte
    /// attachment in <c>Files</c> and two items in the multi-value <c>Tags</c>.
    /// </summary>
    /// <returns>The database image.</returns>
    private async ValueTask<byte[]> CreateDocsDatabaseAsync()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            this.ct))
        {
            await writer.CreateTableAsync(
                "Docs",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                    new ColumnDefinition("Size", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                    new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
                ],
                [new IndexDefinition("PK_Docs", "Id") { IsPrimaryKey = true }],
                this.ct);

            var rows = new List<object[]>(DocCount);
            for (int id = 1; id <= DocCount; id++)
            {
                rows.Add([DBNull.Value, $"Doc {id}", id * 10, DBNull.Value, DBNull.Value]);
            }

            await writer.InsertRowsAsync("Docs", rows, this.ct);

            for (int id = 1; id <= DocCount; id++)
            {
                byte[] content = new byte[DocAttachmentBytes];
                RandomNumberGenerator.Fill(content);
                var key = new Dictionary<string, object?> { ["Id"] = id };
                await writer.AddAttachmentAsync("Docs", "Files", key, new AttachmentInput($"doc{id}.jpg", content), this.ct);
                await writer.AddMultiValueItemAsync("Docs", "Tags", key, id, this.ct);
                await writer.AddMultiValueItemAsync("Docs", "Tags", key, 1000 + id, this.ct);
            }
        }

        return ms.ToArray();
    }

    /// <summary>Reads every row of the Docs table through <c>Rows()</c>, which resolves every complex column.</summary>
    /// <param name="bytes">The database image.</param>
    /// <returns>The rows, in table order.</returns>
    private async ValueTask<List<object[]>> ReadDocsRowsAsync(byte[] bytes)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await OpenReaderAsync(ms, this.ct);
        List<object[]> rows = await CollectAsync(reader.Rows("Docs", cancellationToken: this.ct));
        Assert.Equal(DocCount, rows.Count);
        Assert.All(rows, row => Assert.IsType<byte[]>(row[3]));
        Assert.All(rows, row => Assert.IsType<byte[]>(row[4]));
        return rows;
    }

    private async ValueTask<Dictionary<string, long>> CountThroughEveryApiAsync(AccessReader reader)
    {
        var counts = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            ["GetRealRowCountAsync"] = await reader.GetRealRowCountAsync(TableName, this.ct),
            ["Rows"] = (await CollectAsync(reader.Rows(TableName, cancellationToken: this.ct))).Count,
            ["Rows<T>"] = (await CollectAsync(reader.Rows<ItemRow>(TableName, cancellationToken: this.ct))).Count,
            ["Rows<T> (projected)"] = (await CollectAsync(reader.Rows<IdOnlyRow>(TableName, cancellationToken: this.ct))).Count,
            ["RowsAsStrings"] = (await CollectAsync(reader.RowsAsStrings(TableName, cancellationToken: this.ct))).Count,
            ["ReadTableAsync<T>"] = (await reader.ReadTableAsync<ItemRow>(TableName, cancellationToken: this.ct)).Count,
            ["ReadTableAsync<T> (projected)"] = (await reader.ReadTableAsync<IdOnlyRow>(TableName, cancellationToken: this.ct)).Count,
        };

        using (DataTable typed = await reader.ReadTableAsync(TableName, cancellationToken: this.ct))
        {
            counts["ReadTableAsync"] = typed.Rows.Count;
        }

        using (DataTable strings = await reader.ReadTableAsStringsAsync(TableName, cancellationToken: this.ct))
        {
            counts["ReadTableAsStringsAsync"] = strings.Rows.Count;
        }

        using (DataTable first = await reader.ReadFirstTableAsStringsAsync(cancellationToken: this.ct))
        {
            counts["ReadFirstTableAsStringsAsync"] = first.Rows.Count;
        }

        return counts;
    }

    private sealed class DocumentRow
    {
        public int Id { get; set; }

        public string? Title { get; set; }

        public object? Files { get; set; }
    }

    private sealed class DocumentFilesRow
    {
        public int Id { get; set; }

        public object? Files { get; set; }
    }

    private sealed class TagRow
    {
        public int Id { get; set; }

        public object? Labels { get; set; }
    }

    private sealed class ItemRow
    {
        public int Id { get; set; }

        public string? Name { get; set; }
    }

    private sealed class IdOnlyRow
    {
        public int Id { get; set; }
    }

    private sealed class DocSummaryRow
    {
        public int Id { get; set; }

        public string? Name { get; set; }

        public int Size { get; set; }
    }

    private sealed class DocTagsRow
    {
        public int Id { get; set; }

        public object? Tags { get; set; }
    }

    private sealed class ComplexFieldsDocumentRow
    {
        public int Id { get; set; }

        public string? Title { get; set; }
    }

    private sealed class ComplexDataTestRow
    {
        public string? Id { get; set; }

        [Column("memo-data")]
        public string? Memo { get; set; }
    }
}
