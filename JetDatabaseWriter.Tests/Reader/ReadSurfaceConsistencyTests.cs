namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
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
/// Checks that the reader's table-read APIs (<c>Rows</c>, <c>Rows&lt;T&gt;</c>,
/// <c>RowsAsStrings</c>, <c>ReadTableAsync</c>, <c>ReadTableAsync&lt;T&gt;</c>,
/// <c>ReadTableAsStringsAsync</c>, <c>ReadFirstTableAsStringsAsync</c> and
/// <c>GetRealRowCountAsync</c>) agree on which rows a table holds.
/// </summary>
public sealed class ReadSurfaceConsistencyTests
{
    private const string TableName = "Items";
    private const int ItemCount = 300;

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    public static TheoryData<DatabaseFormat> Formats => new()
    {
        DatabaseFormat.Jet3Mdb,
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.AceAccdb,
    };

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

    private static async ValueTask<AccessReader> OpenReaderAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
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
    private static async ValueTask CorruptAsync(byte[] bytes, Action<DatabaseFile, byte[], int> corrupt, CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream(bytes, writable: true);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(TableName, cancellationToken);
        Assert.NotNull(entry);

        IReadOnlyList<long> pages = await harness.Database.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken);
        Assert.True(pages.Count >= 2, $"Expected the table to span several data pages; it has {pages.Count}.");

        int pageStart = checked((int)(pages[1] * harness.Database.PageSizeBytes));
        corrupt(harness.Database, bytes, pageStart);
    }

    private static void CorruptFirstRowColumnCount(DatabaseFile database, byte[] bytes, int pageStart)
    {
        byte[] page = bytes.AsSpan(pageStart, database.PageSizeBytes).ToArray();
        RowBound row = database.EnumerateLiveRowBounds(page).First();
        bytes.AsSpan(pageStart + row.RowStart, database.RowFields.NumCols).Clear();
    }

    private static void CorruptRowCountPastCapacity(DatabaseFile database, byte[] bytes, int pageStart)
    {
        int capacity = (database.PageSizeBytes - database.DataPage.RowsStart) / 2;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageStart + database.DataPage.NumRows, 2), checked((ushort)(capacity + 100)));
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
}
