namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Checks that the reader's table-read APIs (<c>Rows</c>, <c>Rows&lt;T&gt;</c>,
/// <c>RowsAsStrings</c>, <c>ReadTableAsync</c>, <c>ReadTableAsync&lt;T&gt;</c>,
/// <c>ReadTableAsStringsAsync</c>, <c>ReadFirstTableAsStringsAsync</c> and
/// <c>GetRealRowCountAsync</c>) agree on which rows a table holds.
/// </summary>
public sealed class ReadSurfaceConsistencyTests
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

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
}
