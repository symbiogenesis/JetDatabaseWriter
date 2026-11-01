namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Complex cells resolve only the parent rows actually consumed.</summary>
public sealed class ComplexColumnStreamingTests
{
    /// <summary>A first-row read does not decompress an unrelated parent's attachments.</summary>
    /// <param name="surface">The public read surface.</param>
    [Theory]
    [InlineData("rows")]
    [InlineData("strings")]
    [InlineData("table")]
    [InlineData("string-table")]
    [InlineData("poco")]
    [InlineData("indexed-poco")]
    public async Task FirstRow_DoesNotConsumeUnrelatedAttachmentBudget(string surface)
    {
        await using MemoryStream stream = await CreateFixtureAsync(8192);
        await using AccessReader reader = await OpenReaderAsync(stream, 128);

        byte[] cell = await ReadFirstCellAsync(reader, surface);

        AttachmentRecord item = Assert.Single(ComplexCellValue.ReadAttachments(cell));
        Assert.Equal("first.txt", item.FileName);
        Assert.Equal(Enumerable.Repeat((byte)1, 32), item.FileData);
    }

    /// <summary>The budget remains cumulative when enumeration reaches another parent.</summary>
    [Fact]
    public async Task Rows_AttachmentBudgetRemainsCumulativeAcrossParents()
    {
        await using MemoryStream stream = await CreateFixtureAsync(32);
        await using AccessReader reader = await OpenReaderAsync(stream, 80);
        await using IAsyncEnumerator<object[]> enumerator = reader.Rows("Docs", cancellationToken: TestContext.Current.CancellationToken).GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal(1, enumerator.Current[0]);
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await enumerator.MoveNextAsync());

        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
    }

    /// <summary>An unrelated external payload is not decoded before the selected parent's cell.</summary>
    [Fact]
    public async Task FirstRow_DoesNotReadUnrelatedLongValue()
    {
        await using MemoryStream stream = await CreateFixtureAsync(8192);
        await using AccessReader reader = await OpenReaderAsync(stream, 32768, maxLongValueBytes: 128);

        object[] first = await reader.Rows("Docs", cancellationToken: TestContext.Current.CancellationToken).FirstAsync(TestContext.Current.CancellationToken);

        Assert.Equal("first.txt", Assert.Single(ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(first[1]))).FileName);
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.GetAttachmentsAsync("Docs", "Files", TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
    }

    /// <summary>A stale parent index never authorizes another parent's attachment payload.</summary>
    /// <param name="strict">The scalar decoding policy.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StaleReferenceIndex_RefusesBeforePayloadAdmission(bool strict)
    {
        await using MemoryStream stream = await CreateFixtureAsync(8192);
        ComplexColumnInfo descriptor;
        await using (AccessReader inspection = await OpenReaderAsync(stream, 32768))
        {
            descriptor = Assert.Single(await inspection.GetComplexColumnsAsync("Docs", TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using (AccessWriter harness = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            long flatPage = CatalogValueReader.TdefPageFromId(descriptor.FlatTableId);
            TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(flatPage, descriptor.FlatTableName, ct);
            List<LocatedRow> items = await harness.Services.Snapshots.ReadRowsAsync(flatPage, ct);
            int referenceOrdinal = definition.FindColumnIndex("_Files");
            Assert.True(referenceOrdinal >= 0);
            LocatedRow first = items[0];
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(first.Location.DataPageNumber, ct);
            Assert.True(RowDecodePlan.TryParseRowLayout(harness.Database.Format.RowFields, page, first.Location.RowStart, first.Location.RowSize, definition.HasVarColumns, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(harness.Database.Format.RowFields, page, first.Location.RowStart, first.Location.RowSize, layout, definition.Columns[referenceOrdinal]);
            Assert.Equal(4, slice.DataLen);
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(first.Location.RowStart + slice.DataStart, 4), Assert.IsType<int>(items[1].Values[referenceOrdinal]));
            await harness.Pager.WritePageAsync(first.Location.DataPageNumber, page, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, MaxAttachmentContentBytesPerOperation = 1 }, leaveOpen: true, TestContext.Current.CancellationToken);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows("Docs", cancellationToken: TestContext.Current.CancellationToken).FirstAsync(TestContext.Current.CancellationToken));

        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.NotNull(error.ErrorInfo.IndexName);
    }

    /// <summary>Cancellation between parents prevents further complex-value decoding.</summary>
    [Fact]
    public async Task Rows_CancellationBetweenParentsStopsResolution()
    {
        await using MemoryStream stream = await CreateFixtureAsync(8192);
        await using AccessReader reader = await OpenReaderAsync(stream, 128);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using IAsyncEnumerator<object[]> enumerator = reader.Rows("Docs", cancellationToken: cancellation.Token).GetAsyncEnumerator(cancellation.Token);

        Assert.True(await enumerator.MoveNextAsync());
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await enumerator.MoveNextAsync());
    }

    private static async ValueTask<byte[]> ReadFirstCellAsync(AccessReader reader, string surface)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        switch (surface)
        {
            case "rows":
                return Assert.IsType<byte[]>((await reader.Rows("Docs", cancellationToken: ct).FirstAsync(ct))[1]);
            case "strings":
                return DecodeCell((await reader.RowsAsStrings("Docs", cancellationToken: ct).FirstAsync(ct))[1]);
            case "table":
                using (DataTable table = await reader.ReadTableAsync("Docs", maxRows: 1, cancellationToken: ct))
                {
                    return Assert.IsType<byte[]>(Assert.Single(table.Rows.Cast<DataRow>())[1]);
                }

            case "string-table":
                using (DataTable table = await reader.ReadTableAsStringsAsync("Docs", maxRows: 1, cancellationToken: ct))
                {
                    return DecodeCell(Assert.IsType<string>(Assert.Single(table.Rows.Cast<DataRow>())[1]));
                }

            case "poco":
                return Assert.IsType<byte[]>((await reader.Rows<DocRow>("Docs", cancellationToken: ct).FirstAsync(ct)).Files);
            case "indexed-poco":
                return Assert.IsType<byte[]>((await reader.Rows<DocRow>("Docs", row => row.Id == 1, cancellationToken: ct).FirstAsync(ct)).Files);
            default:
                throw new ArgumentOutOfRangeException(nameof(surface));
        }
    }

    private static byte[] DecodeCell(string value)
    {
        const string prefix = "data:application/octet-stream;base64,";
        Assert.StartsWith(prefix, value, StringComparison.Ordinal);
        return Convert.FromBase64String(value[prefix.Length..]);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, long budget, int maxLongValueBytes = 16 * 1024 * 1024)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, MaxAttachmentContentBytesPerOperation = budget, MaxLongValueBytes = maxLongValueBytes }, leaveOpen: true, TestContext.Current.CancellationToken);
    }

    private static async Task<MemoryStream> CreateFixtureAsync(int secondPayloadLength)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ct);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value], [2, DBNull.Value]], ct);
            await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("first.txt", [.. Enumerable.Repeat((byte)1, 32)]), ct);
            await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 2 }, new AttachmentInput("second.jpg", [.. Enumerable.Repeat((byte)2, secondPayloadLength)]), ct);
        }

        stream.Position = 0;
        return stream;
    }

    private sealed class DocRow
    {
        public int Id { get; set; }

        public byte[]? Files { get; set; }
    }
}
