namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for the row-level complex-column APIs:
/// <see cref="IAccessWriter.AddAttachmentAsync"/>,
/// <see cref="IAccessWriter.AddMultiValueItemAsync"/>,
/// <see cref="IAccessReader.GetAttachmentsAsync"/>,
/// <see cref="IAccessReader.GetMultiValueItemsAsync"/>, and the
/// <see cref="AttachmentWrapper"/> encoder / decoder per
/// <c>docs/design/complex-columns-format-notes.md</c> §3.
/// </summary>
public sealed class ComplexColumnsRowApiTests
{
    [Fact]
    public void AttachmentWrapper_RoundTrips_Raw_For_Jpg()
    {
        // Compressed-format extensions (jpg, zip, ...) skip deflate per spec §3.2.
        byte[] payload = Encoding.UTF8.GetBytes("FAKE-JPEG-PAYLOAD");
        byte[] wrapped = AttachmentWrapper.Encode("jpg", payload);

        Assert.Equal((byte)0x00, wrapped[0]); // typeFlag = 0 (raw)

        bool ok = AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded);
        Assert.True(ok);
        Assert.Equal("jpg", ext);
        Assert.Equal(payload, decoded);
    }

    [Fact]
    public void AttachmentWrapper_RoundTrips_Deflated_For_Txt()
    {
        // Generic text extensions are deflate-compressed per spec §3.2.
        byte[] payload = Encoding.UTF8.GetBytes(new string('a', 256));
        byte[] wrapped = AttachmentWrapper.Encode("txt", payload);

        Assert.Equal((byte)0x01, wrapped[0]); // typeFlag = 1 (deflate)

        bool ok = AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded);
        Assert.True(ok);
        Assert.Equal("txt", ext);
        Assert.Equal(payload, decoded);
    }

    [Fact]
    public void AttachmentWrapper_TryDecode_RejectsUnknownTypeFlag()
    {
        byte[] junk = new byte[32];
        junk[0] = 0xFF;
        Assert.False(AttachmentWrapper.TryDecode(junk, out _, out _));
    }

    [Fact]
    public async Task AddAttachmentAsync_RoundTrips_ViaGetAttachmentsAsync()
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Documents",
                [1, DBNull.Value],
                TestContext.Current.CancellationToken);

            byte[] payload = Encoding.UTF8.GetBytes("hello attachments");
            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("notes.txt", payload),
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var attachments = await reader.GetAttachmentsAsync("Documents", "Files", TestContext.Current.CancellationToken);

        var single = Assert.Single(attachments);
        Assert.Equal("notes.txt", single.FileName);
        Assert.Equal("txt", single.FileType);
        Assert.True(single.ConceptualTableId > 0);
        Assert.Equal(Encoding.UTF8.GetBytes("hello attachments"), single.FileData);
    }

    [Fact]
    public async Task AddAttachmentAsync_TwoFilesSameRow_ShareConceptualTableId()
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Documents",
                [1, DBNull.Value],
                TestContext.Current.CancellationToken);

            var key = new Dictionary<string, object> { ["Id"] = 1 };
            await writer.AddAttachmentAsync("Documents", "Files", key, new AttachmentInput("a.txt", Encoding.UTF8.GetBytes("aaa")), TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync("Documents", "Files", key, new AttachmentInput("b.txt", Encoding.UTF8.GetBytes("bbb")), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var attachments = await reader.GetAttachmentsAsync("Documents", "Files", TestContext.Current.CancellationToken);

        Assert.Equal(2, attachments.Count);
        Assert.Equal(attachments[0].ConceptualTableId, attachments[1].ConceptualTableId);
        Assert.Contains(attachments, a => a.FileName == "a.txt");
        Assert.Contains(attachments, a => a.FileName == "b.txt");
    }

    [Fact]
    public async Task AddAttachmentAsync_TwoParentRows_GetDistinctConceptualTableIds()
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "Documents",
                [[1, DBNull.Value], [2, DBNull.Value]],
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync("Documents", "Files", new Dictionary<string, object> { ["Id"] = 1 }, new AttachmentInput("one.txt", Encoding.UTF8.GetBytes("one")), TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync("Documents", "Files", new Dictionary<string, object> { ["Id"] = 2 }, new AttachmentInput("two.txt", Encoding.UTF8.GetBytes("two")), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var attachments = await reader.GetAttachmentsAsync("Documents", "Files", TestContext.Current.CancellationToken);

        Assert.Equal(2, attachments.Count);
        Assert.NotEqual(attachments[0].ConceptualTableId, attachments[1].ConceptualTableId);
    }

    [Fact]
    public async Task AddAttachmentAsync_NoMatchingRow_Throws()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(
            "Documents",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
            ],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 99 },
                new AttachmentInput("x.txt", [1, 2, 3]),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AddAttachmentAsync_OnMultiValueColumn_Throws()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(
            "Tags",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Labels", typeof(object))
                {
                    IsMultiValue = true,
                    MultiValueElementType = typeof(int),
                },
            ],
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync("Tags", [1, DBNull.Value], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.AddAttachmentAsync(
                "Tags",
                "Labels",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("x.txt", [1]),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AddMultiValueItemAsync_RoundTrips_ViaGetMultiValueItems()
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Tags",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Labels", typeof(object))
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(int),
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("Tags", [1, DBNull.Value], TestContext.Current.CancellationToken);

            var key = new Dictionary<string, object> { ["Id"] = 1 };
            await writer.AddMultiValueItemAsync("Tags", "Labels", key, 100, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Tags", "Labels", key, 200, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Tags", "Labels", key, 300, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var items = await reader.GetMultiValueItemsAsync("Tags", "Labels", TestContext.Current.CancellationToken);

        Assert.Equal(3, items.Count);
        Assert.All(items, t => Assert.Equal(items[0].ConceptualTableId, t.ConceptualTableId));
        var sortedValues = items.Select(t => Convert.ToInt32(t.Value, System.Globalization.CultureInfo.InvariantCulture)).OrderBy(v => v).ToArray();
        Assert.Equal(100, sortedValues[0]);
        Assert.Equal(200, sortedValues[1]);
        Assert.Equal(300, sortedValues[2]);
    }

    [Fact]
    public async Task AddMultiValueItemAsync_OnAttachmentColumn_Throws()
    {
        var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(
            "Documents",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
            ],
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync("Documents", [1, DBNull.Value], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.AddMultiValueItemAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                42,
                TestContext.Current.CancellationToken));
    }
}
