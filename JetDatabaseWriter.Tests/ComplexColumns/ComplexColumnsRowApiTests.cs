namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using Xunit;

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
    public void AttachmentWrapper_TryDecode_AcceptsKnownRawDeflateSample()
    {
        byte[] wrapped =
        [
            0x01, 0x00, 0x00, 0x00,
            0x24, 0x00, 0x00, 0x00,
            0x13, 0x61, 0x60, 0x60, 0x60, 0x04, 0x62, 0x0E,
            0x20, 0x2E, 0x61, 0xA8, 0x00, 0x62, 0x06, 0x86,
            0xA2, 0xC4, 0x72, 0x85, 0x94, 0xD4, 0xB4, 0x9C,
            0xC4, 0x92, 0x54, 0x85, 0x82, 0xC4, 0xCA, 0x9C,
            0xFC, 0xC4, 0x14, 0x00,
        ];

        byte[] rawBody = wrapped.AsSpan(8, BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(4, 4))).ToArray();
        Assert.Throws<InvalidDataException>(() => InflateWithZlib(rawBody));

        bool ok = AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded);

        Assert.True(ok);
        Assert.Equal("txt", ext);
        Assert.Equal(Encoding.UTF8.GetBytes("raw deflate payload"), decoded);
    }

    [Fact]
    public void AccessReader_DecompressAttachmentData_ZlibWrappedSample_InflatesFromHeader()
    {
        byte[] raw =
        [
            0x01, 0xA5, 0x5A,
            0x78, 0xDA, 0xAB, 0xCA, 0xC9, 0x4C, 0x52, 0x48,
            0x2C, 0x29, 0x49, 0x4C, 0xCE, 0xC8, 0x4D, 0xCD,
            0x2B, 0x51, 0x28, 0x48, 0xAC, 0xCC, 0xC9, 0x4F,
            0x4C, 0x01, 0x00, 0x6B, 0xCA, 0x09, 0x05,
        ];

        byte[] decoded = InvokeDecompressAttachmentData(raw, 1);

        Assert.Equal(Encoding.UTF8.GetBytes("zlib attachment payload"), decoded);
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
        await using var ms = new MemoryStream();

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
        await using var ms = new MemoryStream();

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
        await using var ms = new MemoryStream();

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
        await using var ms = new MemoryStream();
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
        await using var ms = new MemoryStream();
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
        await using var ms = new MemoryStream();

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
        await using var ms = new MemoryStream();
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

    /// <summary>
    /// §2.2 gap: attachment with a zero-byte payload. The wrapper must
    /// encode and decode cleanly even when <c>FileData.Length == 0</c>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddAttachmentAsync_ZeroBytePayload_RoundTrips()
    {
        await using var ms = new MemoryStream();

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

            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput("empty.dat", Array.Empty<byte>()),
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var attachments = await reader.GetAttachmentsAsync(
            "Documents",
            "Files",
            TestContext.Current.CancellationToken);

        var single = Assert.Single(attachments);
        Assert.Equal("empty.dat", single.FileName);
        Assert.NotNull(single.FileData);
        Assert.Empty(single.FileData);
    }

    /// <summary>
    /// §2.2 gap: multi-value text column with mixed value lengths. Covers
    /// the <c>T_TEXT</c> element path in the flat child table, including
    /// an empty-string value to exercise the zero-length variable-column
    /// entry.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddMultiValueItemAsync_TextWithMixedLengths_RoundTrips()
    {
        await using var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Products",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Tags", typeof(object))
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(string),
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Products",
                [1, DBNull.Value],
                TestContext.Current.CancellationToken);

            var key = new Dictionary<string, object> { ["Id"] = 1 };

            // Mixed lengths: empty string, short, medium, long.
            await writer.AddMultiValueItemAsync("Products", "Tags", key, string.Empty, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Products", "Tags", key, "a", TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Products", "Tags", key, "medium-tag", TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Products", "Tags", key, new string('x', 80), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var items = await reader.GetMultiValueItemsAsync(
            "Products",
            "Tags",
            TestContext.Current.CancellationToken);

        Assert.Equal(4, items.Count);

        var values = items
            .Select(i => i.Value?.ToString() ?? string.Empty)
            .OrderBy(v => v.Length)
            .ToArray();

        Assert.Equal(string.Empty, values[0]);
        Assert.Equal("a", values[1]);
        Assert.Equal("medium-tag", values[2]);
        Assert.Equal(new string('x', 80), values[3]);
    }

    private static byte[] InvokeDecompressAttachmentData(byte[] bytes, int offset)
    {
        MethodInfo? method = typeof(AccessReader).GetMethod("DecompressAttachmentData", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return (byte[])method!.Invoke(null, [bytes, offset])!;
    }

    private static byte[] InflateWithZlib(byte[] bytes)
    {
        using var input = new MemoryStream(bytes);
        using var zlib = new ZLibStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        zlib.CopyTo(output);
        return output.ToArray();
    }
}
