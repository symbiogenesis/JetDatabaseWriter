namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>
/// Round-trip tests for the row-level complex-column APIs:
/// <see cref="IAccessWriter.AddAttachmentAsync"/>,
/// <see cref="IAccessWriter.AddMultiValueItemAsync"/>,
/// <see cref="IAccessReader.GetAttachmentsAsync"/>,
/// <see cref="IAccessReader.GetMultiValueItemsAsync"/>, and the
/// <see cref="AttachmentWrapper"/> encoder / decoder per
/// <see href="docs/design/complex-columns-format-notes.md" /> §3.
/// </summary>
public sealed class ComplexColumnsRowApiTests
{
    [Fact]
    public void AttachmentWrapper_RoundTrips_Raw_For_Jpg()
    {
        // Formats Access stores raw (jpg, png, zip, ...) skip deflate per spec §3.2.
        byte[] payload = Encoding.UTF8.GetBytes("FAKE-JPEG-PAYLOAD");
        byte[] wrapped = AttachmentWrapper.Encode("jpg", payload);

        // typeFlag 0, dataLen = the content length, then the content as is.
        byte[] content = AccessContent("jpg", payload);
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(wrapped));
        Assert.Equal(content.Length, BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(4)));
        Assert.Equal(content, wrapped[8..]);

        bool ok = AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded);
        Assert.True(ok);
        Assert.Equal("jpg", ext);
        Assert.Equal(payload, decoded);
    }

    [Fact]
    public void AttachmentWrapper_RoundTrips_Deflated_For_Txt()
    {
        // Every other extension is compressed, in Access's zlib format (§3.2).
        byte[] payload = Encoding.UTF8.GetBytes(new string('a', 256));
        byte[] wrapped = AttachmentWrapper.Encode("txt", payload);

        // typeFlag 1, dataLen = the uncompressed content length (20 + payload
        // for "txt"), a 78 5E zlib header and the big-endian Adler-32 of the
        // content. The deflate blocks themselves differ between zlib builds.
        byte[] content = AccessContent("txt", payload);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(wrapped));
        Assert.Equal(20 + payload.Length, BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(4)));
        Assert.Equal(content.Length, BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(4)));
        Assert.Equal([0x78, 0x5E], wrapped[8..10]);
        Assert.Equal(Adler32(content), BinaryPrimitives.ReadUInt32BigEndian(wrapped.AsSpan(wrapped.Length - 4)));
        Assert.Equal(content, InflateZlib(wrapped[8..]));

        bool ok = AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded);
        Assert.True(ok);
        Assert.Equal("txt", ext);
        Assert.Equal(payload, decoded);
    }

    [Fact]
    public void AttachmentWrapper_Encode_EmptyExtension_WritesNulOnlyExtension()
    {
        // Follows Jackcess: the extension is just its NUL, so the count is 1
        // and the content header is 14 bytes. What Access writes here is unchecked.
        byte[] payload = [1, 2, 3];
        byte[] wrapped = AttachmentWrapper.Encode(string.Empty, payload);

        byte[] content = InflateZlib(wrapped[8..]);
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(wrapped));
        Assert.Equal(AccessContent(string.Empty, payload), content);
        Assert.Equal(14, BinaryPrimitives.ReadInt32LittleEndian(content));
        Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(8)));

        Assert.True(AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded));
        Assert.Equal(string.Empty, ext);
        Assert.Equal(payload, decoded);
    }

    [Fact]
    public void AttachmentWrapper_Encode_UpperCaseExtension_LowercasesIt()
    {
        // Jackcess lowercases the extension before choosing raw storage and
        // writing it into the content header.
        byte[] payload = Encoding.UTF8.GetBytes("FAKE-JPEG-PAYLOAD");
        byte[] wrapped = AttachmentWrapper.Encode("JPG", payload);

        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(wrapped));
        Assert.Equal(AccessContent("jpg", payload), wrapped[8..]);
        Assert.True(AttachmentWrapper.TryDecode(wrapped, out string ext, out _));
        Assert.Equal("jpg", ext);
    }

    [Theory]
    [InlineData("jpg", 0)]
    [InlineData("jpeg", 0)]
    [InlineData("gif", 0)]
    [InlineData("png", 0)]
    [InlineData("zip", 0)]
    [InlineData("cab", 0)]
    [InlineData("docx", 0)]
    [InlineData("xlsx", 0)]
    [InlineData("xlsb", 0)]
    [InlineData("pptx", 0)]
    [InlineData("txt", 1)]
    [InlineData("pdf", 1)]
    [InlineData("bmp", 1)]
    [InlineData("gz", 1)]
    [InlineData("mp3", 1)]
    [InlineData("7z", 1)]
    [InlineData("rar", 1)]
    [InlineData("mpg", 1)]
    public void AttachmentWrapper_Encode_SkipList_MatchesAccess(string extension, int expectedTypeFlag)
    {
        // The formats Access stores raw (Jackcess 4.0.12's COMPRESSED_FORMATS);
        // Access deflates everything else, even already-compressed formats.
        byte[] payload = Encoding.UTF8.GetBytes("payload");
        byte[] wrapped = AttachmentWrapper.Encode(extension, payload);

        Assert.Equal(expectedTypeFlag, BinaryPrimitives.ReadInt32LittleEndian(wrapped));
        Assert.True(AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded));
        Assert.Equal(extension, ext);
        Assert.Equal(payload, decoded);
    }

    [Fact]
    public void AttachmentWrapper_TryDecode_LegacyWriterRawDeflate_StillDecodes()
    {
        // Earlier builds of this library wrote raw deflate with dataLen set to
        // the compressed length and the extension length counted in bytes.
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
        Assert.Throws<InvalidDataException>(() => InflateZlib(rawBody));

        bool ok = AttachmentWrapper.TryDecode(wrapped, out string ext, out byte[] decoded);

        Assert.True(ok);
        Assert.Equal("txt", ext);
        Assert.Equal(Encoding.UTF8.GetBytes("raw deflate payload"), decoded);
    }

    [Fact]
    public void AttachmentWrapper_TryDecode_AcceptsAccessZlibWrappedBody()
    {
        // Access (and Jackcess) compress with a zlib header, store the
        // uncompressed content length in dataLen and count the extension in
        // characters including its NUL (4 for "txt"), not in bytes.
        byte[] payload = Encoding.UTF8.GetBytes("zlib attachment payload");
        byte[] content = AccessContent("txt", payload);
        Assert.Equal(4, BinaryPrimitives.ReadInt32LittleEndian(content.AsSpan(8)));

        byte[] body = DeflateWithZlib(content);
        byte[] wrapped = new byte[8 + body.Length];
        BinaryPrimitives.WriteInt32LittleEndian(wrapped, 1);
        BinaryPrimitives.WriteInt32LittleEndian(wrapped.AsSpan(4), content.Length);
        body.CopyTo(wrapped, 8);

        bool ok = AttachmentWrapper.TryDecode(wrapped, out string decodedExt, out byte[] decoded);

        Assert.True(ok);
        Assert.Equal("txt", decodedExt);
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
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
                new Dictionary<string, object?> { ["Id"] = 1 },
                new AttachmentInput("notes.txt", payload),
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "Files", TestContext.Current.CancellationToken);

        AttachmentRecord single = Assert.Single(attachments);
        Assert.Equal("notes.txt", single.FileName);
        Assert.Equal("txt", single.FileType);
        Assert.True(single.ConceptualTableId > 0);
        Assert.Equal(Encoding.UTF8.GetBytes("hello attachments"), single.FileData);
    }

    [Fact]
    public async Task AddAttachmentAsync_TwoFilesSameRow_ShareConceptualTableId()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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

            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            await writer.AddAttachmentAsync("Documents", "Files", key, new AttachmentInput("a.txt", Encoding.UTF8.GetBytes("aaa")), TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync("Documents", "Files", key, new AttachmentInput("b.txt", Encoding.UTF8.GetBytes("bbb")), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "Files", TestContext.Current.CancellationToken);

        Assert.Equal(2, attachments.Count);
        Assert.Equal(attachments[0].ConceptualTableId, attachments[1].ConceptualTableId);
        Assert.Contains(attachments, a => a.FileName == "a.txt");
        Assert.Contains(attachments, a => a.FileName == "b.txt");
    }

    [Fact]
    public async Task AddAttachmentAsync_TwoParentRows_GetDistinctConceptualTableIds()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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

            await writer.AddAttachmentAsync("Documents", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("one.txt", Encoding.UTF8.GetBytes("one")), TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync("Documents", "Files", new Dictionary<string, object?> { ["Id"] = 2 }, new AttachmentInput("two.txt", Encoding.UTF8.GetBytes("two")), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "Files", TestContext.Current.CancellationToken);

        Assert.Equal(2, attachments.Count);
        Assert.NotEqual(attachments[0].ConceptualTableId, attachments[1].ConceptualTableId);
    }

    [Fact]
    public async Task AddAttachmentAsync_NoMatchingRow_Throws()
    {
        await using var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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

        await Assert.ThrowsAsync<JetOperationException>(async () =>
            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object?> { ["Id"] = 99 },
                new AttachmentInput("x.txt", [1, 2, 3]),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AddAttachmentAsync_OnMultiValueColumn_Throws()
    {
        await using var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
                new Dictionary<string, object?> { ["Id"] = 1 },
                new AttachmentInput("x.txt", [1]),
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AddMultiValueItemAsync_RoundTrips_ViaGetMultiValueItems()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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

            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            await writer.AddMultiValueItemAsync("Tags", "Labels", key, 100, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Tags", "Labels", key, 200, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Tags", "Labels", key, 300, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<MultiValueItem> items = await reader.GetMultiValueItemsAsync("Tags", "Labels", TestContext.Current.CancellationToken);

        Assert.Equal(3, items.Count);
        Assert.All(items, item => Assert.Equal(items[0].ConceptualTableId, item.ConceptualTableId));
        int[] sortedValues = items.Select(item => Convert.ToInt32(item.Value, System.Globalization.CultureInfo.InvariantCulture)).Order().ToArray();
        Assert.Equal(100, sortedValues[0]);
        Assert.Equal(200, sortedValues[1]);
        Assert.Equal(300, sortedValues[2]);
    }

    [Fact]
    public async Task AddMultiValueItemAsync_OnAttachmentColumn_Throws()
    {
        await using var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
                new Dictionary<string, object?> { ["Id"] = 1 },
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

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
                new Dictionary<string, object?> { ["Id"] = 1 },
                new AttachmentInput("empty.dat", []),
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync(
            "Documents",
            "Files",
            TestContext.Current.CancellationToken);

        AttachmentRecord single = Assert.Single(attachments);
        Assert.Equal("empty.dat", single.FileName);
        Assert.NotNull(single.FileData);
        Assert.Empty(single.FileData);
    }

    /// <summary>
    /// §2.2 gap: multi-value text column with mixed value lengths. Covers
    /// the <c>Text</c> element path in the flat child table, including
    /// an empty-string value to exercise the zero-length variable-column
    /// entry.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AddMultiValueItemAsync_TextWithMixedLengths_RoundTrips()
    {
        await using var ms = new MemoryStream();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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

            var key = new Dictionary<string, object?> { ["Id"] = 1 };

            // Mixed lengths: empty string, short, medium, long.
            await writer.AddMultiValueItemAsync("Products", "Tags", key, string.Empty, TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Products", "Tags", key, "a", TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Products", "Tags", key, "medium-tag", TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Products", "Tags", key, new string('x', 80), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<MultiValueItem> items = await reader.GetMultiValueItemsAsync(
            "Products",
            "Tags",
            TestContext.Current.CancellationToken);

        Assert.Equal(4, items.Count);

        string[] values = items
            .Select(i => i.Value?.ToString() ?? string.Empty)
            .OrderBy(v => v.Length)
            .ToArray();

        Assert.Equal(string.Empty, values[0]);
        Assert.Equal("a", values[1]);
        Assert.Equal("medium-tag", values[2]);
        Assert.Equal(new string('x', 80), values[3]);
    }

    private static byte[] DeflateWithZlib(byte[] bytes)
    {
        using var output = new MemoryStream();
        using (var zlib = new ZLibStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            zlib.Write(bytes);
        }

        return output.ToArray();
    }
}
