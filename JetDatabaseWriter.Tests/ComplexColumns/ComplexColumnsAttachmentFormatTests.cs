namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>
/// The attachment <c>FileData</c> wrapper the writer stores matches the one
/// Access writes: typeFlag, <c>dataLen</c> = the uncompressed content length,
/// the extension counted in characters including its NUL, and a zlib stream
/// (<c>78 5E</c>, deflate blocks, big-endian Adler-32) for every format Access
/// does not store raw. Checked against all 24 Access-authored attachments in
/// the fixtures, and against a decoder that reads the wrapper the way Jackcess
/// <c>AttachmentColumnInfoImpl.decodeData</c> does.
/// </summary>
public sealed class ComplexColumnsAttachmentFormatTests
{
    /// <summary>Gets the Access-authored attachment columns and how many attachments each holds.</summary>
    /// <returns>Fixture path, table, column and attachment count.</returns>
    public static TheoryData<string, string, string, int> AccessAttachmentColumns() => new()
    {
        { TestDatabases.ComplexFields, "Documents", "Attachments", 2 },
        { TestDatabases.ComplexDataTestV2007, "Table1", "attach-data", 3 },
        { TestDatabases.ComplexDataTestV2010, "Table1", "attach-data", 3 },
        { TestDatabases.NorthwindTraders, "ProductCategories", "ProductCategoryImage", 16 },
    };

    /// <summary>
    /// Re-encoding each Access attachment's decoded file with its
    /// <c>FileType</c> gives Access's wrapper back: byte for byte when Access
    /// stored it raw, and with the same typeFlag, <c>dataLen</c>, zlib header,
    /// Adler-32 and inflated content when Access compressed it. Only the
    /// deflate blocks may differ, because no .NET runtime reproduces Access's
    /// compressor (they also differ between the zlib builds of .NET 8 and 10).
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <param name="table">The parent table.</param>
    /// <param name="column">The attachment column.</param>
    /// <param name="count">How many attachments the column holds.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AccessAttachmentColumns))]
    public async Task AccessAuthoredFileData_ReEncodesToSameWrapper(string fixture, string table, string column, int count)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        List<StoredAttachment> stored = await ReadStoredAttachmentsAsync(ms, table, column);
        Assert.Equal(count, stored.Count);

        foreach (StoredAttachment attachment in stored)
        {
            Assert.True(AttachmentWrapper.TryDecode(attachment.FileData, out string extension, out byte[] payload), attachment.FileName);
            Assert.Equal(attachment.FileType, extension);

            byte[] reEncoded = AttachmentWrapper.Encode(attachment.FileType, payload);
            if (BinaryPrimitives.ReadInt32LittleEndian(attachment.FileData) == 0)
            {
                Assert.Equal(attachment.FileData, reEncoded);
                continue;
            }

            Assert.Equal(attachment.FileData[..10], reEncoded[..10]);
            Assert.Equal(attachment.FileData[^4..], reEncoded[^4..]);
            Assert.Equal(InflateZlib(attachment.FileData[8..]), InflateZlib(reEncoded[8..]));
        }
    }

    /// <summary>
    /// The writer's stored <c>FileData</c> decodes with Jackcess's rules: zlib
    /// for a compressed body, and exactly <c>dataLen - headerLen</c> file bytes
    /// after the content header. Covers a small compressed text file, a 20 KB
    /// one that does not compress and so spans a chained LVAL value, and a
    /// jpg Access stores raw.
    /// </summary>
    /// <param name="fileName">The attachment's file name.</param>
    /// <param name="length">The file length.</param>
    /// <param name="expectedTypeFlag">The typeFlag Access would write for the extension.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("a.txt", 40, 1)]
    [InlineData("big.txt", 20 * 1024, 1)]
    [InlineData("p.jpg", 1500, 0)]
    public async Task AddAttachment_WriterBytes_DecodeWithJackcessSemantics(string fileName, int length, int expectedTypeFlag)
    {
        byte[] file = length < 100 ? Encoding.UTF8.GetBytes(new string('t', length)) : Incompressible(length);

        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await writer.CreateTableAsync(
                "Docs",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
                Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput(fileName, file), Ct);
        }

        StoredAttachment stored = Assert.Single(await ReadStoredAttachmentsAsync(ms, "Docs", "Files"));
        Assert.Equal(expectedTypeFlag, BinaryPrimitives.ReadInt32LittleEndian(stored.FileData));
        Assert.Equal(file, DecodeLikeJackcess(stored.FileData));

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(file, Assert.Single(await reader.GetAttachmentsAsync("Docs", "Files", Ct)).FileData);
    }

    /// <summary>
    /// Mirrors Jackcess <c>AttachmentColumnInfoImpl.decodeData</c>: inflate a
    /// typeFlag-1 body as zlib, skip the content header, then read exactly
    /// <c>dataLen - headerLen</c> bytes. The extension length field is not read.
    /// </summary>
    /// <param name="encoded">The stored <c>FileData</c> value.</param>
    private static byte[] DecodeLikeJackcess(byte[] encoded)
    {
        int typeFlag = BinaryPrimitives.ReadInt32LittleEndian(encoded);
        int dataLen = BinaryPrimitives.ReadInt32LittleEndian(encoded.AsSpan(4));
        Assert.True(typeFlag is 0 or 1, $"Unknown typeFlag {typeFlag}.");

        using var body = new MemoryStream(encoded, 8, encoded.Length - 8);
        using Stream content = typeFlag == 1 ? new ZLibStream(body, CompressionMode.Decompress) : body;
        using var reader = new BinaryReader(content);
        int headerLen = reader.ReadInt32();
        Assert.Equal(headerLen - 4, reader.ReadBytes(headerLen - 4).Length);
        byte[] data = reader.ReadBytes(dataLen - headerLen);
        Assert.Equal(dataLen - headerLen, data.Length);
        return data;
    }

    /// <summary>Returns xorshift bytes, which deflate cannot shrink.</summary>
    /// <param name="length">The number of bytes.</param>
    private static byte[] Incompressible(int length)
    {
        byte[] bytes = new byte[length];
        uint state = 2463534242;
        for (int i = 0; i < length; i++)
        {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            bytes[i] = unchecked((byte)state);
        }

        return bytes;
    }

    /// <summary>
    /// Reads every row of <paramref name="column"/>'s flat table with
    /// <c>FileData</c> as stored, through the writer's snapshot decode.
    /// </summary>
    /// <param name="ms">The database stream.</param>
    /// <param name="table">The parent table.</param>
    /// <param name="column">The attachment column.</param>
    private static async Task<List<StoredAttachment>> ReadStoredAttachmentsAsync(MemoryStream ms, string table, string column)
    {
        int flatTableId;
        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            flatTableId = Assert.Single(await reader.GetComplexColumnsAsync(table, Ct), c => c.ColumnName == column).FlatTableId;
        }

        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        long flatTdefPage = CatalogValueReader.TdefPageFromId((long)flatTableId);
        TableDef flat = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(flatTdefPage, Ct));
        int fileName = flat.FindColumnIndex("FileName");
        int fileType = flat.FindColumnIndex("FileType");
        int fileData = flat.FindColumnIndex("FileData");

        return [.. (await harness.Services.Snapshots.ReadRowsAsync(flatTdefPage, Ct)).Select(row => new StoredAttachment(
            (string)row.Values[fileName],
            (string)row.Values[fileType],
            (byte[])row.Values[fileData]))];
    }

    /// <summary>One flat-table attachment row with its <c>FileData</c> as stored.</summary>
    /// <param name="FileName">The file name.</param>
    /// <param name="FileType">The <c>FileType</c> column.</param>
    /// <param name="FileData">The stored wrapper bytes.</param>
    private sealed record StoredAttachment(string FileName, string FileType, byte[] FileData);
}
