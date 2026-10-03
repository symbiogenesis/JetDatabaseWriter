namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// <see cref="OleObjectValue"/> unwraps the stored bytes of OLE Object values: an
/// OLE Package's embedded or linked file and another embedded object's native data,
/// checked against the Access-authored testOleV2007 and test2 fixtures. It never
/// throws, and a value it cannot follow comes back as its stored bytes.
/// </summary>
public sealed class OleObjectValueTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the test2 fixtures, each holding a Word document in MSP_PROJECTS.</summary>
    /// <returns>The fixture paths.</returns>
    public static TheoryData<string> Test2Fixtures() => [TestDatabases.Test2V1997, TestDatabases.Test2V2000, TestDatabases.Test2V2010];

    /// <summary>
    /// Every value of testOleV2007's OLE column, read through the public reader as
    /// stored bytes, parses as Access inserted it: files dropped in as OLE Packages,
    /// two links to files, and Word, Excel and Acrobat documents.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Parse_TestOleV2007Values_UnwrapsEveryObject()
    {
        Assert.SkipUnless(await TestDatabases.IsReadableAsync(TestDatabases.TestOleV2007, Ct), "testOleV2007 is not readable.");
        Dictionary<int, byte[]> values = await ReadOleValuesByLengthAsync(TestDatabases.TestOleV2007, "Table1", "ole_data");

        AssertEmbeddedFile(values[443], "test_data.txt", "Z:\\jackcess_test\\ole\\test_data.txt", 38);
        AssertEmbeddedFile(values[461], "test_datau2.txt", "Z:\\jackcess_test\\ole\\test_datau2.txt", 38);
        OleObjectContent jpg = AssertEmbeddedFile(values[94_602], "test_image.jpg", "Z:\\jackcess_test\\ole\\test_image.jpg", 94_188);
        Assert.Equal("image/jpeg", jpg.MediaType);
        OleObjectContent bmp = AssertEmbeddedFile(values[768_468], "test_image.bmp", "Z:\\jackcess_test\\ole\\test_image.bmp", 768_054);
        Assert.Equal("image/bmp", bmp.MediaType);
        AssertEmbeddedFile(values[377_327], "test_embed_access.accdb", "Z:\\jackcess_test\\ole\\test_embed_access.accdb", -1);

        foreach ((int length, string file) in new[] { (186, "test_data.txt"), (192, "test_datau2.txt") })
        {
            OleObjectContent link = OleObjectValue.Parse(values[length]);
            Assert.Equal(OleObjectKind.LinkedFile, link.Kind);
            Assert.Equal(file, link.FileName);
            Assert.Equal("Z:\\jackcess_test\\ole\\" + file, link.SourcePath);
            Assert.Empty(link.Content);
        }

        OleObjectContent word = OleObjectValue.Parse(values[25_066]);
        Assert.Equal(OleObjectKind.EmbeddedObject, word.Kind);
        Assert.Equal("Word.Document.8", word.ClassName);
        Assert.Equal("Document", word.DisplayName);
        Assert.Equal(24_576, word.Content.Length);
        Assert.Equal([0xD0, 0xCF, 0x11, 0xE0], word.Content[..4]);

        OleObjectContent excel = OleObjectValue.Parse(values[24_899]);
        Assert.Equal(OleObjectKind.EmbeddedObject, excel.Kind);
        Assert.Equal("Excel.Sheet.8", excel.ClassName);
        Assert.Equal(23_552, excel.Content.Length);

        OleObjectContent acrobat = OleObjectValue.Parse(values[5_946_670]);
        Assert.Equal(OleObjectKind.EmbeddedObject, acrobat.Kind);
        Assert.Equal("AcroExch.Document.7", acrobat.ClassName);
        Assert.Equal(4_492_288, acrobat.Content.Length);
    }

    /// <summary>The Word document in test2's MSP_PROJECTS unwraps to its 22,528 bytes on Jet3, Jet4 and ACCDB.</summary>
    /// <param name="fixture">The fixture path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(Test2Fixtures))]
    public async Task Parse_Test2ProjectDocument_ReturnsNativeData(string fixture)
    {
        Assert.SkipUnless(await TestDatabases.IsReadableAsync(fixture, Ct), $"{fixture} is not readable.");
        byte[] stored = Assert.Single((await ReadOleValuesByLengthAsync(fixture, "MSP_PROJECTS", "RESERVED_BINARY_DATA")).Values);

        OleObjectContent content = OleObjectValue.Parse(stored);

        Assert.Equal(OleObjectKind.EmbeddedObject, content.Kind);
        Assert.Equal("Word.Document.8", content.ClassName);
        Assert.Equal(22_528, content.Content.Length);
        Assert.Equal("application/msword", content.MediaType);
        Assert.Equal(content.Content, OleObjectValue.GetContent(stored));
    }

    [Fact]
    public void Parse_NoAccessHeader_IsNotWrappedAndReturnsTheStoredBytes()
    {
        byte[] stored = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3];

        OleObjectContent content = OleObjectValue.Parse(stored);

        Assert.Equal(OleObjectKind.NotWrapped, content.Kind);
        Assert.Same(stored, content.Content);
        Assert.Equal("image/png", content.MediaType);
        Assert.Same(stored, OleObjectValue.GetContent(stored));
        Assert.Equal(OleObjectKind.NotWrapped, OleObjectValue.Parse([]).Kind);
    }

    [Fact]
    public void Parse_SyntheticPackage_ReturnsTheEmbeddedFile()
    {
        byte[] file = [.. "%PDF-1.7 "u8, .. Enumerable.Range(0, 300).Select(i => unchecked((byte)i))];
        byte[] stored = OleObjectReadTests.BuildPackage("report.pdf", file);

        OleObjectContent content = OleObjectValue.Parse(stored);

        Assert.Equal(OleObjectKind.EmbeddedFile, content.Kind);
        Assert.Equal("Packager Shell Object", content.DisplayName);
        Assert.Equal("Package", content.ClassName);
        Assert.Equal("report.pdf", content.FileName);
        Assert.Equal("C:\\Source\\report.pdf", content.SourcePath);
        Assert.Equal(file, content.Content);
        Assert.Equal("application/pdf", content.MediaType);
    }

    [Fact]
    public void Parse_LinkedObject_ReturnsTheLinkSource()
    {
        byte[] display = "Worksheet\0"u8.ToArray();
        byte[] cls = "Excel.Sheet.8\0"u8.ToArray();
        byte[] topic = "C:\\Data\\book.xls\0"u8.ToArray();
        using var ms = new MemoryStream();
        using (var writer = new BinaryWriter(ms, Encoding.ASCII, leaveOpen: true))
        {
            writer.Write((ushort)0x1C15);
            writer.Write((ushort)(20 + display.Length + cls.Length));
            writer.Write(1);
            writer.Write((ushort)display.Length);
            writer.Write((ushort)cls.Length);
            writer.Write((ushort)20);
            writer.Write((ushort)(20 + display.Length));
            writer.Write(uint.MaxValue);
            writer.Write(display);
            writer.Write(cls);
            writer.Write(0x0501);
            writer.Write(1);
            writer.Write(cls.Length);
            writer.Write(cls);
            writer.Write(topic.Length);
            writer.Write(topic);
            writer.Write(0);
        }

        OleObjectContent content = OleObjectValue.Parse(ms.ToArray());

        Assert.Equal(OleObjectKind.LinkedFile, content.Kind);
        Assert.Equal("Excel.Sheet.8", content.ClassName);
        Assert.Equal("C:\\Data\\book.xls", content.SourcePath);
        Assert.Empty(content.Content);
    }

    /// <summary>
    /// Every prefix of a package, from empty to one byte short, parses without
    /// throwing. A prefix that ends before the end of the native data is Unknown
    /// (or NotWrapped, before the signature) and returns itself; one that still
    /// holds the whole native data unwraps to the file, since only the
    /// presentation data and Access's trailer are missing.
    /// </summary>
    [Fact]
    public void Parse_EveryTruncatedPrefixOfAPackage_NeverThrowsAndNeverReadsPastTheEnd()
    {
        byte[] file = Encoding.ASCII.GetBytes("the embedded file");
        byte[] stored = OleObjectReadTests.BuildPackage("f.txt", file);
        int nativeEnd = stored.Length - 12;

        for (int length = 0; length < stored.Length; length++)
        {
            byte[] prefix = stored[..length];
            OleObjectContent content = OleObjectValue.Parse(prefix);
            if (length < nativeEnd)
            {
                Assert.True(content.Kind is OleObjectKind.Unknown or OleObjectKind.NotWrapped, $"A {length}-byte prefix parsed as {content.Kind}.");
                Assert.Same(prefix, content.Content);
            }
            else
            {
                Assert.Equal(OleObjectKind.EmbeddedFile, content.Kind);
                Assert.Equal(file, content.Content);
            }
        }
    }

    [Fact]
    public void Parse_RandomBytesAfterTheAccessSignature_AreUnknownAndReturnTheStoredBytes()
    {
        // A fixed xorshift sequence, so every run checks the same 2,000 values.
        uint state = 20261003;
        for (int i = 0; i < 2000; i++)
        {
            byte[] stored = new byte[2 + (int)(Next(ref state) % 398)];
            for (int b = 0; b < stored.Length; b++)
            {
                stored[b] = unchecked((byte)Next(ref state));
            }

            stored[0] = 0x15;
            stored[1] = 0x1C;

            OleObjectContent content = OleObjectValue.Parse(stored);

            Assert.Equal(OleObjectKind.Unknown, content.Kind);
            Assert.Same(stored, content.Content);
        }
    }

    [Fact]
    public void Parse_HeaderLengthsPastTheValue_AreUnknown()
    {
        byte[] stored = OleObjectReadTests.BuildPackage("f.txt", [1, 2, 3]);

        byte[] headerTooLong = (byte[])stored.Clone();
        headerTooLong[2] = 0xFF;
        headerTooLong[3] = 0xFF;
        Assert.Equal(OleObjectKind.Unknown, OleObjectValue.Parse(headerTooLong).Kind);

        byte[] nativeTooLong = (byte[])stored.Clone();
        const int nativeSize = 50 + 4 + 4 + 4 + 8 + 4 + 4;
        nativeTooLong[nativeSize] = 0xFF;
        nativeTooLong[nativeSize + 1] = 0xFF;
        nativeTooLong[nativeSize + 2] = 0xFF;
        nativeTooLong[nativeSize + 3] = 0xFF;
        OleObjectContent content = OleObjectValue.Parse(nativeTooLong);
        Assert.Equal(OleObjectKind.Unknown, content.Kind);
        Assert.Same(nativeTooLong, content.Content);
    }

    [Theory]
    [InlineData("FFD8FFE0", "image/jpeg")]
    [InlineData("89504E470D0A1A0A", "image/png")]
    [InlineData("474946383961", "image/gif")]
    [InlineData("424D3600", "image/bmp")]
    [InlineData("49492A0008000000", "image/tiff")]
    [InlineData("4D4D002A", "image/tiff")]
    [InlineData("255044462D", "application/pdf")]
    [InlineData("504B0304", "application/zip")]
    [InlineData("D0CF11E0A1B11AE1", "application/msword")]
    [InlineData("7B5C727466", "application/rtf")]
    [InlineData("00014D42", null)]
    [InlineData("151C3200", null)]
    [InlineData("", null)]
    public void DetectMediaType_LooksOnlyAtTheFirstByte(string hex, string? expected)
        => Assert.Equal(expected, OleObjectValue.DetectMediaType(Convert.FromHexString(hex)));

    private static uint Next(ref uint state)
    {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        return state;
    }

    private static OleObjectContent AssertEmbeddedFile(byte[] stored, string fileName, string sourcePath, int length)
    {
        OleObjectContent content = OleObjectValue.Parse(stored);
        Assert.Equal(OleObjectKind.EmbeddedFile, content.Kind);
        Assert.Equal("Package", content.ClassName);
        Assert.Equal("Packager Shell Object", content.DisplayName);
        Assert.Equal(fileName, content.FileName);
        Assert.Equal(sourcePath, content.SourcePath);
        if (length >= 0)
        {
            Assert.Equal(length, content.Content.Length);
        }

        return content;
    }

    /// <summary>Reads every non-null value of an OLE column through the public reader, keyed by length.</summary>
    /// <param name="path">The fixture path.</param>
    /// <param name="table">The table.</param>
    /// <param name="column">The OLE column.</param>
    private static async Task<Dictionary<int, byte[]>> ReadOleValuesByLengthAsync(string path, string table, string column)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: Ct);
        int ordinal = (await reader.GetColumnMetadataAsync(table, Ct)).ToList().FindIndex(c => c.Name == column);
        var values = new Dictionary<int, byte[]>();
        await foreach (object[] row in reader.Rows(table, cancellationToken: Ct))
        {
            if (row[ordinal] is byte[] value)
            {
                values[value.Length] = value;
            }
        }

        return values;
    }
}
