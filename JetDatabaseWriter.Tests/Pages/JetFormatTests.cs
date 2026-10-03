namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the immutable format profile <see cref="JetFormat"/> that every open
/// database file builds from its header: the format, the page size, the code
/// page and the per-format byte layouts, spelled out here from mdbtools
/// HACKING.md and Jackcess <c>JetFormat</c> rather than read back from the
/// layout structs, plus the text and name codecs that depend on them.
/// </summary>
public sealed class JetFormatTests
{
    /// <summary>"café ü": every character is in Windows-1252, and two of them are above 0x7F.</summary>
    private const string SampleText = "café ü";

    /// <summary>The sample's Windows-1252 bytes, which are also its compressed-Unicode payload.</summary>
    private static readonly byte[] SampleAnsiBytes = [0x63, 0x61, 0x66, 0xE9, 0x20, 0xFC];

    [Theory]
    [InlineData("Jet3Test", DatabaseFormat.Jet3Mdb, 2048, 4, 8, 10, 12, 43, 18, 1)]
    [InlineData("AdventureLT2008", DatabaseFormat.Jet4Mdb, 4096, 4, 12, 14, 16, 63, 25, 2)]
    [InlineData("NorthwindTraders", DatabaseFormat.AceAccdb, 4096, 4, 12, 14, 16, 63, 25, 2)]
    [InlineData("ComplexFields", DatabaseFormat.AceAccdb, 4096, 4, 12, 14, 16, 63, 25, 2)]
    public async Task FromHeader_ReadsFormatPageSizeAndLayouts(
        string fixture,
        DatabaseFormat kind,
        int pageSize,
        int dataPageTDefOffset,
        int dataPageNumRows,
        int dataPageRowsStart,
        int tdefNumRows,
        int tdefBlockEnd,
        int columnDescriptorSize,
        int rowColumnCountSize)
    {
        var format = JetFormat.FromHeader(await ReadHeaderAsync(fixture));

        Assert.Equal(kind, format.Kind);
        Assert.Equal(kind == DatabaseFormat.Jet3Mdb, format.IsJet3);
        Assert.Equal(pageSize, format.PageSize);
        Assert.Equal(dataPageTDefOffset, format.DataPage.TDefOff);
        Assert.Equal(dataPageNumRows, format.DataPage.NumRows);
        Assert.Equal(dataPageRowsStart, format.DataPage.RowsStart);
        Assert.Equal(format.DataPage, format.LvalPage.DataPage);
        Assert.Equal(tdefNumRows, format.TDef.NumRows);
        Assert.Equal(tdefBlockEnd, format.TDef.BlockEnd);
        Assert.Equal(columnDescriptorSize, format.ColumnDescriptor.Size);
        Assert.Equal(rowColumnCountSize, format.RowFields.NumCols);
        Assert.Equal(kind, format.Index.Format);
        Assert.Equal(kind == DatabaseFormat.Jet3Mdb ? 0x16 : 0x1B, format.IndexPage.BitmaskOffset);
    }

    [Theory]
    [InlineData("Jet3Test")]
    [InlineData("AdventureLT2008")]
    [InlineData("NorthwindTraders")]
    public async Task FromHeader_ResolvesWindows1252(string fixture)
    {
        var format = JetFormat.FromHeader(await ReadHeaderAsync(fixture));

        Assert.Equal(1252, format.CodePage);
        Assert.Equal(1252, format.AnsiEncoding.CodePage);
    }

    /// <summary>
    /// Jet3 files from builds before the header mask was written hold zeros
    /// there, which unmask to code page 17019; .NET has no such encoding, so
    /// the profile reads them as UTF-8, the encoding those builds wrote.
    /// </summary>
    [Fact]
    public void FromHeader_UnmaskedJet3Header_FallsBackToUtf8()
    {
        byte[] header = new byte[Constants.DatabaseHeader.Length];
        header[0x01] = 0x01;

        var format = JetFormat.FromHeader(header);

        Assert.Equal(DatabaseFormat.Jet3Mdb, format.Kind);
        Assert.Equal(65001, format.CodePage);
        _ = Assert.IsType<UTF8Encoding>(format.AnsiEncoding, exactMatch: false);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void TextCodec_MatchesFormat(DatabaseFormat kind)
    {
        var format = JetFormat.For(kind);

        byte[] encoded = format.EncodeText(SampleText);
        byte[] expected = kind == DatabaseFormat.Jet3Mdb ? SampleAnsiBytes : [0xFF, 0xFE, .. SampleAnsiBytes];
        Assert.Equal(expected, encoded);
        Assert.Equal(SampleText, format.DecodeText(encoded, 0, encoded.Length));
        Assert.Equal(string.Empty, format.DecodeText(encoded, 0, 0));

        byte[] uncompressed = format.EncodeText(SampleText, compress: false);
        byte[] expectedUncompressed = kind == DatabaseFormat.Jet3Mdb ? SampleAnsiBytes : Encoding.Unicode.GetBytes(SampleText);
        Assert.Equal(expectedUncompressed, uncompressed);
        Assert.Equal(SampleText, format.DecodeText(uncompressed, 0, uncompressed.Length));
    }

    [Fact]
    public void TextCodec_Jet3_RefusesCharacterOutsideCodePage()
    {
        var jet3 = JetFormat.For(DatabaseFormat.Jet3Mdb);

        Assert.Null(jet3.DescribeUnstorableCharacter(SampleText));
        Assert.Equal("'中' (U+4E2D)", jet3.DescribeUnstorableCharacter("x中"));
        _ = Assert.Throws<EncoderFallbackException>(() => jet3.EncodeAnsiText("中"));
        Assert.Null(JetFormat.For(DatabaseFormat.AceAccdb).DescribeUnstorableCharacter("x中"));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ReadColumnName_ReadsJet3AndJet4Names(DatabaseFormat kind)
    {
        var format = JetFormat.For(kind);
        byte[] record = format.EncodeTDefNameRecord(SampleText);
        int prefix = kind == DatabaseFormat.Jet3Mdb ? 1 : 2;
        int payload = kind == DatabaseFormat.Jet3Mdb ? SampleAnsiBytes.Length : SampleText.Length * 2;
        Assert.Equal(prefix + payload, record.Length);

        byte[] td = [0xAA, .. record, 0xBB];
        int pos = 1;
        int consumed = format.ReadColumnName(td, ref pos, out string name);

        Assert.Equal(SampleText, name);
        Assert.Equal(record.Length, consumed);
        Assert.Equal(1 + record.Length, pos);

        // A name that runs past the buffer is not read.
        int truncatedPos = 1;
        Assert.Equal(-1, format.ReadColumnName(td.AsSpan(0, td.Length - 2).ToArray(), ref truncatedPos, out string truncated));
        Assert.Equal(string.Empty, truncated);
    }

    [Theory]
    [InlineData("Jet3Test")]
    [InlineData("AdventureLT2008")]
    [InlineData("NorthwindTraders")]
    public async Task For_MatchesFromHeaderLayouts(string fixture)
    {
        var fromHeader = JetFormat.FromHeader(await ReadHeaderAsync(fixture));
        var forFormat = JetFormat.For(fromHeader.Kind);

        Assert.Equal(fromHeader.Kind, forFormat.Kind);
        Assert.Equal(fromHeader.IsJet3, forFormat.IsJet3);
        Assert.Equal(fromHeader.PageSize, forFormat.PageSize);
        Assert.Equal(JetFormat.PageSizeOf(fromHeader.Kind), forFormat.PageSize);
        Assert.Equal(1252, forFormat.CodePage);
        Assert.Equal(fromHeader.DataPage, forFormat.DataPage);
        Assert.Equal(fromHeader.LvalPage, forFormat.LvalPage);
        Assert.Equal(fromHeader.TDef, forFormat.TDef);
        Assert.Equal(fromHeader.ColumnDescriptor, forFormat.ColumnDescriptor);
        Assert.Equal(fromHeader.RowFields, forFormat.RowFields);
        Assert.Equal(fromHeader.Index, forFormat.Index);
        Assert.Equal(fromHeader.IndexPage, forFormat.IndexPage);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 2048)]
    [InlineData(DatabaseFormat.Jet4Mdb, 4096)]
    [InlineData(DatabaseFormat.AceAccdb, 4096)]
    public void PageSizeOf_MatchesFormat(DatabaseFormat kind, int pageSize)
    {
        Assert.Equal(pageSize, JetFormat.PageSizeOf(kind));
        Assert.Equal(pageSize, new DatabaseStatistics { Format = kind }.PageSize);
    }

    /// <summary>
    /// The database file an open reader builds takes its format members from
    /// its profile, which it builds from the file's header.
    /// </summary>
    /// <param name="fixture">The fixture name.</param>
    [Theory]
    [InlineData("Jet3Test")]
    [InlineData("NorthwindTraders")]
    public async Task DatabaseFile_FormatMembers_ComeFromProfile(string fixture)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(PathOf(fixture), cancellationToken: TestContext.Current.CancellationToken);
        DatabaseFile db = harness.Database;
        var expected = JetFormat.FromHeader(await ReadHeaderAsync(fixture));

        Assert.Equal(expected.Kind, db.Profile.Kind);
        Assert.Equal(expected.CodePage, db.Profile.CodePage);
        Assert.Equal(db.Profile.Kind, db.Format);
        Assert.Equal(db.Profile.PageSize, db.PageSizeBytes);
        Assert.Equal(db.Profile.CodePage, db.CodePage);
        Assert.Equal(db.Profile.DataPage, db.DataPage);
        Assert.Equal(db.Profile.TDef, db.TDef);
        Assert.Equal(db.Profile.RowFields, db.RowFields);
    }

    private static async ValueTask<byte[]> ReadHeaderAsync(string fixture)
    {
        byte[] file = await File.ReadAllBytesAsync(PathOf(fixture), TestContext.Current.CancellationToken);
        return file.AsSpan(0, Constants.DatabaseHeader.Length).ToArray();
    }

    private static string PathOf(string fixture) => fixture switch
    {
        "Jet3Test" => TestDatabases.Jet3Test,
        "AdventureLT2008" => TestDatabases.AdventureWorks,
        "NorthwindTraders" => TestDatabases.NorthwindTraders,
        "ComplexFields" => TestDatabases.ComplexFields,
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, "Unknown fixture."),
    };
}
