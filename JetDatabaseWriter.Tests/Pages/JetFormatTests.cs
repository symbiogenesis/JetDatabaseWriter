namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the immutable format profile <see cref="JetFormat"/> that every open
/// database file builds from its header: the format, the page size, the code
/// page and the per-format byte layouts, spelled out here from mdbtools
/// HACKING.md and Jackcess <c>JetFormat</c> rather than read back from the
/// layout structs, the capability flags and per-format values code asks it
/// for instead of comparing formats, and the text and name codecs that
/// depend on them.
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
        Assert.Equal(kind == DatabaseFormat.Jet3Mdb, format.RowFields.HasJumpTable);
        Assert.Equal(kind == DatabaseFormat.AceAccdb ? 28 : -1, format.TDef.ComplexAutoNumber);
        Assert.Equal(kind == DatabaseFormat.Jet3Mdb ? 39 : 52, format.Index.RealIdxPhysSize);
        Assert.Equal(kind == DatabaseFormat.Jet3Mdb ? 0x16 : 0x1B, format.IndexPage.BitmaskOffset);
    }

    /// <summary>
    /// Pins every capability flag of each format: the facts library code asks
    /// the profile instead of comparing formats. The column types and features
    /// each format stores, Jet4's legacy Numeric index keys, the TDEF format
    /// magic and free-space word the writer stamps only on Jet4 and ACE, and
    /// index seeks on all supported formats.
    /// A flag added to the profile appears here and needs its rows updated.
    /// </summary>
    /// <param name="kind">The database format.</param>
    /// <param name="expectedTrueFlags">The profile's flags that are true, in name order.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "IsJet3 SupportsIndexSeeks")]
    [InlineData(DatabaseFormat.Jet4Mdb, "LegacyNumericIndexKeys SupportsIndexSeeks SupportsNumeric WritesTDefFormatMagic WritesTDefFreeSpace")]
    [InlineData(DatabaseFormat.AceAccdb, "SupportsBigInt SupportsCalculatedColumns SupportsComplexColumns SupportsDateTimeExtended SupportsIndexSeeks SupportsNumeric WritesTDefFormatMagic WritesTDefFreeSpace")]
    public void CapabilityFlags_MatchFormat(DatabaseFormat kind, string expectedTrueFlags)
        => Assert.Equal(expectedTrueFlags, TrueFlags(JetFormat.ForNewDatabase(kind)));

    /// <summary>
    /// Pins the per-format values the profile answers: the version name the
    /// statistics and diagnostics print, the commit-lock offset, the header
    /// signature and version byte of a new database (which the profile's
    /// <see cref="JetFormat.DetectFormat"/> reads back), and the text encoding
    /// and magic of an <c>LvProp</c> property block.
    /// </summary>
    /// <param name="kind">The database format.</param>
    /// <param name="versionName">The expected version name.</param>
    /// <param name="commitLockOffset">The expected commit-lock offset.</param>
    /// <param name="headerSignature">The expected signature at header offset 4, without its NUL.</param>
    /// <param name="newDatabaseVersion">The expected version byte at header offset 0x14 of a new database.</param>
    /// <param name="propertyTextCodePage">The expected code page of property-block text.</param>
    /// <param name="propertyBlockMagic">The expected property-block magic.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "Jet3", 0xFFFFFFFEL, "Standard Jet DB", 0, 1252, 0x00444B4BU)]
    [InlineData(DatabaseFormat.Jet4Mdb, "Jet4/ACE", 0xFFFFFFFEL, "Standard Jet DB", 1, 1200, 0x0032524DU)]
    [InlineData(DatabaseFormat.AceAccdb, "Jet4/ACE", 0xFFFFFFFCL, "Standard ACE DB", 2, 1200, 0x0032524DU)]
    public void Values_MatchFormat(
        DatabaseFormat kind,
        string versionName,
        long commitLockOffset,
        string headerSignature,
        int newDatabaseVersion,
        int propertyTextCodePage,
        uint propertyBlockMagic)
    {
        var format = JetFormat.ForNewDatabase(kind);

        Assert.Equal(versionName, format.VersionName);
        Assert.Equal(commitLockOffset, format.CommitLockOffset);
        Assert.Equal(Encoding.ASCII.GetBytes(headerSignature + "\0"), format.HeaderSignature.ToArray());
        Assert.Equal(newDatabaseVersion, format.NewDatabaseVersion);
        Assert.Equal(kind, JetFormat.DetectFormat(TDefPageBuilder.BuildEmptyDatabase(kind, fullCatalogSchema: true)));
        Assert.Equal(propertyTextCodePage, JetFormat.ForNewDatabase(kind).PropertyTextEncoding.CodePage);
        Assert.Equal(propertyBlockMagic, JetFormat.PropertyBlockMagicOf(kind));
    }

    /// <summary>
    /// The format comes from the version byte at header offset 0x14: 0 is
    /// Jet3, 1 is Jet4, and the named Access 2007–2019 versions are ACE.
    /// </summary>
    /// <param name="version">The version byte.</param>
    /// <param name="expected">The expected format.</param>
    [Theory]
    [InlineData(0x00, DatabaseFormat.Jet3Mdb)]
    [InlineData(0x01, DatabaseFormat.Jet4Mdb)]
    [InlineData(0x02, DatabaseFormat.AceAccdb)]
    [InlineData(0x03, DatabaseFormat.AceAccdb)]
    [InlineData(0x04, DatabaseFormat.AceAccdb)]
    [InlineData(0x05, DatabaseFormat.AceAccdb)]
    [InlineData(0x06, DatabaseFormat.AceAccdb)]
    public void DetectFormat_ReadsTheVersionByte(int version, DatabaseFormat expected)
    {
        byte[] header = new byte[Constants.DatabaseHeader.Length];
        header[0x14] = (byte)version;

        Assert.Equal(expected, JetFormat.DetectFormat(header));
    }

    [Fact]
    public void DetectFormat_UnknownVersions_AreExplicitlyUnsupported()
    {
        byte[] header = new byte[Constants.DatabaseHeader.Length];
        for (int version = 7; version <= byte.MaxValue; version++)
        {
            header[0x14] = (byte)version;
            Assert.Throws<NotSupportedException>(() => JetFormat.DetectFormat(header));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(20)]
    public void DetectFormat_TruncatedHeader_RefusesMissingVersion(int length)
    {
        Assert.Throws<ArgumentException>(() => JetFormat.DetectFormat(new byte[length]));
    }

    /// <summary>
    /// A value outside the enum gets no profile, so <c>CreateDatabaseAsync</c>
    /// refuses it, naming its parameter, before it writes anything.
    /// </summary>
    [Fact]
    public void ForNewDatabase_UndefinedFormat_Throws()
    {
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(() => JetFormat.ForNewDatabase((DatabaseFormat)3));

        Assert.Equal("format", ex.ParamName);
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

    /// <summary>An invalid code page must not silently reinterpret stored text as UTF-8.</summary>
    [Fact]
    public void FromHeader_UnmaskedJet3Header_RefusesUnknownCodePage()
    {
        byte[] header = new byte[Constants.DatabaseHeader.Length];
        header[0x01] = 0x01;
        NotSupportedException ex = Assert.Throws<NotSupportedException>(() => JetFormat.FromHeader(header));
        Assert.Contains("code page 17019", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void TextCodec_MatchesFormat(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);

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
        var jet3 = JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb);

        Assert.Null(jet3.DescribeUnstorableCharacter(SampleText));
        Assert.Equal("'中' (U+4E2D)", jet3.DescribeUnstorableCharacter("x中"));
        _ = Assert.Throws<EncoderFallbackException>(() => jet3.EncodeAnsiText("中"));
        Assert.Null(JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb).DescribeUnstorableCharacter("x中"));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ReadColumnName_ReadsJet3AndJet4Names(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
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
    public async Task ForNewDatabase_MatchesFromHeaderLayoutsAndFlags(string fixture)
    {
        var fromHeader = JetFormat.FromHeader(await ReadHeaderAsync(fixture));
        var forFormat = JetFormat.ForNewDatabase(fromHeader.Kind);

        Assert.Equal(fromHeader.Kind, forFormat.Kind);
        Assert.Equal(TrueFlags(fromHeader), TrueFlags(forFormat));
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
    /// The database file builds its immutable format from the open file header.
    /// </summary>
    /// <param name="fixture">The fixture name.</param>
    [Theory]
    [InlineData("Jet3Test")]
    [InlineData("NorthwindTraders")]
    public async Task DatabaseFile_FormatComesFromHeader(string fixture)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(PathOf(fixture), cancellationToken: TestContext.Current.CancellationToken);
        DatabaseFile db = harness.Database;
        var expected = JetFormat.FromHeader(await ReadHeaderAsync(fixture));

        Assert.Equal(expected.Kind, db.Format.Kind);
        Assert.Equal(expected.CodePage, db.Format.CodePage);
        Assert.Equal(expected.PageSize, db.Format.PageSize);
        Assert.Equal(expected.DataPage, db.Format.DataPage);
        Assert.Equal(expected.TDef, db.Format.TDef);
        Assert.Equal(expected.RowFields, db.Format.RowFields);

        byte[] text = db.Format.EncodeText(SampleText);
        Assert.Equal(SampleText, db.Format.DecodeText(text, 0, text.Length));
        Assert.Equal(string.Empty, db.Format.DecodeText(text, 0, 0));
    }

    /// <summary>
    /// Lists the names of the profile's <see cref="bool"/> properties that are
    /// true, in name order, read by reflection so a new flag cannot be missed.
    /// </summary>
    /// <param name="format">The profile.</param>
    /// <returns>The names, separated by spaces.</returns>
    private static string TrueFlags(JetFormat format)
        => string.Join(
            ' ',
            typeof(JetFormat).GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Where(property => property.PropertyType == typeof(bool) && property.GetValue(format) is true)
                .Select(static property => property.Name)
                .OrderBy(static name => name, StringComparer.Ordinal));

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
