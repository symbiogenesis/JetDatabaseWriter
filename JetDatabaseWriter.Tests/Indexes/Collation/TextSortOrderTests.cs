namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using Xunit;

public sealed class TextSortOrderTests
{
    [Theory]
    [InlineData(false, (byte)0)]
    [InlineData(true, (byte)0)]
    [InlineData(true, (byte)1)]
    public void EncodeTextEntry_UsesDeclaredSortOrder(bool hasVersion, byte version)
    {
        var order = new TextSortOrder(0x0409, version, hasVersion);
        byte[] expected = !hasVersion
            ? General97TextIndexEncoder.Encode("Éléphant!", true)
            : version == 0
                ? GeneralLegacyTextIndexEncoder.Encode("Éléphant!", true)
                : GeneralTextIndexEncoder.Encode("Éléphant!", true);
        Assert.Equal(expected, IndexKeyEncoder.EncodeTextEntry(order, "Éléphant!", true));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public void ZeroHeaderSortOrder_NormalizesNewColumnDefault(DatabaseFormat kind, bool hasVersion)
    {
        byte[] header = new byte[JetFormat.PageSizeOf(kind)];
        header[0x14] = JetFormat.ForNewDatabase(kind).NewDatabaseVersion;
        EncryptionManager.TransformHeaderMask(header, kind);
        byte[] original = (byte[])header.Clone();
        JetFormat format = JetFormat.FromHeader(header);
        Assert.Equal(new TextSortOrder(0x0409, 0, hasVersion), format.DefaultTextSortOrder);
        Assert.Equal(original, header);
    }

    [Fact]
    public void ZeroSortOrder_UsesGeneralLegacy()
    {
        Assert.Equal(GeneralLegacyTextIndexEncoder.Encode("Éléphant!", true), IndexKeyEncoder.EncodeTextEntry(default, "Éléphant!", true));
    }

    [Fact]
    public void ExpressionComparison_KeepsTrailingSpacesAndLongSuffixes()
    {
        var collation = JetTextCollation.GeneralLegacy;
        Assert.Equal(0, collation.Compare("Éléphant!", "éléphant!"));
        Assert.NotEqual(0, collation.Compare("a", "a "));
        string prefix = new('a', Constants.IndexTextEncoding.MaxTextIndexCharLength);
        Assert.True(collation.Compare(prefix + "a", prefix + "b") < 0);
    }

    [Fact]
    public void GeneralExpressionComparison_HighExpansionWindowsHaveNoIndexByteLimit()
    {
        var collation = new JetTextCollation(new TextSortOrder(0x0409, 1, true));
        string text = new('\u06D7', Constants.IndexTextEncoding.MaxTextIndexCharLength * 2);
        Assert.Equal(0, collation.Compare(text, text));
        Assert.True(collation.Compare(text + "a", text + "b") < 0);
        Assert.True(collation.Compare(text + "b", text + "a") > 0);
        Assert.NotEqual(0, collation.Compare(text, text + " "));
    }

    [Fact]
    public void GeneralExpressionComparison_LongAccentedValuesKeepTheirSuffixes()
    {
        var collation = new JetTextCollation(new TextSortOrder(0x0409, 1, true));
        string text = new('é', 255);
        Assert.Equal(0, collation.Compare(text, text));
        Assert.True(collation.Compare(text + "a", text + "b") < 0);
    }

    [Fact]
    public void GeneralLongKeyWithoutSuffixTable_RefusesEncoding()
    {
        Assert.Throws<NotSupportedException>(() => IndexKeyEncoder.EncodeTextEntry(new TextSortOrder(0x0409, 1, true), new string('é', 255)));
    }

    [Fact]
    public void UnsupportedSortOrder_RefusesNonNullText()
    {
        Assert.Throws<NotSupportedException>(() => IndexKeyEncoder.EncodeTextEntry(new TextSortOrder(0x041D, 0, true), "a", true));
    }
}
