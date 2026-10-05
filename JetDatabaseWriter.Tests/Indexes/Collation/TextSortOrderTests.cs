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
        byte[] expected = (hasVersion, version) switch
        {
            (false, _) => General97TextIndexEncoder.Encode("Éléphant!", true),
            (true, 0) => GeneralLegacyTextIndexEncoder.Encode("Éléphant!", true),
            _ => GeneralTextIndexEncoder.Encode("Éléphant!", true),
        };
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
        var format = JetFormat.FromHeader(header);
        Assert.Equal(new TextSortOrder(0x0409, 0, hasVersion), format.DefaultTextSortOrder);
        Assert.Equal(original, header);
    }

    [Fact]
    public void ZeroSortOrder_UsesGeneralLegacy()
        => Assert.Equal(GeneralLegacyTextIndexEncoder.Encode("Éléphant!", true), IndexKeyEncoder.EncodeTextEntry(default, "Éléphant!", true));

    [Fact]
    public void ExpressionComparison_KeepsTrailingSpacesAndLongSuffixes()
    {
        JetTextCollation collation = JetTextCollation.GeneralLegacy;
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

    [Theory]
    [InlineData(false, (byte)0)]
    [InlineData(true, (byte)0)]
    [InlineData(true, (byte)1)]
    public void WholeTextComparison_PrimaryWeightsPrecedeEarlierAccents(bool hasVersion, byte version)
    {
        var collation = new JetTextCollation(new TextSortOrder(0x0409, version, hasVersion));
        string middle = new('a', 126);
        Assert.True(collation.Compare("é" + middle + "a", "e" + middle + "z") < 0);
        Assert.Equal(0, collation.Compare(middle + "\0b", middle + "b"));
        string memo = new('a', 20000);
        Assert.True(collation.Compare("é" + memo + "a", "e" + memo + "z") < 0);
        Assert.NotEqual(0, collation.Compare(memo, memo + " "));
    }

    [Theory]
    [InlineData(false, (byte)0)]
    [InlineData(true, (byte)0)]
    [InlineData(true, (byte)1)]
    public void ComparisonKeys_PreserveShortIndexKeyOrdering(bool hasVersion, byte version)
    {
        var order = new TextSortOrder(0x0409, version, hasVersion);
        var collation = new JetTextCollation(order);
        string[] corpus = ["", "a", "a ", "ab", "a-b", "a'b", "a\rb", "a\tb", "é", "e", "Æ", "AE", "\u06D7", "\u0001", "a\u0001b"];
        foreach (string left in corpus)
        {
            foreach (string right in corpus)
            {
                byte[] leftIndex = EncodeIndexComparison(order, left);
                byte[] rightIndex = EncodeIndexComparison(order, right);
                Assert.Equal(Math.Sign(IndexPageCodec.CompareKeyBytes(leftIndex, rightIndex)), Math.Sign(collation.Compare(left, right)));
            }
        }

        Assert.Equal(collation.EncodeComparisonKey("a"), collation.EncodeComparisonKey("a ", trimTrailingSpaces: true));
    }

    [Theory]
    [InlineData((byte)0)]
    [InlineData((byte)1)]
    public void MemoComparison_UnprintablePositionsDoNotWrap(byte version)
    {
        var collation = new JetTextCollation(new TextSortOrder(0x0409, version, true));
        string primary = new('a', 20000);
        Assert.True(collation.Compare(primary.Insert(9000, "-"), primary.Insert(18000, "-")) < 0);
        Assert.True(collation.Compare(primary.Insert(18000, "-"), primary.Insert(9000, "-")) > 0);
    }

    [Fact]
    public void GeneralLongKeyWithoutSuffixTable_RefusesEncoding()
        => Assert.Throws<NotSupportedException>(() => IndexKeyEncoder.EncodeTextEntry(new TextSortOrder(0x0409, 1, true), new string('é', 255)));

    [Fact]
    public void UnsupportedSortOrder_RefusesNonNullText()
        => Assert.Throws<NotSupportedException>(() => IndexKeyEncoder.EncodeTextEntry(new TextSortOrder(0x041D, 0, true), "a", true));

    private static byte[] EncodeIndexComparison(TextSortOrder order, string text)
    {
        if (!order.HasVersion)
        {
            return General97TextIndexEncoder.Encode(text, true, false);
        }

        return order.Version == 0
            ? GeneralLegacyTextIndexEncoder.Encode(text, true, false)
            : GeneralTextIndexEncoder.Encode(text, true, false);
    }
}
