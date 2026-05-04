namespace JetDatabaseWriter.Tests.Internal;

using System;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Unit tests for <see cref="IndexKeyEncoder"/>. The assertions verify
/// the encoder produces the byte sequences described in
/// <c>docs/design/index-and-relationship-format-notes.md</c> §4.3 (entry flag
/// byte) and §5 (per-type sort-key encoding), and that lexicographic byte
/// comparison of the encoded forms matches the natural numeric ordering of
/// the input values for every supported fixed-width type.
/// </summary>
public sealed class IndexKeyEncoderTests
{
    // Column type codes (mirrored from AccessBase).
    private const byte T_BYTE = 0x02;
    private const byte T_INT = 0x03;
    private const byte T_LONG = 0x04;
    private const byte T_MONEY = 0x05;
    private const byte T_FLOAT = 0x06;
    private const byte T_DOUBLE = 0x07;
    private const byte T_DATETIME = 0x08;
    private const byte T_TEXT = 0x0A;
    private const byte T_GUID = 0x0F;
    private const byte T_NUMERIC = 0x10;

    [Fact]
    public void Null_Ascending_EmitsSingleZeroFlagByte()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_LONG, value: null, ascending: true);
        Assert.Equal(new byte[] { 0x00 }, encoded);
    }

    [Fact]
    public void Null_Descending_EmitsSingleFFFlagByte()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_LONG, value: DBNull.Value, ascending: false);
        Assert.Equal(new byte[] { 0xFF }, encoded);
    }

    [Fact]
    public void Long_Zero_Ascending_TopByteFlippedToHighBitSet()
    {
        // §5: int32 → BE bytes, top byte XOR 0x80. 0 → 80 00 00 00.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_LONG, 0, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x80, 0x00, 0x00, 0x00 }, encoded);
    }

    [Fact]
    public void Long_PositiveOne_Ascending()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_LONG, 1, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x80, 0x00, 0x00, 0x01 }, encoded);
    }

    [Fact]
    public void Long_NegativeOne_Ascending()
    {
        // -1 in two's complement int32 = FF FF FF FF; top byte XOR 0x80 = 7F.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_LONG, -1, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x7F, 0xFF, 0xFF, 0xFF }, encoded);
    }

    [Fact]
    public void Long_Ordering_IsLexicographic_Ascending()
    {
        int[] values = [int.MinValue, -1000, -1, 0, 1, 1000, int.MaxValue];
        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_LONG, values[i], ascending: true);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(CompareLex(encoded[i - 1], encoded[i]) < 0, $"Ascending order violated between {values[i - 1]} and {values[i]}.");
        }
    }

    [Fact]
    public void Int_Ordering_IsLexicographic_Ascending()
    {
        short[] values = [short.MinValue, -1, 0, 1, short.MaxValue];
        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_INT, values[i], ascending: true);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(CompareLex(encoded[i - 1], encoded[i]) < 0);
        }

        // INT key length: flag(1) + 2 bytes = 3.
        Assert.All(encoded, e => Assert.Equal(3, e.Length));
    }

    [Fact]
    public void Byte_IsUnsignedAndUnflipped()
    {
        // T_BYTE is unsigned in Access — no sign-bit flip.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_BYTE, (byte)0, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x00 }, encoded);

        encoded = IndexKeyEncoder.EncodeEntry(T_BYTE, (byte)255, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0xFF }, encoded);
    }

    [Fact]
    public void Money_DecimalScaledBy10000_EncodedAsInt64()
    {
        // 1.2345 → scaled = 12345; 8 bytes BE with top byte XOR 0x80.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_MONEY, 1.2345m, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x39 }, encoded);
    }

    [Fact]
    public void Double_PositiveAndNegative_OrderCorrectly()
    {
        double[] values = [double.NegativeInfinity, -1e10, -1.0, -double.Epsilon, 0.0, double.Epsilon, 1.0, 1e10, double.PositiveInfinity];
        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_DOUBLE, values[i], ascending: true);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(CompareLex(encoded[i - 1], encoded[i]) < 0, $"Ascending order violated between {values[i - 1]} and {values[i]}.");
        }
    }

    [Fact]
    public void Float_PositiveZero_FlipsSignBit()
    {
        // +0.0f IEEE = 00 00 00 00 → BE same → flip top → 80 00 00 00.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_FLOAT, 0.0f, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x80, 0x00, 0x00, 0x00 }, encoded);
    }

    /// <summary>
    /// Focused unit-level matrix for negative <see cref="float"/> values
    /// across the full <c>(isAsc, isNeg)</c> cross-product. The fixture
    /// sweep in <c>NonTextSingleColumnIndexFixtureTests</c> hits this
    /// positionally only; this guard is a regression catch for the
    /// IEEE-twiddle (negative ⇒ ones-complement <em>every</em> byte; not
    /// just the sign bit).
    /// </summary>
    [Fact]
    public void Float_NegativeAndPositive_OrderCorrectly_AscendingAndDescending()
    {
        float[] values =
        [
            float.NegativeInfinity,
            -1e10f,
            -1.0f,
            -float.Epsilon,
            -0.0f,
            0.0f,
            float.Epsilon,
            1.0f,
            1e10f,
            float.PositiveInfinity,
        ];

        byte[][] asc = new byte[values.Length][];
        byte[][] desc = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            asc[i] = IndexKeyEncoder.EncodeEntry(T_FLOAT, values[i], ascending: true);
            desc[i] = IndexKeyEncoder.EncodeEntry(T_FLOAT, values[i], ascending: false);

            // FLOAT key length: flag(1) + 4 bytes = 5.
            Assert.Equal(5, asc[i].Length);
            Assert.Equal(5, desc[i].Length);

            // Descending must be the bitwise complement of ascending — see §5.
            for (int j = 0; j < asc[i].Length; j++)
            {
                Assert.Equal(unchecked((byte)~asc[i][j]), desc[i][j]);
            }
        }

        // Adjacent-pair monotonicity. -0.0f and +0.0f compare-equal as
        // floats but the IEEE bit pattern differs by exactly the sign bit,
        // so the encoded keys are also distinct — assert non-strict
        // ordering across that one boundary, strict elsewhere.
        for (int i = 1; i < values.Length; i++)
        {
            int cmpAsc = CompareLex(asc[i - 1], asc[i]);
            int cmpDesc = CompareLex(desc[i - 1], desc[i]);
            bool zeroBoundary = values[i - 1] == 0.0f && values[i] == 0.0f;

            if (zeroBoundary)
            {
                Assert.True(cmpAsc <= 0, $"Ascending order violated at zero boundary: {values[i - 1]} vs {values[i]}.");
                Assert.True(cmpDesc >= 0, $"Descending order violated at zero boundary: {values[i - 1]} vs {values[i]}.");
            }
            else
            {
                Assert.True(cmpAsc < 0, $"Ascending order violated: {values[i - 1]} → {values[i]}.");
                Assert.True(cmpDesc > 0, $"Descending order violated: {values[i - 1]} → {values[i]}.");
            }
        }
    }

    /// <summary>
    /// Focused unit-level matrix for negative <see cref="double"/> values
    /// across the full <c>(isAsc, isNeg)</c> cross-product. Mirrors
    /// <see cref="Float_NegativeAndPositive_OrderCorrectly_AscendingAndDescending"/>
    /// for the 8-byte IEEE encoding. Closes the §1.2 gap.
    /// </summary>
    [Fact]
    public void Double_NegativeAndPositive_OrderCorrectly_AscendingAndDescending()
    {
        double[] values =
        [
            double.NegativeInfinity,
            double.MinValue,
            -1e10,
            -1.0,
            -double.Epsilon,
            -0.0,
            0.0,
            double.Epsilon,
            1.0,
            1e10,
            double.MaxValue,
            double.PositiveInfinity,
        ];

        byte[][] asc = new byte[values.Length][];
        byte[][] desc = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            asc[i] = IndexKeyEncoder.EncodeEntry(T_DOUBLE, values[i], ascending: true);
            desc[i] = IndexKeyEncoder.EncodeEntry(T_DOUBLE, values[i], ascending: false);

            // DOUBLE key length: flag(1) + 8 bytes = 9.
            Assert.Equal(9, asc[i].Length);
            Assert.Equal(9, desc[i].Length);

            for (int j = 0; j < asc[i].Length; j++)
            {
                Assert.Equal(unchecked((byte)~asc[i][j]), desc[i][j]);
            }
        }

        for (int i = 1; i < values.Length; i++)
        {
            int cmpAsc = CompareLex(asc[i - 1], asc[i]);
            int cmpDesc = CompareLex(desc[i - 1], desc[i]);
            bool zeroBoundary = values[i - 1] == 0.0 && values[i] == 0.0;

            if (zeroBoundary)
            {
                Assert.True(cmpAsc <= 0, $"Ascending order violated at zero boundary: {values[i - 1]} vs {values[i]}.");
                Assert.True(cmpDesc >= 0, $"Descending order violated at zero boundary: {values[i - 1]} vs {values[i]}.");
            }
            else
            {
                Assert.True(cmpAsc < 0, $"Ascending order violated: {values[i - 1]} → {values[i]}.");
                Assert.True(cmpDesc > 0, $"Descending order violated: {values[i - 1]} → {values[i]}.");
            }
        }
    }

    [Fact]
    public void DateTime_EncodedAsOaDateDouble()
    {
        var dt = new DateTime(2026, 4, 24);
        double oa = dt.ToOADate();
        byte[] expected = IndexKeyEncoder.EncodeEntry(T_DOUBLE, oa, ascending: true);
        byte[] actual = IndexKeyEncoder.EncodeEntry(T_DATETIME, dt, ascending: true);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Descending_IsOnesComplementOfAscending()
    {
        byte[] asc = IndexKeyEncoder.EncodeEntry(T_LONG, 12345, ascending: true);
        byte[] desc = IndexKeyEncoder.EncodeEntry(T_LONG, 12345, ascending: false);

        Assert.Equal(asc.Length, desc.Length);
        for (int i = 0; i < asc.Length; i++)
        {
            Assert.Equal(unchecked((byte)~asc[i]), desc[i]);
        }
    }

    [Fact]
    public void Descending_Ordering_IsReverseOfAscending()
    {
        int[] values = [-100, -1, 0, 1, 100];
        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_LONG, values[i], ascending: false);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            // For descending, larger values must sort *first*.
            Assert.True(CompareLex(encoded[i - 1], encoded[i]) > 0, $"Descending order violated between {values[i - 1]} and {values[i]}.");
        }
    }

    [Theory]
    [InlineData(T_NUMERIC)]
    public void UnsupportedColumnType_Throws(byte columnType)
    {
        Assert.Throws<NotSupportedException>(() => IndexKeyEncoder.EncodeEntry(columnType, 1, ascending: true));
    }

    // ── General Legacy text encoding (full Jackcess port) ──
    //
    // Inline-byte values come from `IndexCodeTables/index_codes_genleg.txt`
    // (e.g. line 0x41 = "S4A" → 'A' encodes inline as 0x4A). Framing per
    // `GeneralLegacyTextIndexEncoder`: ascending = [flag=0x7F] + inline bytes
    // + END_TEXT(0x01) + END_EXTRA_TEXT(0x00). Descending writes the same
    // payload + END_EXTRA_TEXT, ones-complements the payload (flag stays
    // 0x80, the just-written 0x00 becomes 0xFF), then appends a fresh
    // unflipped END_EXTRA_TEXT(0x00).

    [Fact]
    public void Text_EmptyString_EmitsFlagAndFraming()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, string.Empty, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x01, 0x00 }, encoded);
    }

    [Fact]
    public void Text_EmptyString_Descending_AppendsUnflippedTrailer()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, string.Empty, ascending: false);
        Assert.Equal(new byte[] { 0x80, 0xFE, 0xFF, 0x00 }, encoded);
    }

    [Fact]
    public void Text_Digits_UseJackcessInlineCodes()
    {
        // Per Jackcess `index_codes_genleg.txt`: '0'=0x36, '1'=0x38, …, '9'=0x48.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, "0123456789", ascending: true);
        Assert.Equal(
            new byte[] { 0x7F, 0x36, 0x38, 0x3A, 0x3C, 0x3E, 0x40, 0x42, 0x44, 0x46, 0x48, 0x01, 0x00 },
            encoded);
    }

    [Fact]
    public void Text_UpperAndLowerLetters_AreCaseInsensitive()
    {
        byte[] upper = IndexKeyEncoder.EncodeEntry(T_TEXT, "ABCXYZ", ascending: true);
        byte[] lower = IndexKeyEncoder.EncodeEntry(T_TEXT, "abcxyz", ascending: true);
        Assert.Equal(upper, lower);

        // 'A'=0x4A, 'B'=0x4C, 'C'=0x4D, 'X'=0x75, 'Y'=0x76, 'Z'=0x78.
        Assert.Equal(
            new byte[] { 0x7F, 0x4A, 0x4C, 0x4D, 0x75, 0x76, 0x78, 0x01, 0x00 },
            upper);
    }

    [Fact]
    public void Text_TrailingSpacesAreStripped()
    {
        byte[] withSpaces = IndexKeyEncoder.EncodeEntry(T_TEXT, "AB   ", ascending: true);
        byte[] withoutSpaces = IndexKeyEncoder.EncodeEntry(T_TEXT, "AB", ascending: true);
        Assert.Equal(withoutSpaces, withSpaces);
    }

    [Fact]
    public void Text_LongInputIsTruncatedToMaxIndexedChars()
    {
        // VARCHAR(255) → MAX_TEXT_INDEX_CHAR_LENGTH = 127 chars.
        string longInput = new('A', 200);
        byte[] truncated = IndexKeyEncoder.EncodeEntry(T_TEXT, longInput, ascending: true);
        byte[] expected = IndexKeyEncoder.EncodeEntry(T_TEXT, new string('A', 127), ascending: true);
        Assert.Equal(expected, truncated);
    }

    [Fact]
    public void Text_Ordering_MatchesCaseInsensitiveOrdering()
    {
        // Input list is in expected ascending sort order under the General
        // Legacy encoding (digits before letters; trailing-space stripped).
        string[] values = [string.Empty, "0", "1", "9", "A", "AB", "AC", "B", "Z", "ZZ"];
        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_TEXT, values[i], ascending: true);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(
                CompareLex(encoded[i - 1], encoded[i]) < 0,
                $"Ascending order violated between '{values[i - 1]}' and '{values[i]}'.");
        }
    }

    [Fact]
    public void Text_DescendingOrdering_IsReverseOfAscending()
    {
        string[] values = ["A", "AB", "AC", "B", "Z"];
        byte[][] desc = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            desc[i] = IndexKeyEncoder.EncodeEntry(T_TEXT, values[i], ascending: false);
        }

        for (int i = 1; i < desc.Length; i++)
        {
            Assert.True(
                CompareLex(desc[i - 1], desc[i]) > 0,
                $"Descending order violated between '{values[i - 1]}' and '{values[i]}'.");
        }
    }

    /// <summary>
    /// The descending non-null start flag (<c>0x80</c>) must never collapse
    /// into the descending null marker (<c>0xFF</c>) — even when the
    /// inverted payload happens to begin with <c>0xFF</c>. The start flag
    /// is written before the inversion pass and is not flipped, so a
    /// descending non-null key always begins with exactly one <c>0x80</c>
    /// byte and is therefore unambiguously distinguishable from a
    /// single-byte <c>0xFF</c> null entry, regardless of payload contents.
    /// </summary>
    [Fact]
    public void Text_DescendingNonNull_AlwaysStartsWith0x80_NeverCollapsesToNullFlag()
    {
        // FlagDescendingNull is a single 0xFF byte. Anything else with a
        // leading 0x80 cannot be confused with it.
        byte[] descNull = IndexKeyEncoder.EncodeEntry(T_TEXT, value: DBNull.Value, ascending: false);
        Assert.Equal(new byte[] { 0xFF }, descNull);

        // Cover empty, all-spaces (trim → empty), single char, multi-char,
        // and a string long enough that the inverted payload contains at
        // least one 0xFF byte (every END_EXTRA_TEXT 0x00 in the unflipped
        // payload becomes 0xFF after inversion).
        string[] values = [string.Empty, "   ", "A", "ABC", "Hello, World!", new('Z', 50)];
        foreach (string value in values)
        {
            byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, value, ascending: false);

            Assert.True(encoded.Length >= 2, $"Descending non-null encoding of '{value}' is too short.");
            Assert.Equal(0x80, encoded[0]);
            Assert.NotEqual(descNull, encoded);
        }
    }

    [Fact]
    public void Text_Punctuation_NowEncodesViaSimpleHandler()
    {
        // Pre-full text encoder: punctuation threw. With the full Jackcess port,
        // characters like '!' (S9) encode as a SIMPLE inline byte.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, "!", ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x09, 0x01, 0x00 }, encoded);
    }

    [Fact]
    public void Text_NonAscii_UsesInternationalOrSurrogateHandler()
    {
        // Non-ASCII characters route through INTERNATIONAL / surrogate
        // handlers and no longer throw. Just assert the call succeeds and
        // produces the standard framing.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, "café", ascending: true);
        Assert.Equal(0x7F, encoded[0]);
        Assert.Equal(0x00, encoded[^1]);
        Assert.True(encoded.Length > 4, "Encoded entry should contain inline + framing bytes.");
    }

    /// <summary>
    /// Supplementary-Multilingual-Plane (SMP) characters route through the
    /// high+low surrogate handler in <c>GeneralLegacyTextIndexEncoder</c>.
    /// Synthetic round-trip rather than a fixture-table assertion, since
    /// the Jackcess fixtures do not contain SMP-plane indexed values.
    /// </summary>
    [Fact]
    public void Text_SmpPlaneCharacter_RoutesThroughSurrogateHandler()
    {
        // U+1D54F MATHEMATICAL DOUBLE-STRUCK CAPITAL X — encodes as
        // surrogate pair D835 DD4F in .NET strings; both halves take the
        // surrogate path.
        const string smpX = "\uD835\uDD4F";
        const string smpY = "\uD835\uDD50"; // U+1D550 CAPITAL Y — different SMP codepoint.

        byte[] keyX = IndexKeyEncoder.EncodeEntry(T_TEXT, smpX, ascending: true);
        byte[] keyY = IndexKeyEncoder.EncodeEntry(T_TEXT, smpY, ascending: true);
        byte[] keyAsciiX = IndexKeyEncoder.EncodeEntry(T_TEXT, "X", ascending: true);

        // Standard framing: ascending flag + END_EXTRA_TEXT terminator.
        Assert.Equal(0x7F, keyX[0]);
        Assert.Equal(0x00, keyX[^1]);

        // Surrogate handler emits extra-byte marker 0x3F per surrogate
        // half, so an SMP character contributes the byte 0x3F at least
        // once in the encoded payload (extras section).
        Assert.Contains((byte)0x3F, keyX);

        // Distinct SMP code points must yield distinct keys.
        Assert.NotEqual(keyX, keyY);

        // SMP "𝕏" must not collide with ASCII "X".
        Assert.NotEqual(keyX, keyAsciiX);

        // Descending pass must also succeed and flip the payload (start
        // flag is the descending null/non-null marker, terminator is
        // 0x00 unchanged).
        byte[] keyXDesc = IndexKeyEncoder.EncodeEntry(T_TEXT, smpX, ascending: false);
        Assert.NotEqual(keyX, keyXDesc);
        Assert.Equal(0x00, keyXDesc[^1]);
    }

    /// <summary>
    /// Right-to-left scripts (Hebrew, Arabic) and combining-diacritic
    /// NFC/NFD pairs round-trip through the encoder without throwing and
    /// produce stable keys. Documents that Access's General Legacy encoder
    /// treats NFC ("é") and NFD ("e\u0301") as equivalent — both forms
    /// encode to the <em>same</em> key bytes (the combining acute
    /// resolves to the same primary weight + extras-section diacritic as
    /// the precomposed character).
    /// </summary>
    [Fact]
    public void Text_RtlScriptsAndCombiningDiacritics_EncodeStably()
    {
        // Hebrew "shalom" — pure RTL, all in BMP.
        const string hebrew = "\u05E9\u05DC\u05D5\u05DD"; // שלום

        // Arabic "salam" — RTL with shaping.
        const string arabic = "\u0633\u0644\u0627\u0645"; // سلام

        // Pre-composed (NFC) "café" vs decomposed (NFD) "cafe\u0301".
        const string cafeNfc = "caf\u00E9";
        const string cafeNfd = "cafe\u0301";

        // Sanity: the two forms really are distinct character sequences
        // at the .NET-string level (Access's encoder is what folds them).
        Assert.NotEqual(cafeNfc, cafeNfd);
        Assert.Equal(cafeNfc, cafeNfd.Normalize(System.Text.NormalizationForm.FormC));

        // Every script encodes without throwing and emits the standard
        // ascending start flag + null terminator framing.
        foreach (string s in new[] { hebrew, arabic, cafeNfc, cafeNfd })
        {
            byte[] key = IndexKeyEncoder.EncodeEntry(T_TEXT, s, ascending: true);
            Assert.Equal(0x7F, key[0]);
            Assert.Equal(0x00, key[^1]);
            Assert.True(key.Length > 2, $"Encoded key for '{s}' must contain payload, got {key.Length} bytes.");

            // Re-encoding the same string is deterministic.
            Assert.Equal(key, IndexKeyEncoder.EncodeEntry(T_TEXT, s, ascending: true));
        }

        // Distinct scripts produce distinct keys.
        byte[] keyHebrew = IndexKeyEncoder.EncodeEntry(T_TEXT, hebrew, ascending: true);
        byte[] keyArabic = IndexKeyEncoder.EncodeEntry(T_TEXT, arabic, ascending: true);
        Assert.NotEqual(keyHebrew, keyArabic);

        // Pre-composed "é" and decomposed "e + combining-acute" encode to
        // the SAME bytes — Access's General Legacy encoder folds them via
        // the international-handler / extra-codes tables. This matches the
        // Jackcess `IndexCodesTest` behaviour and is the property that
        // lets indexed lookups work regardless of normalization form.
        byte[] keyNfc = IndexKeyEncoder.EncodeEntry(T_TEXT, cafeNfc, ascending: true);
        byte[] keyNfd = IndexKeyEncoder.EncodeEntry(T_TEXT, cafeNfd, ascending: true);
        Assert.Equal(keyNfc, keyNfd);

        // Descending pass succeeds for every script and produces a
        // different key from ascending.
        foreach (string s in new[] { hebrew, arabic, cafeNfc, cafeNfd })
        {
            byte[] asc = IndexKeyEncoder.EncodeEntry(T_TEXT, s, ascending: true);
            byte[] desc = IndexKeyEncoder.EncodeEntry(T_TEXT, s, ascending: false);
            Assert.NotEqual(asc, desc);
            Assert.Equal(0x00, desc[^1]);
        }
    }

    [Fact]
    public void Text_Null_EmitsSingleFlagByte()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, value: null, ascending: true);
        Assert.Equal(new byte[] { 0x00 }, encoded);
    }

    [Fact]
    public void Memo_RoutesThroughTheSameEncoderAsText()
    {
        const byte T_MEMO = 0x0C;
        byte[] memo = IndexKeyEncoder.EncodeEntry(T_MEMO, "Hello", ascending: true);
        byte[] text = IndexKeyEncoder.EncodeEntry(T_TEXT, "Hello", ascending: true);
        Assert.Equal(text, memo);
    }

    // ── GUID encoding via Jackcess "general binary entry" wrapping ──

    [Fact]
    public void Guid_Null_EmitsSingleFlagByte()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_GUID, value: null, ascending: true);
        Assert.Equal(new byte[] { 0x00 }, encoded);
    }

    [Fact]
    public void Guid_Ascending_HasExpectedLayout()
    {
        // GUID display bytes 00 11 22 33 44 55 66 77 88 99 AA BB CC DD EE FF.
        // Storage layout (Guid.ToByteArray): 33 22 11 00 55 44 77 66 88..FF.
        var g = Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF");
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_GUID, g, ascending: true);

        Assert.Equal(19, encoded.Length);
        Assert.Equal(0x7F, encoded[0]); // ascending flag
        Assert.Equal(0x09, encoded[9]); // intermediate length byte
        Assert.Equal(0x08, encoded[18]); // final length byte (8 valid bytes)

        Assert.Equal(
            new byte[]
            {
                0x7F,
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                0x09,
                0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
                0x08,
            },
            encoded);
    }

    [Fact]
    public void Guid_Descending_FlipsDataAndFinalLengthButNotIntermediate()
    {
        var g = Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF");
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_GUID, g, ascending: false);

        Assert.Equal(19, encoded.Length);
        Assert.Equal(0x80, encoded[0]); // descending flag (NOT 0x80 = ~0x7F via post-flip — emitted directly)
        Assert.Equal(0x09, encoded[9]); // intermediate length byte stays unflipped
        Assert.Equal((byte)0xF7, encoded[18]); // final length byte IS flipped → ~0x08 = 0xF7

        // Data bytes are bit-flipped.
        Assert.Equal(unchecked((byte)~0x00), encoded[1]);
        Assert.Equal(unchecked((byte)~0x11), encoded[2]);
        Assert.Equal(unchecked((byte)~0xFF), encoded[17]);
    }

    [Fact]
    public void Guid_Ordering_IsLexicographic_Ascending()
    {
        Guid[] values =
        [
            Guid.Parse("00000000-0000-0000-0000-000000000000"),
            Guid.Parse("00000000-0000-0000-0000-000000000001"),
            Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF"),
            Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFE"),
            Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
        ];

        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_GUID, values[i], ascending: true);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(
                CompareLex(encoded[i - 1], encoded[i]) < 0,
                $"Ascending order violated between {values[i - 1]} and {values[i]}.");
        }
    }

    [Fact]
    public void Guid_Ordering_IsReverseOfAscending_Descending()
    {
        Guid[] values =
        [
            Guid.Parse("00000000-0000-0000-0000-000000000000"),
            Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF"),
            Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
        ];

        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_GUID, values[i], ascending: false);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(
                CompareLex(encoded[i - 1], encoded[i]) > 0,
                $"Descending order violated between {values[i - 1]} and {values[i]}.");
        }
    }

    [Fact]
    public void Guid_AcceptsStringInput()
    {
        var g = Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF");
        byte[] fromGuid = IndexKeyEncoder.EncodeEntry(T_GUID, g, ascending: true);
        byte[] fromString = IndexKeyEncoder.EncodeEntry(T_GUID, "00112233-4455-6677-8899-AABBCCDDEEFF", ascending: true);
        Assert.Equal(fromGuid, fromString);
    }

    // ── T_BINARY (variable raw bytes) ────────────────────────────
    //
    // Same general-binary-entry packing as T_GUID: the data is split into
    // 8-byte zero-padded segments, each followed by a length byte (0x09 for
    // intermediates, the actual valid count for the final segment). On
    // descending the data bytes and the FINAL length byte flip; intermediate
    // 0x09 length bytes stay unflipped.

    private const byte T_BINARY = 0x09;

    private static readonly byte[] BinaryEmpty = [];
    private static readonly byte[] BinaryThree = [0x01, 0x02, 0x03];
    private static readonly byte[] BinaryEight = [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80];
    private static readonly byte[] BinaryNine = [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90];

    [Fact]
    public void Binary_Null_EmitsSingleFlagByte()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_BINARY, value: null, ascending: true);
        Assert.Equal(new byte[] { 0x00 }, encoded);
    }

    [Fact]
    public void Binary_Empty_EmitsSingleZeroPaddedSegment()
    {
        // Empty binary still emits one final segment so two empty values
        // compare equal and sort below any non-empty value.
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_BINARY, BinaryEmpty, ascending: true);
        Assert.Equal(
            new byte[] { 0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
            encoded);
    }

    [Fact]
    public void Binary_ThreeBytes_ZeroPadsToEightAndFinalLengthIsThree()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_BINARY, BinaryThree, ascending: true);
        Assert.Equal(
            new byte[] { 0x7F, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03 },
            encoded);
    }

    [Fact]
    public void Binary_ExactlyEightBytes_SingleSegmentWithFinalLengthEight()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_BINARY, BinaryEight, ascending: true);
        Assert.Equal(
            new byte[] { 0x7F, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x08 },
            encoded);
    }

    [Fact]
    public void Binary_NineBytes_TwoSegmentsWithIntermediateLengthThenFinalOne()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_BINARY, BinaryNine, ascending: true);
        Assert.Equal(
            new byte[]
            {
                0x7F,
                0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80,
                0x09,
                0x90, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x01,
            },
            encoded);
    }

    [Fact]
    public void Binary_Descending_FlipsDataAndFinalLengthButNotIntermediate()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_BINARY, BinaryNine, ascending: false);
        Assert.Equal(0x80, encoded[0]); // descending non-null flag
        Assert.Equal(0x09, encoded[9]); // intermediate length stays unflipped

        // Data bytes flipped.
        Assert.Equal(unchecked((byte)~0x10), encoded[1]);
        Assert.Equal(unchecked((byte)~0x90), encoded[10]);

        // Final length byte (0x01) flipped to 0xFE.
        Assert.Equal((byte)0xFE, encoded[18]);
    }

    [Fact]
    public void Binary_Ordering_IsLexicographic_Ascending()
    {
        byte[][] values =
        [
            BinaryEmpty,
            [0x00],
            [0x00, 0x00],
            BinaryThree,
            [0x01, 0x02, 0x04],
            BinaryEight,
            BinaryNine,
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        ];

        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_BINARY, values[i], ascending: true);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(
                CompareLex(encoded[i - 1], encoded[i]) < 0,
                $"Ascending order violated between binary index {i - 1} and {i}.");
        }
    }

    [Fact]
    public void Binary_Ordering_IsReverseOfAscending_Descending()
    {
        byte[][] values =
        [
            BinaryEmpty,
            BinaryThree,
            BinaryEight,
            BinaryNine,
        ];

        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeEntry(T_BINARY, values[i], ascending: false);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(
                CompareLex(encoded[i - 1], encoded[i]) > 0,
                $"Descending order violated between binary index {i - 1} and {i}.");
        }
    }

    [Fact]
    public void Binary_RejectsNonByteArrayValue()
    {
        Assert.Throws<ArgumentException>(() => IndexKeyEncoder.EncodeEntry(T_BINARY, "not bytes", ascending: true));
    }

    // ── T_NUMERIC (Decimal) ────────────────────────────────────────

    [Fact]
    public void Numeric_Null_Ascending_EmitsSingleZeroFlagByte()
    {
        byte[] encoded = IndexKeyEncoder.EncodeNumericEntry(value: null, ascending: true, targetScale: 0, legacy: false);
        Assert.Equal(new byte[] { 0x00 }, encoded);
    }

    [Fact]
    public void Numeric_Zero_Ascending_NewStyle_HasFFSignByteAndZeroMantissa()
    {
        // New-style: byte 0 = 0xFF; pos+asc → no flip → result FF + 16 zero bytes.
        byte[] encoded = IndexKeyEncoder.EncodeNumericEntry(0m, ascending: true, targetScale: 0, legacy: false);
        Assert.Equal(18, encoded.Length);
        Assert.Equal(0x7F, encoded[0]); // ascending non-null flag.
        Assert.Equal(0xFF, encoded[1]);
        for (int i = 2; i < 18; i++)
        {
            Assert.Equal(0x00, encoded[i]);
        }
    }

    [Fact]
    public void Numeric_PositiveOne_Ascending_NewStyle_MantissaIsBigEndianOne()
    {
        byte[] encoded = IndexKeyEncoder.EncodeNumericEntry(1m, ascending: true, targetScale: 0, legacy: false);
        Assert.Equal(0x7F, encoded[0]);
        Assert.Equal(0xFF, encoded[1]);

        // 16-byte BE mantissa = ...00 01.
        for (int i = 2; i < 17; i++)
        {
            Assert.Equal(0x00, encoded[i]);
        }

        Assert.Equal(0x01, encoded[17]);
    }

    [Fact]
    public void Numeric_NegativeOne_Ascending_NewStyle_FlipsAllSeventeenBytes()
    {
        // New-style + neg + asc: byte0=0xFF then flip all 17 → 00 + 0xFF...0xFE.
        byte[] encoded = IndexKeyEncoder.EncodeNumericEntry(-1m, ascending: true, targetScale: 0, legacy: false);
        Assert.Equal(0x7F, encoded[0]);
        Assert.Equal(0x00, encoded[1]);
        for (int i = 2; i < 17; i++)
        {
            Assert.Equal(0xFF, encoded[i]);
        }

        Assert.Equal(0xFE, encoded[17]);
    }

    [Fact]
    public void Numeric_PositiveOne_Ascending_Legacy_MatchesNewStyle()
    {
        // For (asc, pos) the legacy and new-style outputs are identical.
        byte[] newStyle = IndexKeyEncoder.EncodeNumericEntry(1m, ascending: true, targetScale: 0, legacy: false);
        byte[] legacy = IndexKeyEncoder.EncodeNumericEntry(1m, ascending: true, targetScale: 0, legacy: true);
        Assert.Equal(newStyle, legacy);
    }

    [Fact]
    public void Numeric_NegativeOne_Descending_Legacy_DiffersFromNewStyle()
    {
        // (desc, neg) is the case where legacy and new-style diverge:
        // legacy → 00 + raw mantissa; new-style → FF + raw mantissa.
        byte[] legacy = IndexKeyEncoder.EncodeNumericEntry(-1m, ascending: false, targetScale: 0, legacy: true);
        byte[] newStyle = IndexKeyEncoder.EncodeNumericEntry(-1m, ascending: false, targetScale: 0, legacy: false);
        Assert.Equal(0x00, legacy[1]);
        Assert.Equal(0xFF, newStyle[1]);
    }

    [Fact]
    public void Numeric_Ascending_Ordering_IsLexicographic_NewStyle()
    {
        decimal[] values = [-1000m, -1.5m, -1m, -0.01m, 0m, 0.01m, 1m, 1.5m, 1000m];
        const int targetScale = 2;
        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeNumericEntry(values[i], ascending: true, targetScale, legacy: false);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(
                CompareLex(encoded[i - 1], encoded[i]) < 0,
                $"Ascending order violated between {values[i - 1]} and {values[i]}.");
        }
    }

    [Fact]
    public void Numeric_Descending_Ordering_IsReverseLexicographic_NewStyle()
    {
        decimal[] values = [-1000m, -1m, 0m, 1m, 1000m];
        const int targetScale = 0;
        byte[][] encoded = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            encoded[i] = IndexKeyEncoder.EncodeNumericEntry(values[i], ascending: false, targetScale, legacy: false);
        }

        for (int i = 1; i < encoded.Length; i++)
        {
            Assert.True(
                CompareLex(encoded[i - 1], encoded[i]) > 0,
                $"Descending order violated between {values[i - 1]} and {values[i]}.");
        }
    }

    [Fact]
    public void Numeric_Descending_Ordering_SameSign_IsReverseLexicographic_Legacy()
    {
        // Legacy fixed-point indexes have a known cross-sign ordering bug (MS KB
        // 837148; cited by Jackcess) — descending leaves negatives before
        // positives in lex order regardless of magnitude. Validate strict
        // descending order WITHIN a single sign instead.
        decimal[] positives = [1m, 10m, 1000m];
        byte[][] encPos = new byte[positives.Length][];
        for (int i = 0; i < positives.Length; i++)
        {
            encPos[i] = IndexKeyEncoder.EncodeNumericEntry(positives[i], ascending: false, targetScale: 0, legacy: true);
        }

        for (int i = 1; i < encPos.Length; i++)
        {
            Assert.True(
                CompareLex(encPos[i - 1], encPos[i]) > 0,
                $"Legacy descending order violated between {positives[i - 1]} and {positives[i]}.");
        }
    }

    [Fact]
    public void Numeric_TargetScaleNormalization_OneEqualsOnePointZeroZero()
    {
        // 1 and 1.00 represent the same numeric value; encoded with the same
        // target scale they must produce identical byte sequences.
        byte[] one = IndexKeyEncoder.EncodeNumericEntry(1m, ascending: true, targetScale: 2, legacy: false);
        byte[] oneHundredHundredths = IndexKeyEncoder.EncodeNumericEntry(1.00m, ascending: true, targetScale: 2, legacy: false);
        Assert.Equal(one, oneHundredHundredths);
    }

    [Fact]
    public void Numeric_TargetScaleSmallerThanNatural_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            IndexKeyEncoder.EncodeNumericEntry(1.50m, ascending: true, targetScale: 0, legacy: false));
    }

    [Fact]
    public void Numeric_OverflowsSixteenByteMantissa_Throws()
    {
        // BigInteger pow: 10^28 = 32 digits ≈ 13 bytes; multiply by ~10^10 mantissa
        // pushes it beyond 16 bytes after rescaling to scale 28.
        // 79228162514264337593543950335 (29 digits, scale 0).
        // Re-encoding at scale 28 needs 29+28 = 57 digits, well beyond 128 bits.
        decimal big = decimal.MaxValue;
        Assert.Throws<NotSupportedException>(() =>
            IndexKeyEncoder.EncodeNumericEntry(big, ascending: true, targetScale: 28, legacy: false));
    }

    [Fact]
    public void Numeric_ComputeMaxNumericScale_ReturnsHighestScaleAcrossValues()
    {
        var values = new object?[] { 1m, 1.5m, null, 1.50m, 0.001m, DBNull.Value };
        Assert.Equal(3, IndexKeyEncoder.ComputeMaxNumericScale(values));
    }

    [Fact]
    public void Numeric_AcceptsBoxedNumericTypes()
    {
        // Integer and double inputs route through the existing ToDecimal helper.
        byte[] fromDecimal = IndexKeyEncoder.EncodeNumericEntry(42m, ascending: true, targetScale: 0, legacy: false);
        byte[] fromInt = IndexKeyEncoder.EncodeNumericEntry(42, ascending: true, targetScale: 0, legacy: false);
        byte[] fromDouble = IndexKeyEncoder.EncodeNumericEntry(42.0, ascending: true, targetScale: 0, legacy: false);
        Assert.Equal(fromDecimal, fromInt);
        Assert.Equal(fromDecimal, fromDouble);
    }

    /// <summary>
    /// Cross-format isolation of the NUMERIC sort-key sign byte
    /// (payload byte 0, i.e. encoded byte 1 after the entry-flag prefix).
    /// Closes the §1.2 test-coverage gap entry "Per-format isolated assertion
    /// of <c>LegacyFixedPointColumnDescriptor</c> vs
    /// <c>FixedPointColumnDescriptor</c> sign-byte handling": the existing
    /// <c>Numeric_*</c> facts cover four of the eight (asc/desc × pos/neg ×
    /// legacy/new-style) combinations positionally, but no single assertion
    /// pins down the sign-byte branch across the full grid.
    /// <para>
    /// Sign-byte derivation per <see cref="IndexKeyEncoder.EncodeNumericEntry"/>:
    /// </para>
    /// <list type="bullet">
    /// <item><description><b>Legacy</b> (Jet4 .mdb): if (negative == ascending)
    /// flip all 17 bytes; THEN byte 0 = 0x00 (negative) or 0xFF (positive).
    /// The overwrite happens AFTER the flip, so the sign byte is a function of
    /// the <em>sign alone</em> in legacy form (positive → 0xFF, negative → 0x00).
    /// This is the documented MS KB 837148 / Jackcess
    /// <c>LegacyFixedPointColumnDescriptor</c> quirk that breaks cross-sign
    /// descending order on Jet4 .mdb.</description></item>
    /// <item><description><b>New-style</b> (ACCDB / ACE): byte 0 = 0xFF; THEN
    /// if (negative == ascending) flip all 17 bytes. The flip happens AFTER the
    /// sign-byte stamp, so the sign byte is 0xFF when not flipped (when
    /// negative != ascending) and 0x00 when flipped (when negative == ascending).
    /// </description></item>
    /// </list>
    /// </summary>
    [Theory]
    [InlineData(true, false, true, 0xFF)] // asc, pos, legacy -> 0xFF (positive sign post-overwrite)
    [InlineData(true, true, true, 0x00)] // asc, neg, legacy -> 0x00 (negative sign post-overwrite)
    [InlineData(false, false, true, 0xFF)] // desc, pos, legacy -> 0xFF
    [InlineData(false, true, true, 0x00)] // desc, neg, legacy -> 0x00
    [InlineData(true, false, false, 0xFF)] // asc, pos, new-style -> 0xFF (no flip; neg != asc)
    [InlineData(true, true, false, 0x00)] // asc, neg, new-style -> 0xFF then flipped (neg == asc)
    [InlineData(false, false, false, 0x00)] // desc, pos, new-style -> 0xFF then flipped (neg == asc, both false)
    [InlineData(false, true, false, 0xFF)] // desc, neg, new-style -> 0xFF (no flip; neg != asc)
    public void Numeric_SignByte_IsolatedAcrossFormatAndDirection(bool ascending, bool negative, bool legacy, int expectedSignByte)
    {
        decimal value = negative ? -1m : 1m;
        byte[] encoded = IndexKeyEncoder.EncodeNumericEntry(value, ascending, targetScale: 0, legacy);

        // Layout: encoded[0] = entry-flag (0x7F asc / 0x80 desc); encoded[1..17] = 17-byte payload.
        Assert.Equal(18, encoded.Length);
        Assert.Equal(ascending ? (byte)0x7F : (byte)0x80, encoded[0]);
        Assert.Equal((byte)expectedSignByte, encoded[1]);
    }

    /// <summary>
    /// Isolated sign-byte verification for <c>T_MONEY</c> across all four
    /// <c>asc/desc × pos/neg</c> outcomes. Money uses
    /// <c>EncodeSignedBigEndian(scaled, 8)</c> which writes the scaled int64
    /// in big-endian order and XORs the top byte with <c>0x80</c>. The flag
    /// byte precedes the 8-byte payload, so <c>encoded[1]</c> carries the
    /// sign information: non-negative scaled values (top BE byte &lt; 0x80
    /// before XOR) become <c>≥ 0x80</c>, and negative values (top BE byte
    /// ≥ 0x80 before XOR) become <c>&lt; 0x80</c>. Descending is the
    /// ones-complement of ascending, so the sign byte flips accordingly.
    /// </summary>
    [Theory]
    [InlineData(true, false, 0x80)] // asc, pos: scaled=10000, top BE byte=0x00, XOR 0x80 → 0x80
    [InlineData(true, true, 0x7F)] // asc, neg: scaled=-10000, top BE byte=0xFF, XOR 0x80 → 0x7F
    [InlineData(false, false, 0x7F)] // desc, pos: ones-complement of 0x80 → 0x7F
    [InlineData(false, true, 0x80)] // desc, neg: ones-complement of 0x7F → 0x80
    public void Money_SignByte_IsolatedAcrossDirection(bool ascending, bool negative, int expectedSignByte)
    {
        decimal value = negative ? -1m : 1m;
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_MONEY, value, ascending);

        // Layout: encoded[0] = entry-flag (0x7F asc / 0x80 desc); encoded[1..8] = 8-byte payload.
        Assert.Equal(9, encoded.Length);
        Assert.Equal(ascending ? (byte)0x7F : (byte)0x80, encoded[0]);
        Assert.Equal((byte)expectedSignByte, encoded[1]);
    }

    /// <summary>
    /// Full ascending + descending ordering matrix for <c>T_MONEY</c>,
    /// mirroring <see cref="Float_NegativeAndPositive_OrderCorrectly_AscendingAndDescending"/>
    /// and <see cref="Double_NegativeAndPositive_OrderCorrectly_AscendingAndDescending"/>.
    /// Closes the §1.2 coverage gap: the fixture sweep covers Money
    /// positionally only; this test isolates the encoding correctness
    /// across the full value range including negative boundary values.
    /// </summary>
    [Fact]
    public void Money_NegativeAndPositive_OrderCorrectly_AscendingAndDescending()
    {
        // OACurrency range: ±922,337,203,685,477.5807
        decimal[] values =
        [
            -922337203685477.5808m,
            -1000m,
            -1m,
            -0.0001m,
            0m,
            0.0001m,
            1m,
            1000m,
            922337203685477.5807m,
        ];

        byte[][] asc = new byte[values.Length][];
        byte[][] desc = new byte[values.Length][];
        for (int i = 0; i < values.Length; i++)
        {
            asc[i] = IndexKeyEncoder.EncodeEntry(T_MONEY, values[i], ascending: true);
            desc[i] = IndexKeyEncoder.EncodeEntry(T_MONEY, values[i], ascending: false);

            // MONEY key length: flag(1) + 8 bytes = 9.
            Assert.Equal(9, asc[i].Length);
            Assert.Equal(9, desc[i].Length);

            // Descending must be the bitwise complement of ascending.
            for (int j = 0; j < asc[i].Length; j++)
            {
                Assert.Equal(unchecked((byte)~asc[i][j]), desc[i][j]);
            }
        }

        for (int i = 1; i < values.Length; i++)
        {
            int cmpAsc = CompareLex(asc[i - 1], asc[i]);
            int cmpDesc = CompareLex(desc[i - 1], desc[i]);

            Assert.True(cmpAsc < 0, $"Ascending order violated: {values[i - 1]} → {values[i]}.");
            Assert.True(cmpDesc > 0, $"Descending order violated: {values[i - 1]} → {values[i]}.");
        }
    }

    private static int CompareLex(byte[] a, byte[] b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int c = a[i].CompareTo(b[i]);
            if (c != 0)
            {
                return c;
            }
        }

        return a.Length.CompareTo(b.Length);
    }
}
