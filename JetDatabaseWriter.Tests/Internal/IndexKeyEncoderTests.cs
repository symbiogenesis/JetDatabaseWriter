namespace JetDatabaseWriter.Tests;

using System;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Unit tests for <see cref="IndexKeyEncoder"/> (W2). The assertions verify
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
        int[] values = { int.MinValue, -1000, -1, 0, 1, 1000, int.MaxValue };
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
        short[] values = { short.MinValue, -1, 0, 1, short.MaxValue };
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
        double[] values = { double.NegativeInfinity, -1e10, -1.0, -double.Epsilon, 0.0, double.Epsilon, 1.0, 1e10, double.PositiveInfinity };
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
        int[] values = { -100, -1, 0, 1, 100 };
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

    // ── W7: General Legacy text encoding (digits + ASCII letters only) ──

    [Fact]
    public void Text_EmptyString_EmitsFlagAndTerminator()
    {
        // §5: text key terminator is 0x00 (or 0xFF when negated for descending).
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, string.Empty, ascending: true);
        Assert.Equal(new byte[] { 0x7F, 0x00 }, encoded);
    }

    [Fact]
    public void Text_Digits_MapTo56Through5F()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, "0123456789", ascending: true);
        Assert.Equal(
            new byte[] { 0x7F, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F, 0x00 },
            encoded);
    }

    [Fact]
    public void Text_UpperAndLowerLetters_MapToSameRange60Through79()
    {
        // Case-insensitive per HACKING.md §5.
        byte[] upper = IndexKeyEncoder.EncodeEntry(T_TEXT, "ABCXYZ", ascending: true);
        byte[] lower = IndexKeyEncoder.EncodeEntry(T_TEXT, "abcxyz", ascending: true);
        Assert.Equal(upper, lower);
        Assert.Equal(
            new byte[] { 0x7F, 0x60, 0x61, 0x62, 0x77, 0x78, 0x79, 0x00 },
            upper);
    }

    [Fact]
    public void Text_Ordering_DigitsSortBeforeLetters()
    {
        string[] values = { string.Empty, "0", "1", "9", "A", "AB", "AC", "B", "Z", "ZZ" };
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
    public void Text_Descending_IsOnesComplementOfAscending()
    {
        byte[] asc = IndexKeyEncoder.EncodeEntry(T_TEXT, "AB12", ascending: true);
        byte[] desc = IndexKeyEncoder.EncodeEntry(T_TEXT, "AB12", ascending: false);
        Assert.Equal(asc.Length, desc.Length);
        for (int i = 0; i < asc.Length; i++)
        {
            Assert.Equal(unchecked((byte)~asc[i]), desc[i]);
        }

        // Terminator becomes 0xFF after ones-complement.
        Assert.Equal(0xFF, desc[^1]);
    }

    [Fact]
    public void Text_Null_EmitsSingleFlagByte()
    {
        byte[] encoded = IndexKeyEncoder.EncodeEntry(T_TEXT, value: null, ascending: true);
        Assert.Equal(new byte[] { 0x00 }, encoded);
    }

    [Theory]
    [InlineData(" ")] // space
    [InlineData("a b")] // contains space
    [InlineData("hello!")] // punctuation
    [InlineData("caf\u00E9")] // non-ASCII (é)
    [InlineData("_underscore")]
    public void Text_UnsupportedCharacter_Throws(string value)
    {
        Assert.Throws<NotSupportedException>(() => IndexKeyEncoder.EncodeEntry(T_TEXT, value, ascending: true));
    }

    // ── W12: GUID encoding via Jackcess "general binary entry" wrapping ──

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
        {
            Guid.Parse("00000000-0000-0000-0000-000000000000"),
            Guid.Parse("00000000-0000-0000-0000-000000000001"),
            Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF"),
            Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFE"),
            Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
        };

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
        {
            Guid.Parse("00000000-0000-0000-0000-000000000000"),
            Guid.Parse("00112233-4455-6677-8899-AABBCCDDEEFF"),
            Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
        };

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

    // ── W13: T_NUMERIC (Decimal) ────────────────────────────────────────

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
        decimal[] values = { -1000m, -1.5m, -1m, -0.01m, 0m, 0.01m, 1m, 1.5m, 1000m };
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
        decimal[] values = { -1000m, -1m, 0m, 1m, 1000m };
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
        decimal[] positives = { 1m, 10m, 1000m };
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
