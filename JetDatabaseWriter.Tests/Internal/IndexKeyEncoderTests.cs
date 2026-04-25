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
    [InlineData(T_GUID)]
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
