namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Globalization;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Pins the contract for <see cref="JetTypeInfo.ReadFixedTyped(ReadOnlySpan{byte}, int, ColumnType, int, bool)"/>: the typed
/// fixed-width decode that powers the typed-row read path. Each test verifies
/// parity with the legacy <see cref="JetTypeInfo.ReadFixedString(ReadOnlySpan{byte}, int, ColumnType, int, bool)"/> +
/// <see cref="TypedValueParser.ParseValue"/> round-trip the typed reader is
/// replacing — except where the round-trip is documented as lossy (sub-second
/// DateTime precision), in which case the typed path is asserted to keep
/// the un-truncated value while the round-trip drops it.
/// </summary>
public sealed class JetTypeInfoReadFixedTypedTests
{
    [Theory]
    [InlineData(ByteType, 1)]
    [InlineData(IntegerType, 2)]
    [InlineData(LongIntegerType, 4)]
    [InlineData(MoneyType, 8)]
    [InlineData(FloatType, 4)]
    [InlineData(DoubleType, 8)]
    [InlineData(DateTimeType, 8)]
    [InlineData(BigIntType, 8)]
    [InlineData(NumericType, 17)]
    [InlineData(GuidType, 16)]
    [InlineData(DateTimeExtendedType, 42)]
    [InlineData(ComplexType, 4)]
    [InlineData(AttachmentType, 4)]
    public void TryGetVariableSlotFixedPayloadSize_FixedPayloadTypes_ReturnsRequiredSize(ColumnType columnType, int expectedSize)
    {
        Assert.True(JetTypeInfo.TryGetVariableSlotFixedPayloadSize(columnType, out int actualSize));
        Assert.Equal(expectedSize, actualSize);
    }

    [Theory]
    [InlineData(BooleanType)]
    [InlineData(BinaryType)]
    [InlineData(TextType)]
    [InlineData(OleType)]
    [InlineData(MemoType)]
    public void TryGetVariableSlotFixedPayloadSize_NonFixedPayloadTypes_ReturnsFalse(ColumnType columnType)
    {
        Assert.False(JetTypeInfo.TryGetVariableSlotFixedPayloadSize(columnType, out int actualSize));
        Assert.Equal(0, actualSize);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(127)]
    [InlineData(255)]
    public void Byte_RoundTripsThroughParseValue(byte value)
    {
        byte[] row = [value];
        AssertParity(row, start: 0, ByteType, size: 1, expected: value);
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1)]
    [InlineData(short.MaxValue)]
    public void Int_NonNegative_RoundTripsThroughParseValue(short value)
    {
        byte[] row = new byte[2];
        BinaryPrimitives.WriteInt16LittleEndian(row, value);
        AssertParity(row, start: 0, IntegerType, size: 2, expected: value);
    }

    /// <summary>
    /// Negative shorts should decode losslessly through both paths.
    /// This verifies the legacy string formatter reads Integer as signed
    /// little-endian rather than unsigned+cast under checked arithmetic.
    /// </summary>
    /// <param name="value">The value.</param>
    [Theory]
    [InlineData((short)-1)]
    [InlineData(short.MinValue)]
    public void Int_Negative_RoundTripsThroughParseValue(short value)
    {
        byte[] row = new byte[2];
        BinaryPrimitives.WriteInt16LittleEndian(row, value);

        AssertParity(row, start: 0, IntegerType, size: 2, expected: value);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Long_RoundTripsThroughParseValue(int value)
    {
        byte[] row = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(row, value);
        AssertParity(row, start: 0, LongIntegerType, size: 4, expected: value);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(1L)]
    [InlineData(-1L)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void BigInt_RoundTripsThroughParseValue(long value)
    {
        byte[] row = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(row, value);
        AssertParity(row, start: 0, BigIntType, size: 8, expected: value);
    }

    [Theory]
    [InlineData(0f)]
    [InlineData(1f)]
    [InlineData(-1.5f)]
    [InlineData(3.14159f)]
    public void Float_RoundTripsThroughParseValue(float value)
    {
        byte[] row = new byte[4];
        BinaryPrimitives.WriteSingleLittleEndian(row, value);
        AssertParity(row, start: 0, FloatType, size: 4, expected: value);
    }

    [Theory]
    [InlineData(0d)]
    [InlineData(1d)]
    [InlineData(-1.5d)]
    [InlineData(3.141592653589793d)]
    public void Double_RoundTripsThroughParseValue(double value)
    {
        byte[] row = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(row, value);
        AssertParity(row, start: 0, DoubleType, size: 8, expected: value);
    }

    /// <summary>
    /// DateTime values whose seconds line up exactly survive the
    /// <c>"yyyy-MM-dd HH:mm:ss"</c> round-trip, so parity with the legacy path
    /// must hold here.
    /// </summary>
    /// <param name="isoText">ISO-formatted date/time text.</param>
    [Theory]
    [InlineData("1899-12-30 00:00:00")] // OADate epoch
    [InlineData("1900-01-01 00:00:00")]
    [InlineData("1970-01-01 00:00:00")]
    [InlineData("2026-05-02 12:34:56")]
    [InlineData("9999-12-31 23:59:59")] // upper edge that round-trips losslessly
    public void DateTime_SecondPrecision_RoundTripsThroughParseValue(string isoText)
    {
        var dt = DateTime.ParseExact(isoText, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        byte[] row = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(row, dt.ToOADate());

        AssertParity(row, start: 0, DateTimeType, size: 8, expected: dt);
    }

    /// <summary>
    /// Documents that the typed path keeps sub-second precision the legacy
    /// string round-trip drops. This is an intentional improvement, not a
    /// regression.
    /// </summary>
    [Fact]
    public void DateTime_SubSecondPrecision_TypedKeepsItRoundTripDoesNot()
    {
        var dt = new DateTime(2026, 5, 2, 12, 34, 56, 789, DateTimeKind.Unspecified);
        byte[] row = new byte[8];
        BinaryPrimitives.WriteDoubleLittleEndian(row, dt.ToOADate());

        object typed = JetTypeInfo.ReadFixedTyped(row, start: 0, DateTimeType, size: 8);
        DateTime typedDt = Assert.IsType<DateTime>(typed);

        // Round-trip via OADate has its own quantization, but it preserves
        // sub-second information that the "yyyy-MM-dd HH:mm:ss" format strips.
        Assert.NotEqual(0, typedDt.Millisecond);

        string formatted = JetTypeInfo.ReadFixedString(row, start: 0, DateTimeType, size: 8);
        var roundTripped = (DateTime)TypedValueParser.ParseValue(formatted, typeof(DateTime));
        Assert.Equal(0, roundTripped.Millisecond);
    }

    /// <summary>
    /// Date/Time Extended stores seven fractional-second digits, which map
    /// directly to .NET DateTime ticks.
    /// </summary>
    [Fact]
    public void DateTimeExtended_TickPrecision_RoundTripsThroughParseValue()
    {
        DateTime expected = new DateTime(2021, 6, 14, 22, 45, 12, 345, DateTimeKind.Unspecified).AddTicks(6789);
        byte[] row = new byte[42];
        JetTypeInfo.WriteDateTimeExtended(row, expected);

        AssertParity(row, start: 0, DateTimeExtendedType, size: 42, expected);
    }

    /// <summary>
    /// Money is stored as an OACurrency int64 with implicit scale=4. Verify
    /// the typed path returns the same decimal the round-trip parses.
    /// </summary>
    /// <param name="oaCurrency">The oa currency.</param>
    /// <param name="expectedDecimal">The expected decimal.</param>
    [Theory]
    [InlineData(0L, "0.0000")]
    [InlineData(12345L, "1.2345")]
    [InlineData(-12345L, "-1.2345")]
    [InlineData(99999999999999L, "9999999999.9999")]

    // OACurrency boundary regression: the int64 range corresponds to
    // ±922,337,203,685,477.5807. Verify these survive the decimal
    // conversion without rounding (mdbtools / older ODBC paths
    // historically clipped the high bits here).
    [InlineData(long.MaxValue, "922337203685477.5807")]
    [InlineData(long.MinValue + 1, "-922337203685477.5807")]
    [InlineData(long.MinValue, "-922337203685477.5808")]
    public void Money_Scale4_RoundTripsThroughParseValue(long oaCurrency, string expectedDecimal)
    {
        byte[] row = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(row, oaCurrency);
        decimal expected = decimal.Parse(expectedDecimal, CultureInfo.InvariantCulture);

        AssertParity(row, start: 0, MoneyType, size: 8, expected: expected);
    }

    [Fact]
    public void Guid_RoundTripsThroughParseValue()
    {
        var expected = Guid.Parse("12345678-9abc-def0-1234-56789abcdef0");
        byte[] row = expected.ToByteArray();

        AssertParity(row, start: 0, GuidType, size: 16, expected: expected);
    }

    /// <summary>
    /// Numeric (Numeric) values inside the .NET decimal range round-trip
    /// through the legacy string path; the typed path must agree.
    /// </summary>
    /// <param name="lo">The lower byte.</param>
    /// <param name="mid">The middle byte.</param>
    /// <param name="hi">The upper byte.</param>
    /// <param name="negative">The negative.</param>
    /// <param name="scale">The scale.</param>
    /// <param name="expectedDecimal">The expected decimal.</param>
    [Theory]
    [InlineData(0u, 0u, 0u, false, (byte)0, "0")]
    [InlineData(12345u, 0u, 0u, false, (byte)0, "12345")]
    [InlineData(12345u, 0u, 0u, true, (byte)0, "-12345")]
    [InlineData(12345u, 0u, 0u, false, (byte)4, "1.2345")]
    public void Numeric_InRange_RoundTripsThroughParseValue(uint lo, uint mid, uint hi, bool negative, byte scale, string expectedDecimal)
    {
        byte[] row = BuildNumericRow(lo, mid, hi, negative);
        decimal expected = decimal.Parse(expectedDecimal, CultureInfo.InvariantCulture);

        AssertNumericParity(row, scale, expected, strictNumeric: true);
    }

    /// <summary>
    /// Decimal values whose mantissa words have the high bit set (for example
    /// <see cref="decimal.MaxValue"/> with all-ones lo/mid/hi) must preserve the
    /// raw 96-bit bit pattern through both the typed and string decode paths.
    /// </summary>
    [Fact]
    public void Numeric_DecimalMaxValue_RoundTripsThroughParseValue()
    {
        byte[] row = BuildNumericRow(lo: 0xFFFFFFFFu, mid: 0xFFFFFFFFu, hi: 0xFFFFFFFFu, negative: false);

        AssertNumericParity(row, scale: 0, decimal.MaxValue);
    }

    [Fact]
    public void Numeric_StrictMode_ScaleOver28_Throws()
    {
        byte[] row = BuildNumericRow(lo: 1, mid: 0, hi: 0, negative: false);
        ColumnInfo column = NumericColumn(scale: 29);

        _ = Assert.Throws<JetLimitationException>(() =>
            JetTypeInfo.ReadFixedTyped(row, start: 0, column, size: 17, strictNumeric: true));
    }

    [Fact]
    public void Numeric_StrictMode_BufferTooShort_Throws()
    {
        byte[] row = new byte[8]; // far less than the 16 bytes Numeric needs

        _ = Assert.Throws<JetLimitationException>(() =>
            JetTypeInfo.ReadFixedTyped(row, start: 0, NumericType, size: 17, strictNumeric: true));
    }

    [Fact]
    public void Numeric_NonStrict_ScaleOver28_ReturnsDBNull()
    {
        byte[] row = BuildNumericRow(lo: 1, mid: 0, hi: 0, negative: false);
        ColumnInfo column = NumericColumn(scale: 29);

        object result = JetTypeInfo.ReadFixedTyped(row, start: 0, column, size: 17, strictNumeric: false);

        Assert.Equal(DBNull.Value, result);
    }

    [Theory]
    [InlineData(ComplexType)]
    [InlineData(AttachmentType)]
    public void Complex_ReturnsCxSentinelString(ColumnType columnType)
    {
        byte[] row = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(row, 42);

        object typed = JetTypeInfo.ReadFixedTyped(row, start: 0, columnType, size: 4);
        string viaString = JetTypeInfo.ReadFixedString(row, start: 0, columnType, size: 4);

        // Typed path now emits a typed ComplexIdRef sentinel rather than the
        // legacy "__CX:N__" string used by ReadFixedString — keep the string
        // path pinned for the diagnostics/RowsAsStrings consumer and assert
        // both encode the same complex_id.
        ComplexIdRef cir = Assert.IsType<ComplexIdRef>(typed);
        Assert.Equal(42, cir.Id);
        Assert.Equal("__CX:42__", viaString);
    }

    [Theory]
    [InlineData(ComplexType)]
    [InlineData(AttachmentType)]
    public void Complex_TooShort_ReturnsDBNull(ColumnType columnType)
    {
        byte[] row = new byte[2]; // size < 4

        object result = JetTypeInfo.ReadFixedTyped(row, start: 0, columnType, size: 2);

        Assert.Equal(DBNull.Value, result);
    }

    [Fact]
    public void OutOfRange_ReturnsDBNull()
    {
        byte[] row = new byte[2]; // LongInteger needs 4 bytes

        object result = JetTypeInfo.ReadFixedTyped(row, start: 0, LongIntegerType, size: 4);

        Assert.Equal(DBNull.Value, result);
    }

    private static byte[] BuildNumericRow(uint lo, uint mid, uint hi, bool negative)
    {
        // Access stores Numeric cells as [sign][16-byte unsigned magnitude].
        // The descriptor supplies scale; each 4-byte magnitude segment is
        // byte-swapped on page, matching Jackcess' fixNumericByteOrder helper.
        byte[] row = new byte[17];
        row[0] = negative ? (byte)0x80 : (byte)0x00;

        Span<byte> magnitudeLe = stackalloc byte[16];
        BinaryPrimitives.WriteUInt32LittleEndian(magnitudeLe[..4], lo);
        BinaryPrimitives.WriteUInt32LittleEndian(magnitudeLe.Slice(4, 4), mid);
        BinaryPrimitives.WriteUInt32LittleEndian(magnitudeLe.Slice(8, 4), hi);

        Span<byte> magnitudeBe = stackalloc byte[16];
        for (int i = 0; i < magnitudeBe.Length; i++)
        {
            magnitudeBe[i] = magnitudeLe[15 - i];
        }

        JetTypeInfo.FixNumericByteOrder(magnitudeBe);
        magnitudeBe.CopyTo(row.AsSpan(1, 16));
        return row;
    }

    private static ColumnInfo NumericColumn(byte scale) => new()
    {
        Type = NumericType,
        Size = 17,
        NumericPrecision = 28,
        NumericScale = scale,
    };

    private static void AssertNumericParity(byte[] row, byte scale, object expected, bool strictNumeric = false)
    {
        ColumnInfo column = NumericColumn(scale);
        object typed = JetTypeInfo.ReadFixedTyped(row, start: 0, column, size: 17, strictNumeric);
        Assert.Equal(expected, typed);

        string formatted = JetTypeInfo.ReadFixedString(row, start: 0, column, size: 17, strictNumeric);
        object viaRoundTrip = TypedValueParser.ParseValue(formatted, typeof(decimal));
        Assert.Equal(expected, viaRoundTrip);
    }

    private static void AssertParity(byte[] row, int start, ColumnType type, int size, object expected, bool strictNumeric = false)
    {
        // Typed path returns the boxed primitive directly.
        object typed = JetTypeInfo.ReadFixedTyped(row, start, type, size, strictNumeric);
        Assert.Equal(expected, typed);

        // Legacy round-trip: format → parse → boxed primitive. Must agree
        // (unless documented otherwise — see DateTime sub-second test).
        string formatted = JetTypeInfo.ReadFixedString(row, start, type, size, strictNumeric);
        Type targetType = JetTypeInfo.GetClrType(type) ?? typeof(string);
        object viaRoundTrip = TypedValueParser.ParseValue(formatted, targetType);
        Assert.Equal(expected, viaRoundTrip);
    }
}
