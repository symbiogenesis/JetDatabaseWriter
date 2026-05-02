namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Buffers.Binary;
using System.Globalization;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Pins the contract for <see cref="AccessBase.ReadFixedTyped"/>: the typed
/// fixed-width decode that powers the typed-row read path. Each test verifies
/// parity with the legacy <see cref="AccessBase.ReadFixedString"/> +
/// <see cref="TypedValueParser.ParseValue"/> round-trip the typed reader is
/// replacing — except where the round-trip is documented as lossy (sub-second
/// T_DATETIME precision), in which case the typed path is asserted to keep
/// the un-truncated value while the round-trip drops it.
/// </summary>
public sealed class ReadFixedTypedTests
{
    private const byte T_BYTE = 0x02;
    private const byte T_INT = 0x03;
    private const byte T_LONG = 0x04;
    private const byte T_MONEY = 0x05;
    private const byte T_FLOAT = 0x06;
    private const byte T_DOUBLE = 0x07;
    private const byte T_DATETIME = 0x08;
    private const byte T_GUID = 0x0F;
    private const byte T_NUMERIC = 0x10;
    private const byte T_ATTACHMENT = 0x11;
    private const byte T_COMPLEX = 0x12;

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(127)]
    [InlineData(255)]
    public void Byte_RoundTripsThroughParseValue(byte value)
    {
        byte[] row = [value];
        AssertParity(row, start: 0, T_BYTE, size: 1, expected: value);
    }

    [Theory]
    [InlineData((short)0)]
    [InlineData((short)1)]
    [InlineData(short.MaxValue)]
    public void Int_NonNegative_RoundTripsThroughParseValue(short value)
    {
        byte[] row = new byte[2];
        BinaryPrimitives.WriteInt16LittleEndian(row, value);
        AssertParity(row, start: 0, T_INT, size: 2, expected: value);
    }

    /// <summary>
    /// Negative shorts trip the legacy <c>(short)Ru16(...)</c> cast under
    /// <c>&lt;CheckForOverflowUnderflow&gt;true&lt;/CheckForOverflowUnderflow&gt;</c>:
    /// <see cref="AccessBase.ReadFixedString"/> catches the
    /// <see cref="OverflowException"/> and returns <see cref="string.Empty"/>,
    /// which <see cref="TypedValueParser.ParseValue"/> maps to
    /// <see cref="DBNull.Value"/>. The typed path uses
    /// <see cref="BinaryPrimitives.ReadInt16LittleEndian(System.ReadOnlySpan{byte})"/>
    /// and keeps the correct value.
    /// </summary>
    [Theory]
    [InlineData((short)-1)]
    [InlineData(short.MinValue)]
    public void Int_Negative_TypedKeepsValue_RoundTripDropsToDBNull(short value)
    {
        byte[] row = new byte[2];
        BinaryPrimitives.WriteInt16LittleEndian(row, value);

        object typed = AccessBase.ReadFixedTyped(row, start: 0, T_INT, size: 2);
        Assert.Equal(value, typed);

        string formatted = AccessBase.ReadFixedString(row, start: 0, T_INT, size: 2);
        object viaRoundTrip = TypedValueParser.ParseValue(formatted, typeof(short));
        Assert.Equal(DBNull.Value, viaRoundTrip);
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
        AssertParity(row, start: 0, T_LONG, size: 4, expected: value);
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
        AssertParity(row, start: 0, T_FLOAT, size: 4, expected: value);
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
        AssertParity(row, start: 0, T_DOUBLE, size: 8, expected: value);
    }

    /// <summary>
    /// T_DATETIME values whose seconds line up exactly survive the
    /// <c>"yyyy-MM-dd HH:mm:ss"</c> round-trip, so parity with the legacy path
    /// must hold here.
    /// </summary>
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

        AssertParity(row, start: 0, T_DATETIME, size: 8, expected: dt);
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

        object typed = AccessBase.ReadFixedTyped(row, start: 0, T_DATETIME, size: 8);
        var typedDt = Assert.IsType<DateTime>(typed);

        // Round-trip via OADate has its own quantization, but it preserves
        // sub-second information that the "yyyy-MM-dd HH:mm:ss" format strips.
        Assert.NotEqual(0, typedDt.Millisecond);

        string formatted = AccessBase.ReadFixedString(row, start: 0, T_DATETIME, size: 8);
        var roundTripped = (DateTime)TypedValueParser.ParseValue(formatted, typeof(DateTime));
        Assert.Equal(0, roundTripped.Millisecond);
    }

    /// <summary>
    /// T_MONEY is stored as an OACurrency int64 with implicit scale=4. Verify
    /// the typed path returns the same decimal the round-trip parses.
    /// </summary>
    [Theory]
    [InlineData(0L, "0.0000")]
    [InlineData(12345L, "1.2345")]
    [InlineData(-12345L, "-1.2345")]
    [InlineData(99999999999999L, "9999999999.9999")]
    public void Money_Scale4_RoundTripsThroughParseValue(long oaCurrency, string expectedDecimal)
    {
        byte[] row = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(row, oaCurrency);
        var expected = decimal.Parse(expectedDecimal, CultureInfo.InvariantCulture);

        AssertParity(row, start: 0, T_MONEY, size: 8, expected: expected);
    }

    [Fact]
    public void Guid_RoundTripsThroughParseValue()
    {
        var expected = Guid.Parse("12345678-9abc-def0-1234-56789abcdef0");
        byte[] row = expected.ToByteArray();

        AssertParity(row, start: 0, T_GUID, size: 16, expected: expected);
    }

    /// <summary>
    /// Numeric (T_NUMERIC) values inside the .NET decimal range round-trip
    /// through the legacy string path; the typed path must agree.
    /// </summary>
    [Theory]
    [InlineData(0u, 0u, 0u, false, (byte)0, "0")]
    [InlineData(12345u, 0u, 0u, false, (byte)0, "12345")]
    [InlineData(12345u, 0u, 0u, true, (byte)0, "-12345")]
    [InlineData(12345u, 0u, 0u, false, (byte)4, "1.2345")]
    public void Numeric_InRange_RoundTripsThroughParseValue(uint lo, uint mid, uint hi, bool negative, byte scale, string expectedDecimal)
    {
        byte[] row = BuildNumericRow(lo, mid, hi, negative, scale);
        var expected = decimal.Parse(expectedDecimal, CultureInfo.InvariantCulture);

        AssertParity(row, start: 0, T_NUMERIC, size: 17, expected: expected, strictNumeric: true);
    }

    /// <summary>
    /// Decimal values whose mantissa words have the high bit set (e.g.
    /// <see cref="decimal.MaxValue"/> with all-ones lo/mid/hi) trip the
    /// <c>(int)uint</c> bit-pattern cast inside <see cref="AccessBase.ReadFixedString"/>'s
    /// <c>ReadNumericString</c> under <c>&lt;CheckForOverflowUnderflow&gt;true&lt;/CheckForOverflowUnderflow&gt;</c>:
    /// the legacy path catches and surfaces an empty string (non-strict) or a
    /// <see cref="JetLimitationException"/> (strict). The typed path uses
    /// <c>unchecked((int)lo)</c> to preserve the bit pattern and returns the
    /// correct decimal.
    /// </summary>
    [Fact]
    public void Numeric_DecimalMaxValue_TypedKeepsValue_RoundTripDropsToDBNull()
    {
        byte[] row = BuildNumericRow(lo: 0xFFFFFFFFu, mid: 0xFFFFFFFFu, hi: 0xFFFFFFFFu, negative: false, scale: 0);

        object typed = AccessBase.ReadFixedTyped(row, start: 0, T_NUMERIC, size: 17);
        Assert.Equal(decimal.MaxValue, typed);

        string formatted = AccessBase.ReadFixedString(row, start: 0, T_NUMERIC, size: 17);
        Assert.Equal(string.Empty, formatted);
        object viaRoundTrip = TypedValueParser.ParseValue(formatted, typeof(decimal));
        Assert.Equal(DBNull.Value, viaRoundTrip);
    }

    [Fact]
    public void Numeric_StrictMode_ScaleOver28_Throws()
    {
        byte[] row = BuildNumericRow(lo: 1, mid: 0, hi: 0, negative: false, scale: 29);

        _ = Assert.Throws<JetLimitationException>(() =>
            AccessBase.ReadFixedTyped(row, start: 0, T_NUMERIC, size: 17, strictNumeric: true));
    }

    [Fact]
    public void Numeric_StrictMode_BufferTooShort_Throws()
    {
        byte[] row = new byte[8]; // far less than the 16 bytes T_NUMERIC needs

        _ = Assert.Throws<JetLimitationException>(() =>
            AccessBase.ReadFixedTyped(row, start: 0, T_NUMERIC, size: 17, strictNumeric: true));
    }

    [Fact]
    public void Numeric_NonStrict_ScaleOver28_ReturnsDBNull()
    {
        byte[] row = BuildNumericRow(lo: 1, mid: 0, hi: 0, negative: false, scale: 29);

        object result = AccessBase.ReadFixedTyped(row, start: 0, T_NUMERIC, size: 17, strictNumeric: false);

        Assert.Equal(DBNull.Value, result);
    }

    [Theory]
    [InlineData(T_COMPLEX)]
    [InlineData(T_ATTACHMENT)]
    public void Complex_ReturnsCxSentinelString(byte type)
    {
        byte[] row = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(row, 42);

        object typed = AccessBase.ReadFixedTyped(row, start: 0, type, size: 4);
        string viaString = AccessBase.ReadFixedString(row, start: 0, type, size: 4);

        Assert.Equal("__CX:42__", typed);
        Assert.Equal(viaString, typed);
    }

    [Theory]
    [InlineData(T_COMPLEX)]
    [InlineData(T_ATTACHMENT)]
    public void Complex_TooShort_ReturnsDBNull(byte type)
    {
        byte[] row = new byte[2]; // size < 4

        object result = AccessBase.ReadFixedTyped(row, start: 0, type, size: 2);

        Assert.Equal(DBNull.Value, result);
    }

    [Fact]
    public void OutOfRange_ReturnsDBNull()
    {
        byte[] row = new byte[2]; // T_LONG needs 4 bytes

        object result = AccessBase.ReadFixedTyped(row, start: 0, T_LONG, size: 4);

        Assert.Equal(DBNull.Value, result);
    }

    private static byte[] BuildNumericRow(uint lo, uint mid, uint hi, bool negative, byte scale)
    {
        // Layout: [precision][scale][sign][pad][lo:4][mid:4][hi:4] (16 bytes
        // worth of useful data; the on-disk slot is 17 bytes including the
        // unused leading precision byte).
        byte[] row = new byte[17];
        row[0] = 0;
        row[1] = scale;
        row[2] = (byte)(negative ? 1 : 0);
        row[3] = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(row.AsSpan(4, 4), lo);
        BinaryPrimitives.WriteUInt32LittleEndian(row.AsSpan(8, 4), mid);
        BinaryPrimitives.WriteUInt32LittleEndian(row.AsSpan(12, 4), hi);
        return row;
    }

    private static void AssertParity(byte[] row, int start, byte type, int size, object expected, bool strictNumeric = false)
    {
        // Typed path returns the boxed primitive directly.
        object typed = AccessBase.ReadFixedTyped(row, start, type, size, strictNumeric);
        Assert.Equal(expected, typed);

        // Legacy round-trip: format → parse → boxed primitive. Must agree
        // (unless documented otherwise — see DateTime sub-second test).
        string formatted = AccessBase.ReadFixedString(row, start, type, size, strictNumeric);
        Type targetType = JetTypeInfo.GetClrType(type) ?? typeof(string);
        object viaRoundTrip = TypedValueParser.ParseValue(formatted, targetType);
        Assert.Equal(expected, viaRoundTrip);
    }
}
