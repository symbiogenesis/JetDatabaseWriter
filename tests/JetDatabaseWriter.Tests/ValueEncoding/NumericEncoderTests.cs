namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Globalization;
using JetDatabaseWriter.ValueEncoding;
using JetDatabaseWriter.ValueEncoding.Models;
using Xunit;

/// <summary>
/// Tests for shared NUMERIC fixed-point payload shaping.
/// </summary>
public sealed class NumericEncoderTests
{
    [Fact]
    public void FixedPointPayload_ZeroAtDeclaredScale_ReturnsPositiveZeroMagnitude()
    {
        byte[] magnitude = new byte[16];

        bool fits = NumericEncoder.TryEncodeFixedPointPayload(
            0m,
            targetScale: 2,
            magnitude,
            out FixedPointPayload payload);

        Assert.True(fits);
        Assert.False(payload.Negative);
        Assert.Equal(0, payload.NaturalScale);
        Assert.Equal(1, payload.DigitCount);
        Assert.Equal(0, payload.MagnitudeByteCount);
        Assert.Equal(new byte[16], magnitude);
    }

    [Fact]
    public void FixedPointPayload_NegativeValueAtLargerTargetScale_RescalesMagnitude()
    {
        byte[] magnitude = new byte[16];

        bool fits = NumericEncoder.TryEncodeFixedPointPayload(
            -1.23m,
            targetScale: 4,
            magnitude,
            out FixedPointPayload payload);

        byte[] expected = new byte[16];
        expected[14] = 0x30;
        expected[15] = 0x0C;

        Assert.True(fits);
        Assert.True(payload.Negative);
        Assert.Equal(2, payload.NaturalScale);
        Assert.Equal(5, payload.DigitCount);
        Assert.Equal(2, payload.MagnitudeByteCount);
        Assert.Equal(expected, magnitude);
    }

    [Fact]
    public void FixedPointPayload_MaxPrecisionInteger_FitsSixteenByteMagnitude()
    {
        decimal value = decimal.Parse("9999999999999999999999999999", CultureInfo.InvariantCulture);
        byte[] magnitude = new byte[16];

        bool fits = NumericEncoder.TryEncodeFixedPointPayload(
            value,
            targetScale: 0,
            magnitude,
            out FixedPointPayload payload);

        byte[] expected =
        [
            0x00, 0x00, 0x00, 0x00,
            0x20, 0x4F, 0xCE, 0x5E,
            0x3E, 0x25, 0x02, 0x61,
            0x0F, 0xFF, 0xFF, 0xFF,
        ];

        Assert.True(fits);
        Assert.False(payload.Negative);
        Assert.Equal(0, payload.NaturalScale);
        Assert.Equal(28, payload.DigitCount);
        Assert.Equal(12, payload.MagnitudeByteCount);
        Assert.Equal(expected, magnitude);
    }

    [Fact]
    public void FixedPointPayload_TargetScaleBelowNaturalScale_ThrowsArgumentException()
    {
        byte[] magnitude = new byte[16];

        ArgumentException ex = Assert.Throws<ArgumentException>(() =>
            NumericEncoder.TryEncodeFixedPointPayload(
                1.50m,
                targetScale: 0,
                magnitude,
                out _));

        Assert.Contains("targetScale (0) must be >= the value's natural scale (2)", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FixedPointPayload_OverflowPastSixteenByteMagnitude_ReturnsFalse()
    {
        byte[] magnitude = new byte[16];

        bool fits = NumericEncoder.TryEncodeFixedPointPayload(
            decimal.MaxValue,
            targetScale: 28,
            magnitude,
            out FixedPointPayload payload);

        Assert.False(fits);
        Assert.Equal(0, payload.NaturalScale);
        Assert.Equal(57, payload.DigitCount);
        Assert.True(payload.MagnitudeByteCount > 16);
    }

    [Theory]
    [InlineData(128, 1, "00000000000000000000000000000080")]
    [InlineData(255, 1, "000000000000000000000000000000FF")]
    [InlineData(256, 2, "00000000000000000000000000000100")]
    public void FixedPointPayload_UnsignedMagnitude_DoesNotIncludeSignPadding(int value, int byteCount, string expectedHex)
    {
        byte[] magnitude = new byte[16];

        bool fits = NumericEncoder.TryEncodeFixedPointPayload(value, targetScale: 0, magnitude, out FixedPointPayload payload);

        Assert.True(fits);
        Assert.Equal(byteCount, payload.MagnitudeByteCount);
        Assert.Equal(Convert.FromHexString(expectedHex), magnitude);
    }

    [Fact]
    public void FixedPointPayload_SixteenByteMagnitudeWithHighBitSet_FitsUnsignedSlot()
    {
        byte[] magnitude = new byte[16];

        bool fits = NumericEncoder.TryEncodeFixedPointPayload(
            20000000000000000000000000000m,
            targetScale: 10,
            magnitude,
            out FixedPointPayload payload);

        Assert.True(fits);
        Assert.Equal(16, payload.MagnitudeByteCount);
        Assert.Equal(Convert.FromHexString("96769950B50D88F41314448000000000"), magnitude);
    }

    [Fact]
    public void FixedPointPayload_Zero_ClearsOnlyMagnitudeSlot()
    {
        byte[] magnitude = new byte[18];
        Array.Fill(magnitude, (byte)0xA5);

        bool fits = NumericEncoder.TryEncodeFixedPointPayload(0m, targetScale: 2, magnitude, out FixedPointPayload payload);

        Assert.True(fits);
        Assert.Equal(0, payload.MagnitudeByteCount);
        Assert.Equal(new byte[16], magnitude[..16]);
        Assert.Equal(new byte[] { 0xA5, 0xA5 }, magnitude[16..]);
    }
}
