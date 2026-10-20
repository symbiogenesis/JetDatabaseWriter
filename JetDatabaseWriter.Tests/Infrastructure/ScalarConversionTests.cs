namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Globalization;
using JetDatabaseWriter.Infrastructure;
using Xunit;

/// <summary>Conversion probes preserve CLR rounding and reject boundary overflow.</summary>
public sealed class ScalarConversionTests
{
    [Fact]
    public void EnumTarget_IsAnExplicitConversionRefusal()
    {
        Assert.False(ScalarConversion.TryConvert(1, typeof(DayOfWeek), out object? result));
        Assert.Null(result);
    }

    [Fact]
    public void EnumSource_ConvertsThroughItsUnderlyingIntegralValue()
    {
        Assert.True(ScalarConversion.TryConvert(DayOfWeek.Monday, typeof(int), out object? result));
        Assert.Equal(1, result);
        Assert.True(ScalarConversion.TryConvert(DayOfWeek.Monday, typeof(string), out object? text));
        Assert.Equal("Monday", text);
    }

    [Theory]
    [InlineData(2.5, typeof(int))]
    [InlineData(3.5, typeof(int))]
    [InlineData(-0.5, typeof(byte))]
    [InlineData(9223372036854774784d, typeof(long))]
    [InlineData(7.922816251426432E28, typeof(decimal))]
    [InlineData(7.922816251426433E28, typeof(decimal))]
    public void NumericProbe_MatchesClrConversion(double value, Type type)
    {
        object expected = Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
        Assert.True(ScalarConversion.TryConvert(value, type, out object? actual));
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(double.NaN, typeof(decimal))]
    [InlineData(double.PositiveInfinity, typeof(int))]
    [InlineData(9223372036854775808d, typeof(long))]
    [InlineData(18446744073709551616d, typeof(ulong))]
    [InlineData(7.922816251426434E28, typeof(decimal))]
    public void OverflowProbe_ReturnsFalse(double value, Type type)
    {
        Assert.False(ScalarConversion.TryConvert(value, type, out object? result));
        Assert.Null(result);
    }

    [Fact]
    public void TextProbe_DoesNotRoundFractionalIntegerText()
    {
        Assert.False(ScalarConversion.TryConvert("2.5", typeof(int), out _));
        Assert.True(ScalarConversion.TryConvert("2.5", typeof(decimal), out object? result));
        Assert.Equal(2.5m, result);
    }
}
