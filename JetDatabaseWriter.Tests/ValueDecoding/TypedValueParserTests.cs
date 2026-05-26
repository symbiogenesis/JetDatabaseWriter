namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

public sealed class TypedValueParserTests
{
    [Theory]
    [InlineData(typeof(string))]
    [InlineData(typeof(int))]
    [InlineData(typeof(DateTime))]
    [InlineData(typeof(byte[]))]
    public void ParseValue_EmptyString_ReturnsDBNull(Type targetType)
    {
        object parsed = TypedValueParser.ParseValue(string.Empty, targetType);

        Assert.Equal(DBNull.Value, parsed);
    }

    [Fact]
    public void ParseValue_ByteArray_DecodesBase64DataUri()
    {
        object parsed = TypedValueParser.ParseValue("data:application/octet-stream;base64,AAECAwQ=", typeof(byte[]));

        byte[] expected = [0, 1, 2, 3, 4];
        byte[] bytes = Assert.IsType<byte[]>(parsed);
        Assert.Equal(expected, bytes);
    }

    [Fact]
    public void ParseValue_ByteArray_DecodesDashSeparatedHex()
    {
        object parsed = TypedValueParser.ParseValue("CA-FE-BA-BE", typeof(byte[]));

        byte[] expected = [0xCA, 0xFE, 0xBA, 0xBE];
        byte[] bytes = Assert.IsType<byte[]>(parsed);
        Assert.Equal(expected, bytes);
    }

    [Fact]
    public void ParseValue_ByteArray_DecodesPlainHex()
    {
        object parsed = TypedValueParser.ParseValue("CAFEbabe", typeof(byte[]));

        byte[] expected = [0xCA, 0xFE, 0xBA, 0xBE];
        byte[] bytes = Assert.IsType<byte[]>(parsed);
        Assert.Equal(expected, bytes);
    }

    [Fact]
    public void ParseValue_ByteArray_MalformedBase64DataUriThrowsInStrictMode()
    {
        _ = Assert.Throws<FormatException>(() =>
            TypedValueParser.ParseValue("data:application/octet-stream;base64,not-base64", typeof(byte[])));
    }

    [Fact]
    public void ParseValue_ByteArray_MalformedBase64DataUriReturnsDBNullInNonStrictMode()
    {
        object parsed = TypedValueParser.ParseValue("data:application/octet-stream;base64,not-base64", typeof(byte[]), strictMode: false);

        Assert.Equal(DBNull.Value, parsed);
    }

    [Fact]
    public void ParseValue_ByteArray_MalformedDashSeparatedHexThrowsInStrictMode()
    {
        FormatException exception = Assert.Throws<FormatException>(() =>
            TypedValueParser.ParseValue("CA--FE", typeof(byte[])));

        Assert.Contains("dash-separated hex", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseValue_ByteArray_MalformedDashSeparatedHexReturnsDBNullInNonStrictMode()
    {
        object parsed = TypedValueParser.ParseValue("CA--FE", typeof(byte[]), strictMode: false);

        Assert.Equal(DBNull.Value, parsed);
    }

    [Theory]
    [InlineData("not-an-int", typeof(int))]
    [InlineData("999999999999999999999999999999", typeof(int))]
    [InlineData("not-a-date", typeof(DateTime))]
    public void ParseValue_InvalidPrimitiveThrowsInStrictMode(string value, Type targetType)
    {
        FormatException exception = Assert.Throws<FormatException>(() =>
            TypedValueParser.ParseValue(value, targetType));

        Assert.Contains(targetType.FullName!, exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("not-an-int", typeof(int))]
    [InlineData("not-a-date", typeof(DateTime))]
    public void ParseValue_InvalidPrimitiveReturnsDBNullInNonStrictMode(string value, Type targetType)
    {
        object parsed = TypedValueParser.ParseValue(value, targetType, strictMode: false);

        Assert.Equal(DBNull.Value, parsed);
    }

    [Theory]
    [InlineData("(OLE chain error: no chunks read)")]
    [InlineData("(memo on LVAL page)")]
    public void ParseValue_ByteArray_LongValueDiagnosticThrowsInStrictMode(string value)
    {
        FormatException exception = Assert.Throws<FormatException>(() =>
            TypedValueParser.ParseValue(value, typeof(byte[])));

        Assert.Contains("long-value decoder", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("(OLE chain error: no chunks read)")]
    [InlineData("(memo on LVAL page)")]
    public void ParseValue_ByteArray_LongValueDiagnosticReturnsDBNullInNonStrictMode(string value)
    {
        object parsed = TypedValueParser.ParseValue(value, typeof(byte[]), strictMode: false);

        Assert.Equal(DBNull.Value, parsed);
    }
}
