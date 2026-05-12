namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

public sealed class TypedValueParserTests
{
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
    public void ParseValue_ByteArray_MalformedDashSeparatedHexReturnsEmpty()
    {
        object parsed = TypedValueParser.ParseValue("CA--FE", typeof(byte[]));

        byte[] bytes = Assert.IsType<byte[]>(parsed);
        Assert.Empty(bytes);
    }
}
