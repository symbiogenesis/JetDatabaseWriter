namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using JetDatabaseWriter.Infrastructure;
using Xunit;

public sealed class BinaryStringParserTests
{
    [Fact]
    public void TryDecodeBase64_DecodesPayloadSpan()
    {
        const string dataUri = "data:application/octet-stream;base64,AAECAwQ=";
        int comma = dataUri.IndexOf(',', StringComparison.Ordinal);

        bool decoded = BinaryStringParser.TryDecodeBase64(dataUri.AsSpan(comma + 1), out byte[] bytes);

        byte[] expected = [0, 1, 2, 3, 4];
        Assert.True(decoded);
        Assert.Equal(expected, bytes);
    }

    [Fact]
    public void TryDecodeBase64_UsesExactDecodedLengthForPaddedInput()
    {
        bool decoded = BinaryStringParser.TryDecodeBase64("TQ==".AsSpan(), out byte[] bytes);

        Assert.True(decoded);
        Assert.Single(bytes);
        Assert.Equal((byte)'M', bytes[0]);
    }

    [Theory]
    [InlineData("not-base64")]
    [InlineData("T===")]
    [InlineData("TQ=")]
    public void TryDecodeBase64_RejectsMalformedInput(string value)
    {
        bool decoded = BinaryStringParser.TryDecodeBase64(value.AsSpan(), out byte[] bytes);

        Assert.False(decoded);
        Assert.Empty(bytes);
    }

    [Fact]
    public void TryParseHexString_DecodesPlainHex()
    {
        bool parsed = BinaryStringParser.TryParseHexString("CAFEbabe".AsSpan(), out byte[] bytes);

        byte[] expected = [0xCA, 0xFE, 0xBA, 0xBE];
        Assert.True(parsed);
        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData("CAF")]
    [InlineData("CAFG")]
    public void TryParseHexString_RejectsMalformedPlainHex(string value)
    {
        bool parsed = BinaryStringParser.TryParseHexString(value.AsSpan(), out byte[] bytes);

        Assert.False(parsed);
        Assert.Empty(bytes);
    }

    [Fact]
    public void TryParseHexString_DecodesDashSeparatedBitConverterFormat()
    {
        bool parsed = BinaryStringParser.TryParseHexString("CA-FE-BA-BE".AsSpan(), out byte[] bytes);

        byte[] expected = [0xCA, 0xFE, 0xBA, 0xBE];
        Assert.True(parsed);
        Assert.Equal(expected, bytes);
    }

    [Fact]
    public void TryParseHexString_DecodesSingleByteFormat()
    {
        bool parsed = BinaryStringParser.TryParseHexString("FF".AsSpan(), out byte[] bytes);

        Assert.True(parsed);
        Assert.Single(bytes);
        Assert.Equal(0xFF, bytes[0]);
    }

    [Theory]
    [InlineData("CA--FE")]
    [InlineData("CA-")]
    [InlineData("C-A")]
    [InlineData("CA-FG")]
    public void TryParseHexString_RejectsMalformedDashSeparatedHex(string value)
    {
        bool parsed = BinaryStringParser.TryParseHexString(value.AsSpan(), out byte[] bytes);

        Assert.False(parsed);
        Assert.Empty(bytes);
    }
}
