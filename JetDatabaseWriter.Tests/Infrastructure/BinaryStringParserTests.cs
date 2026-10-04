namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using JetDatabaseWriter.Infrastructure;
using Xunit;

public sealed class BinaryStringParserTests
{
    [Fact]
    public void TryDecodeBase64_DecodesPayloadSpan()
    {
        bool decoded = BinaryStringParser.TryDecodeBase64("AAECAwQ=".AsSpan(), out byte[] bytes);

        byte[] expected = [0, 1, 2, 3, 4];
        Assert.True(decoded);
        Assert.Equal(expected, bytes);
    }

    [Fact]
    public void TryDecodeBase64DataUri_DecodesPayload()
    {
        bool decoded = BinaryStringParser.TryDecodeBase64DataUri("data:image/png;base64,AAECAwQ=", null, out byte[] bytes);

        byte[] expected = [0, 1, 2, 3, 4];
        Assert.True(decoded);
        Assert.Equal(expected, bytes);
    }

    [Fact]
    public void TryDecodeBase64DataUri_HonorsRequiredMediaType()
    {
        bool decoded = BinaryStringParser.TryDecodeBase64DataUri(
            "data:application/octet-stream;base64,AAECAwQ=",
            "application/octet-stream",
            out byte[] bytes);

        byte[] expected = [0, 1, 2, 3, 4];
        Assert.True(decoded);
        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData("data:image/png,AAECAwQ=")]
    [InlineData("data:image/png;base64,not-base64")]
    [InlineData("not-a-data-uri")]
    public void TryDecodeBase64DataUri_RejectsMalformedDataUri(string value)
    {
        bool decoded = BinaryStringParser.TryDecodeBase64DataUri(value, null, out byte[] bytes);

        Assert.False(decoded);
        Assert.Empty(bytes);
    }

    [Fact]
    public void TryDecodeBase64DataUri_RejectsWrongMediaType()
    {
        bool decoded = BinaryStringParser.TryDecodeBase64DataUri(
            "data:image/png;base64,AAECAwQ=",
            "application/octet-stream",
            out byte[] bytes);

        Assert.False(decoded);
        Assert.Empty(bytes);
    }

    [Fact]
    public void TryDecodeBase64_UsesExactDecodedLengthForPaddedInput()
    {
        bool decoded = BinaryStringParser.TryDecodeBase64("TQ==".AsSpan(), out byte[] bytes);

        Assert.True(decoded);
        Assert.Equal((byte)'M', Assert.Single(bytes));
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
}
