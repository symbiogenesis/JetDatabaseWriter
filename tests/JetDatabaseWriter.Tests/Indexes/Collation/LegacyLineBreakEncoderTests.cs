namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Linq;
using JetDatabaseWriter.Indexes.Collation;
using Xunit;

/// <summary>Long legacy keys preserve actual line-break codes and auxiliary positions.</summary>
public sealed class LegacyLineBreakEncoderTests
{
    [Theory]
    [InlineData("\n", "0804")]
    [InlineData("\r", "0807")]
    [InlineData("\r\n", "08070804")]
    [InlineData("\n\r", "08040807")]
    public void LongKeyPreservesLineBreakAndStopsAt255Characters(string lineBreak, string code)
    {
        ArgumentNullException.ThrowIfNull(lineBreak);
        string text = new string('a', 90) + lineBreak + new string('b', 255 - 90 - lineBreak.Length);
        byte[] expected = [0x7F, .. Enumerable.Repeat((byte)0x4A, 90), .. Convert.FromHexString(code), .. Enumerable.Repeat((byte)0x4C, 255 - 90 - lineBreak.Length), 0x01, 0x00];
        Assert.Equal(expected, GeneralLegacyTextIndexEncoder.Encode(text + "z", ascending: true));
        byte[] descending = [0x80, .. expected.Skip(1).Select(b => unchecked((byte)~b)), 0x00];
        Assert.Equal(descending, GeneralLegacyTextIndexEncoder.Encode(text + "z", ascending: false));
    }

    [Theory]
    [InlineData("\n", 0x8173)]
    [InlineData("\r", 0x8173)]
    [InlineData("\r\n", 0x8177)]
    [InlineData("\n\r", 0x8177)]
    public void LongKeyCountsLineBreaksInUnprintableOffset(string lineBreak, int offset)
    {
        string text = new string('a', 90) + lineBreak + "\u0001" + new string('b', 40);
        byte[] key = GeneralLegacyTextIndexEncoder.Encode(text, ascending: true);
        byte[] expectedSuffix = [0x01, 0x01, 0x01, 0x01, (byte)(offset >> 8), unchecked((byte)offset), 0x06, 0x03, 0x00];
        Assert.Equal(expectedSuffix, key[^expectedSuffix.Length..]);
    }
}
