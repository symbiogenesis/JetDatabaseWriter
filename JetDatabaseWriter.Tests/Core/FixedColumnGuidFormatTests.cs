namespace JetDatabaseWriter.Tests.Core;

using System;
using JetDatabaseWriter.Core;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Pins the string format produced by <see cref="AccessBase.ReadFixedString"/>
/// for <c>T_GUID</c> columns. The format is:
/// <code>{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}</code>
/// — braces, lowercase hex, with the first three groups stored little-endian
/// on disk (matching <see cref="Guid(byte[])"/>'s mixed-endian byte layout
/// and <see cref="Guid.ToString(string)"/> with format <c>"B"</c>).
/// </summary>
public sealed class FixedColumnGuidFormatTests
{
    private const byte T_GUID = 0x0F;

    [Fact]
    public void ReadFixedString_Guid_FormatsWithBracesAndLowercaseHex()
    {
        // The Jet on-disk byte order is the same mixed-endian layout used by
        // System.Guid(byte[]): the first 4 bytes form the first hex group in
        // little-endian, the next two pairs form the second/third groups in
        // little-endian, and the final 8 bytes are stored verbatim.
        byte[] row = [0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF];

        string result = AccessBase.ReadFixedString(row, start: 0, type: T_GUID, size: 16);

        Assert.Equal("{00112233-4455-6677-8899-aabbccddeeff}", result);
    }

    [Fact]
    public void ReadFixedString_Guid_RoundTripsThroughGuidParse()
    {
        var expected = Guid.NewGuid();
        byte[] bytes = expected.ToByteArray();
        byte[] row = new byte[24];
        Buffer.BlockCopy(bytes, 0, row, 4, 16); // place at non-zero offset to exercise 'start'

        string formatted = AccessBase.ReadFixedString(row, start: 4, type: T_GUID, size: 16);

        Assert.StartsWith("{", formatted, StringComparison.Ordinal);
        Assert.EndsWith("}", formatted, StringComparison.Ordinal);
        Assert.Equal(expected, Guid.Parse(formatted));
    }

    [Theory]
    [InlineData("00000000-0000-0000-0000-000000000000")]
    [InlineData("ffffffff-ffff-ffff-ffff-ffffffffffff")]
    [InlineData("12345678-9abc-def0-1234-56789abcdef0")]
    public void ReadFixedString_Guid_PreservesValueExactly(string guidText)
    {
        var expected = Guid.Parse(guidText);
        byte[] row = expected.ToByteArray();

        string formatted = AccessBase.ReadFixedString(row, start: 0, type: T_GUID, size: 16);

        Assert.Equal($"{{{guidText}}}", formatted);
    }

    [Fact]
    public void ReadFixedString_Guid_AllHexCharactersAreLowercase()
    {
        // Use a value whose every hex group contains A–F digits.
        byte[] row = new Guid("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").ToByteArray();

        string formatted = AccessBase.ReadFixedString(row, start: 0, type: T_GUID, size: 16);

        // Strip the brace delimiters and dashes; the remaining 32 hex digits
        // must all be lowercase (no A–F uppercase characters).
        foreach (char c in formatted)
        {
            if (c >= 'A' && c <= 'F')
            {
                Assert.Fail($"Expected lowercase hex but found uppercase '{c}' in '{formatted}'.");
            }
        }
    }

    [Fact]
    public void ReadFixedString_Guid_RespectsStartOffset()
    {
        var first = Guid.Parse("11111111-2222-3333-4444-555555555555");
        var second = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

        byte[] row = new byte[32];
        Buffer.BlockCopy(first.ToByteArray(), 0, row, 0, 16);
        Buffer.BlockCopy(second.ToByteArray(), 0, row, 16, 16);

        Assert.Equal("{11111111-2222-3333-4444-555555555555}", AccessBase.ReadFixedString(row, start: 0, type: T_GUID, size: 16));
        Assert.Equal("{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}", AccessBase.ReadFixedString(row, start: 16, type: T_GUID, size: 16));
    }
}
