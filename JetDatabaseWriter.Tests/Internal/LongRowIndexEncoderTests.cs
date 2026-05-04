namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Linq;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

#pragma warning disable CA1707 // underscores in test names

/// <summary>
/// Unit tests for the 2-chunk "long-row" path added to
/// <see cref="GeneralLegacyTextIndexEncoder"/> /
/// <see cref="GeneralTextIndexEncoder"/>. The fixture-driven byte-exact
/// validation for V2000 / V2003 / V2007 lives in the per-encoder fixture
/// suites; this class adds focused unit assertions for the structural
/// invariants of the 2-chunk path so regressions surface with smaller
/// diffs than the fixture sweeps.
/// </summary>
public sealed class LongRowIndexEncoderTests
{
    [Fact]
    public void Encode_LongInputWithCrLf_EmitsTwoChunksSeparatedBy08070804_GeneralLegacy()
    {
        // 200-char ASCII input with a CRLF at position 90.
        string text = new string('a', 90) + "\r\n" + new string('b', 108);
        byte[] enc = GeneralLegacyTextIndexEncoder.Encode(text, ascending: true);

        AssertHasSubsequence(enc, [0x08, 0x07, 0x08, 0x04]);
        Assert.Equal(0x7F, enc[0]);
        Assert.Equal(0x00, enc[^1]);
    }

    [Fact]
    public void Encode_LongInputWithCrLf_EmitsTwoChunksSeparatedBy07090706_General()
    {
        string text = new string('a', 90) + "\r\n" + new string('b', 108);
        byte[] enc = GeneralTextIndexEncoder.Encode(text, ascending: true);

        AssertHasSubsequence(enc, [0x07, 0x09, 0x07, 0x06]);
        Assert.Equal(0x7F, enc[0]);
        Assert.Equal(0x00, enc[^1]);
    }

    [Fact]
    public void Encode_LongInputWithoutLineBreak_FallsBackToSingleChunkPath()
    {
        // > 127 chars but no CR/LF: must NOT emit a separator (we don't have
        // an Access-authored fixture for this branch). Single-chunk
        // truncation at MaxTextIndexCharLength preserves the pre-2-chunk
        // behaviour.
        string text = new string('a', 200);
        byte[] enc = GeneralLegacyTextIndexEncoder.Encode(text, ascending: true);

        Assert.False(ContainsSubsequence(enc, [0x08, 0x07, 0x08, 0x04]));
    }

    [Fact]
    public void Encode_LongInput_DescendingComplementsTwoChunkPayload_GeneralLegacy()
    {
        string text = new string('a', 90) + "\r\n" + new string('b', 108);
        byte[] asc = GeneralLegacyTextIndexEncoder.Encode(text, ascending: true);
        byte[] desc = GeneralLegacyTextIndexEncoder.Encode(text, ascending: false);

        Assert.Equal(0x7F, asc[0]);
        Assert.Equal(0x80, desc[0]);

        // The descending pass complements the entire payload (everything
        // after the leading flag) and appends a fresh unflipped 0x00. So
        // (asc[0] xor 0xFF) == desc[0] is false (those are the flag bytes),
        // but for every byte in the payload (excluding the trailing 0x00),
        // desc[i] = ~asc[i]. Then the trailing 0x00 is the unflipped
        // sentinel: the last byte of asc is 0x00, the second-to-last of
        // desc is ~0x00 = 0xFF, and the last byte of desc is 0x00.
        Assert.Equal(0x00, desc[^1]);
        Assert.Equal(0xFF, desc[^2]);

        // The complemented genleg separator 08 07 08 04 -> F7 F8 F7 FB.
        AssertHasSubsequence(desc, [0xF7, 0xF8, 0xF7, 0xFB]);
    }

    private static void AssertHasSubsequence(byte[] hay, byte[] needle)
    {
        Assert.True(
            ContainsSubsequence(hay, needle),
            $"Expected sequence {Convert.ToHexString(needle)} in {Convert.ToHexString(hay)}.");
    }

    private static bool ContainsSubsequence(byte[] hay, byte[] needle)
    {
        for (int i = 0; i + needle.Length <= hay.Length; i++)
        {
            if (hay.AsSpan(i, needle.Length).SequenceEqual(needle))
            {
                return true;
            }
        }

        return false;
    }
}
