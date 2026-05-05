namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Property-based and structural assertions for
/// <see cref="GeneralTextIndexEncoder"/> and
/// <see cref="General97TextIndexEncoder"/>. Locks in the framing rules
/// (flag byte, terminator, descending bit-flip) and a handful of
/// hand-derivable byte sequences that a regression in the shared state
/// machine — or in the per-codepoint resource tables — would break.
/// </summary>
public sealed class GeneralEncoderSharedTests
{
    private const byte FlagAscNonNull = 0x7F;
    private const byte FlagDescNonNull = 0x80;
    private const byte FlagAscNull = 0x00;
    private const byte FlagDescNull = 0xFF;

    [Fact]
    public void General_NullAscending_ReturnsSingleNullFlagByte()
    {
        byte[] bytes = GeneralTextIndexEncoder.Encode(null, ascending: true);
        Assert.Equal([FlagAscNull], bytes);
    }

    [Fact]
    public void General_NullDescending_ReturnsSingleNullFlagByte()
    {
        byte[] bytes = GeneralTextIndexEncoder.Encode(null, ascending: false);
        Assert.Equal([FlagDescNull], bytes);
    }

    [Fact]
    public void General97_NullAscending_ReturnsSingleNullFlagByte()
    {
        byte[] bytes = General97TextIndexEncoder.Encode(null, ascending: true);
        Assert.Equal([FlagAscNull], bytes);
    }

    [Fact]
    public void General97_NullDescending_ReturnsSingleNullFlagByte()
    {
        byte[] bytes = General97TextIndexEncoder.Encode(null, ascending: false);
        Assert.Equal([FlagDescNull], bytes);
    }

    private static readonly byte[] EmptyEncodedAsc = [0x7F, 0x00];

    [Fact]
    public void General97_Empty_EmitsFlagPlusEndExtraText()
    {
        Assert.Equal(EmptyEncodedAsc, General97TextIndexEncoder.Encode(string.Empty, ascending: true));
    }

    [Fact]
    public void General97_AllSpaces_EmitsFlagPlusEndExtraText()
    {
        // Trailing-space stripping (post-trim empty) — same encoded form as "".
        Assert.Equal(EmptyEncodedAsc, General97TextIndexEncoder.Encode("   ", ascending: true));
    }

    [Theory]
    [InlineData("ABC")]
    [InlineData("hello")]
    [InlineData("0123456789")]
    [InlineData("Mixed Case 42")]
    public void General97_DescendingPayload_IsOnesComplementOfAscendingPayload(string text)
    {
        byte[] asc = General97TextIndexEncoder.Encode(text, ascending: true);
        byte[] desc = General97TextIndexEncoder.Encode(text, ascending: false);

        Assert.Equal(asc.Length, desc.Length);
        Assert.Equal(FlagAscNonNull, asc[0]);
        Assert.Equal(FlagDescNonNull, desc[0]);

        // The flag byte is never flipped (the ascending vs descending value
        // IS the signal); every byte after the flag must be a 1's-complement
        // pair with its ascending counterpart.
        for (int i = 1; i < asc.Length; i++)
        {
            Assert.Equal(unchecked((byte)~asc[i]), desc[i]);
        }
    }

    [Theory]
    [InlineData("ABC")]
    [InlineData("hello")]
    [InlineData("0123456789")]
    [InlineData("Mixed Case 42")]
    public void General_DescendingPayload_IsOnesComplementOfAscendingPayloadExceptOuterTerminator(string text)
    {
        byte[] asc = GeneralTextIndexEncoder.Encode(text, ascending: true);
        byte[] desc = GeneralTextIndexEncoder.Encode(text, ascending: false);

        // General-Legacy/General share Jackcess's two-step descending trailer:
        //   asc:  flag | payload | END_TEXT(0x01) | ... | END_EXTRA_TEXT(0x00)              [length N]
        //   desc: flag | ~payload | ~END_TEXT(0xFE) | ... | ~END_EXTRA_TEXT(0xFF) | 0x00     [length N+1]
        // i.e. before the bulk flip the encoder appends an extra END_EXTRA_TEXT (which the
        // flip then turns into 0xFF), then appends one more unflipped END_EXTRA_TEXT.
        Assert.Equal(asc.Length + 1, desc.Length);
        Assert.Equal(FlagAscNonNull, asc[0]);
        Assert.Equal(FlagDescNonNull, desc[0]);

        // Bytes 1..N-1 of desc are the ones-complement of bytes 1..N-1 of asc
        // (this includes the asc trailing END_EXTRA_TEXT 0x00 which maps to 0xFF).
        for (int i = 1; i < asc.Length; i++)
        {
            Assert.Equal(unchecked((byte)~asc[i]), desc[i]);
        }

        // Final byte of desc is the unflipped trailing END_EXTRA_TEXT marker.
        Assert.Equal(0x00, desc[^1]);
    }

    [Fact]
    public void General_AsciiUppercase_ProducesSameInlineBytesAsLowercase()
    {
        // The "General" table (Access 2010+) is case-insensitive on inline
        // bytes — same primary weight for "A" and "a"; case is encoded into
        // the trailing extra/unprintable streams. Verify by trimming the
        // trailers and comparing the inline portion.
        byte[] upper = GeneralTextIndexEncoder.Encode("ABC", ascending: true);
        byte[] lower = GeneralTextIndexEncoder.Encode("abc", ascending: true);

        // Inline bytes occupy positions 1..3 (one byte per ASCII letter).
        Assert.Equal(upper[1], lower[1]);
        Assert.Equal(upper[2], lower[2]);
        Assert.Equal(upper[3], lower[3]);
    }

    [Fact]
    public void General97_AsciiUppercase_ProducesSameInlineBytesAsLowercase()
    {
        byte[] upper = General97TextIndexEncoder.Encode("ABC", ascending: true);
        byte[] lower = General97TextIndexEncoder.Encode("abc", ascending: true);

        Assert.Equal(upper[1], lower[1]);
        Assert.Equal(upper[2], lower[2]);
        Assert.Equal(upper[3], lower[3]);
    }

    [Fact]
    public void General_SortedStrings_EncodeToUnsignedSortedBytes()
    {
        // Order-preservation property: sorting strings by their case-folded
        // primary weight should match unsigned-byte sort of the encoded keys.
        // We use a small ASCII-only set so the property is testable without
        // needing access to the per-codepoint table internals.
        string[] inputs = [string.Empty, "A", "AA", "AB", "B", "BA", "C", "Z"];
        AssertEncodeOrderingMatchesInputOrdering(inputs, GeneralTextIndexEncoder.Encode);
    }

    [Fact]
    public void General97_SortedStrings_EncodeToUnsignedSortedBytes()
    {
        string[] inputs = [string.Empty, "A", "AA", "AB", "B", "BA", "C", "Z"];
        AssertEncodeOrderingMatchesInputOrdering(inputs, General97TextIndexEncoder.Encode);
    }

    [Fact]
    public void GeneralLegacy_SortedStrings_EncodeToUnsignedSortedBytes()
    {
        string[] inputs = [string.Empty, "A", "AA", "AB", "B", "BA", "C", "Z"];
        AssertEncodeOrderingMatchesInputOrdering(inputs, GeneralLegacyTextIndexEncoder.Encode);
    }

    private static void AssertEncodeOrderingMatchesInputOrdering(
        string[] sortedInputs,
        TextIndexEncoderFixtureHarness.EncodeText encode)
    {
        var encoded = sortedInputs.Select(s => encode(s, ascending: true)).ToArray();
        for (int i = 1; i < encoded.Length; i++)
        {
            int cmp = CompareUnsigned(encoded[i - 1], encoded[i]);
            string msg = $"Encoding[{i - 1}] (\"{sortedInputs[i - 1]}\") should sort before encoding[{i}] (\"{sortedInputs[i]}\") but unsigned-byte comparison returned {cmp}.";
            Assert.True(cmp < 0, msg);
        }
    }

    private static int CompareUnsigned(byte[] a, byte[] b)
    {
        int min = Math.Min(a.Length, b.Length);
        for (int i = 0; i < min; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                return diff;
            }
        }

        return a.Length - b.Length;
    }
}
