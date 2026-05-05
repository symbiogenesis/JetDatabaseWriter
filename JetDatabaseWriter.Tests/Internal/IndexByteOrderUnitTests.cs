namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Unit-level encoder invariants ported from Jackcess
/// <c>com.healthmarketscience.jackcess.IndexTest</c> and
/// <c>com.healthmarketscience.jackcess.impl.IndexCodesTest</c>:
/// <list type="bullet">
///   <item><c>testByteOrder</c>: unsigned byte ordering of the boundary
///         bytes <c>0x00, 0x01, 0x7F, 0x80, 0xFF</c>.</item>
///   <item><c>testByteCodeComparator</c>: lexicographic unsigned byte-array
///         ordering, including the prefix-vs-extension cases.</item>
///   <item>Encoder pair sanity: <c>"a" &lt; "b"</c>, <c>"abc" &lt; "abd"</c>,
///         <c>"" &lt; "a"</c> in both ascending and descending modes
///         (descending must reverse the comparison).</item>
/// </list>
/// These are pure in-memory checks that don't touch any fixture, so they
/// run quickly and keep the upstream-comparable invariants visible in the
/// test report.
/// </summary>
public sealed class IndexByteOrderUnitTests
{
    [Fact]
    public void ByteOrder_UnsignedBoundaries_AreOrdered()
    {
        // Jackcess `IndexTest.testByteOrder`.
        byte[] sorted = [0x00, 0x01, 0x7F, 0x80, 0xFF];
        for (int i = 1; i < sorted.Length; i++)
        {
            Assert.True(
                sorted[i - 1] < sorted[i],
                $"Unsigned byte order broken at position {i}: 0x{sorted[i - 1]:X2} >= 0x{sorted[i]:X2}");
        }
    }

    [Fact]
    public void ByteCodeComparator_OrdersByPrefixThenLength()
    {
        // Jackcess `IndexTest.testByteCodeComparator`.
        byte[] b1 = [0x00];
        byte[] b2 = [0x00, 0x00];
        byte[] b3 = [0x00, 0x01];
        byte[] b4 = [0x01];
        byte[] b5 = [0x80];
        byte[] b6 = [0xFF];
        byte[] b7 = [0xFF, 0x00];
        byte[] b8 = [0xFF, 0x01];

        var expected = new List<byte[]> { b1, b2, b3, b4, b5, b6, b7, b8 };

        var shuffled = new List<byte[]> { b8, b3, b1, b6, b4, b7, b5, b2 };
        shuffled.Sort(CompareBytesUnsigned);

        Assert.Equal(expected.Count, shuffled.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i], shuffled[i]);
        }
    }

    [Fact]
    public void GeneralLegacyEncoder_AsciiAscending_PreservesLexicographicOrder()
    {
        // Per Jackcess `GeneralLegacyIndexCodes`, ascending encoded keys for
        // ASCII strings must compare in the same order as the source strings.
        // Pick a tight set of boundary cases that have caught bugs upstream.
        string[] ordered =
        [
            string.Empty,
            "a",
            "ab",
            "abc",
            "abd",
            "b",
            "ba",
            "z",
        ];

        var encoded = ordered
            .Select(s => (Source: s, Key: GeneralLegacyTextIndexEncoder.Encode(s, ascending: true)))
            .ToList();

        for (int i = 1; i < encoded.Count; i++)
        {
            int cmp = CompareBytesUnsigned(encoded[i - 1].Key, encoded[i].Key);
            string prevHex = Convert.ToHexString(encoded[i - 1].Key);
            string curHex = Convert.ToHexString(encoded[i].Key);
            string msg = $"Ascending encoder reordered: \"{encoded[i - 1].Source}\" should sort before \"{encoded[i].Source}\" (prev={prevHex} cur={curHex})";
            Assert.True(cmp < 0, msg);
        }
    }

    [Fact]
    public void GeneralLegacyEncoder_AsciiDescending_ReversesLexicographicOrder()
    {
        string[] ordered = [string.Empty, "a", "ab", "abc", "b", "z"];

        var encoded = ordered
            .Select(s => (Source: s, Key: GeneralLegacyTextIndexEncoder.Encode(s, ascending: false)))
            .ToList();

        // Descending: input order [empty, a, ab, abc, b, z] should produce
        // keys whose unsigned ordering is REVERSED.
        for (int i = 1; i < encoded.Count; i++)
        {
            int cmp = CompareBytesUnsigned(encoded[i - 1].Key, encoded[i].Key);
            string prevHex = Convert.ToHexString(encoded[i - 1].Key);
            string curHex = Convert.ToHexString(encoded[i].Key);
            string msg = $"Descending encoder did not reverse order: \"{encoded[i - 1].Source}\" should sort AFTER \"{encoded[i].Source}\" (prev={prevHex} cur={curHex})";
            Assert.True(cmp > 0, msg);
        }
    }

    [Fact]
    public void GeneralLegacyEncoder_NullEncoding_MatchesJackcessFlagBytes()
    {
        // Per Jackcess `IndexCodes.getNullEntryFlag(boolean isAsc)`:
        // ascending null = 0x00, descending null = 0xFF (a single byte).
        byte[] ascNull = GeneralLegacyTextIndexEncoder.Encode(null, ascending: true);
        byte[] descNull = GeneralLegacyTextIndexEncoder.Encode(null, ascending: false);

        Assert.Equal(new byte[] { 0x00 }, ascNull);
        Assert.Equal(new byte[] { 0xFF }, descNull);
    }

    [Fact]
    public void GeneralLegacyEncoder_NonNullStartFlags_MatchJackcessConstants()
    {
        // Per Jackcess `IndexCodes.getStartEntryFlag`: ascending non-null
        // entries begin with 0x7F, descending with 0x80.
        byte[] asc = GeneralLegacyTextIndexEncoder.Encode("a", ascending: true);
        byte[] desc = GeneralLegacyTextIndexEncoder.Encode("a", ascending: false);

        Assert.Equal((byte)0x7F, asc[0]);
        Assert.Equal((byte)0x80, desc[0]);
    }

    private static int CompareBytesUnsigned(byte[] a, byte[] b)
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
