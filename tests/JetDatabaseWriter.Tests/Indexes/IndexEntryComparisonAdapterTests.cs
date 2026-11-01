namespace JetDatabaseWriter.Tests.Indexes;

using System;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Unsigned key comparison agrees with the verified reference before bookmark completion.</summary>
public sealed class IndexEntryComparisonAdapterTests
{
    /// <summary>Every byte pair retains unsigned ordering despite an opposing bookmark order.</summary>
    [Fact]
    public void Comparison_AllBytePairsPrioritizeUnsignedKeysBeforeBookmarks()
    {
        for (int left = 0; left < 256; left++)
        {
            for (int right = 0; right < 256; right++)
            {
                int expected = left == right ? 1 : Math.Sign(left - right);
                AssertComparison(new([0x80, checked((byte)left), 0xFF], 256, 0), new([0x80, checked((byte)right), 0xFF], 255, 255), expected);
            }
        }
    }

    /// <summary>Every possible mismatch and strict prefix is checked around common vector boundaries.</summary>
    /// <param name="length">The shared key width.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(257)]
    public void Comparison_FirstMismatchAndStrictPrefixAgreeWithReference(int length)
    {
        byte[] shared = new byte[length];
        Array.Fill(shared, (byte)0x80);
        AssertComparison(new(shared, 10, 20), new(shared, 10, 20), 0);
        AssertComparison(new(shared, 10, 20), new([.. shared], 10, 20), 0);
        for (int position = 0; position < length; position++)
        {
            byte[] lower = [.. shared];
            lower[position] = 0x7F;
            if (position + 1 < length)
            {
                lower[position + 1] = 0xFF;
            }

            byte[] upper = [.. shared];
            upper[position] = 0x81;
            if (position + 1 < length)
            {
                upper[position + 1] = 0;
            }

            AssertComparison(new(shared, 1, 0), new(lower, 2, 255), 1);
            AssertComparison(new(shared, 2, 255), new(upper, 1, 0), -1);
            AssertComparison(new(shared[..position], 2, 255), new(shared, 1, 0), -1);
        }
    }

    /// <summary>Equal keys preserve page/row precedence without signed narrowing or subtraction overflow.</summary>
    [Fact]
    public void Comparison_EqualKeysOrderAllBookmarkBoundaries()
    {
        long[] pages = [long.MinValue, -1, 0, 1, 255, 256, 65535, 65536, 0xFFFFFF, long.MaxValue];
        byte[] rows = [0, 1, 255];
        for (int left = 0; left < pages.Length * rows.Length; left++)
        {
            for (int right = 0; right < pages.Length * rows.Length; right++)
            {
                AssertComparison(
                    new([0, 0x80, 0xFF], pages[left / rows.Length], rows[left % rows.Length]),
                    new([0, 0x80, 0xFF], pages[right / rows.Length], rows[right % rows.Length]),
                    Math.Sign(left - right));
            }
        }
    }

    /// <summary>Completion normalizes arbitrary signed magnitudes and consults bookmarks only on key equality.</summary>
    [Fact]
    public void Completion_NormalizesSignsBeforeBookmarkDecisions()
    {
        long[] negative = [long.MinValue, int.MinValue, -255, -1];
        long[] positive = [1, 255, int.MaxValue, long.MaxValue];
        foreach (long comparison in negative)
        {
            Assert.Equal(-1, IndexMutationKernel.CompleteEntryComparison(comparison, long.MaxValue, 255, long.MinValue, 0));
        }

        foreach (long comparison in positive)
        {
            Assert.Equal(1, IndexMutationKernel.CompleteEntryComparison(comparison, long.MinValue, 0, long.MaxValue, 255));
        }

        Assert.Equal(-1, IndexMutationKernel.CompleteEntryComparison(0, long.MinValue, 255, long.MaxValue, 0));
        Assert.Equal(1, IndexMutationKernel.CompleteEntryComparison(0, long.MaxValue, 0, long.MinValue, 255));
        Assert.Equal(-1, IndexMutationKernel.CompleteEntryComparison(0, 10, 0, 10, 255));
        Assert.Equal(1, IndexMutationKernel.CompleteEntryComparison(0, 10, 255, 10, 0));
        Assert.Equal(0, IndexMutationKernel.CompleteEntryComparison(0, 10, 255, 10, 255));
    }

    private static void AssertComparison(IndexEntry left, IndexEntry right, int expected)
    {
        Assert.Equal(expected, IndexPageCodec.CompareEntries(left, right));
        Assert.Equal(expected, IndexMutationKernel.CompareEntries(left.Key, left.DataPage, left.DataRow, right.Key, right.DataPage, right.DataRow));
        Assert.Equal(-expected, IndexPageCodec.CompareEntries(right, left));
        Assert.Equal(-expected, IndexMutationKernel.CompareEntries(right.Key, right.DataPage, right.DataRow, left.Key, left.DataPage, left.DataRow));
    }
}
