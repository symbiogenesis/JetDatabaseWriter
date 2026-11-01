namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using Xunit;

/// <summary>Native key/bookmark ordering during leaf splices and intermediate mutations.</summary>
public sealed class IndexEntryOrderingTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Splice_OrdersBookmarksAcrossSortedAndUnorderedRuns(bool reverseExisting, bool reverseAdds)
    {
        List<IndexEntry> existing = [Entry(0, 0), Entry(2, 1), Entry(2, 2), Entry(4, 3)];
        List<IndexEntry> adds = [Entry(0, 4), Entry(2, 5), Entry(2, 6), Entry(5, 7)];
        if (reverseExisting)
        {
            existing.Reverse();
        }

        if (reverseAdds)
        {
            adds.Reverse();
        }

        IndexEntry[] original = [.. existing];
        IndexEntry[] additions = [.. adds];

        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, []);

        Assert.NotNull(result);
        Assert.Equal<int>(
            [0, 4, 1, 2, 5, 6, 3, 7],
            result.Select(entry => (int)entry.DataRow));
        Assert.Equal(original, existing);
        Assert.Equal(additions, adds);
    }

    [Fact]
    public void Splice_MergesSortedSurvivorsAfterRemovingAnUnorderedEntry()
    {
        List<IndexEntry> existing = [Entry(3, 0), Entry(1, 1), Entry(2, 2)];
        List<IndexEntry> adds = [Entry(0, 3), Entry(2, 4)];

        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, [(100, 0)]);

        Assert.NotNull(result);
        Assert.Equal([3, 1, 2, 4], result.Select(entry => entry.DataRow));
        Assert.Equal([0, 1, 2], existing.Select(entry => entry.DataRow));
        Assert.Equal([3, 4], adds.Select(entry => entry.DataRow));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Splice_PreservesLongSharedPrefixKeysAndDuplicateOrder(bool reverseAdditions)
    {
        var existing = new List<IndexEntry>(600);
        var adds = new List<IndexEntry>(60);
        for (int i = 0; i < 600; i++)
        {
            byte[] key = new byte[128];
            key[0] = checked((byte)(i / 100));
            key[^1] = checked((byte)(i % 100));
            existing.Add(new(key, 100 + i, 0));
            if (i % 10 == 0)
            {
                adds.Add(new(key, 100 + i, 1));
            }
        }

        IndexEntry[] ordered = [.. existing];
        if (reverseAdditions)
        {
            adds.Reverse();
        }

        IndexEntry[] original = [.. existing];
        IndexEntry[] additions = [.. adds];

        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, []);

        Assert.NotNull(result);
        Assert.Equal(660, result.Count);
        int offset = 0;
        for (int i = 0; i < ordered.Length; i++)
        {
            Assert.Equal(ordered[i], result[offset++]);
            if (i % 10 == 0)
            {
                Assert.Equal(ordered[i] with { DataRow = 1 }, result[offset++]);
            }
        }

        Assert.Equal(original, existing);
        Assert.Equal(additions, adds);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(0, 3)]
    [InlineData(3, 0)]
    public void Splice_HandlesEmptySortedRuns(int existingCount, int addedCount)
    {
        List<IndexEntry> existing = [.. Enumerable.Range(0, existingCount).Select(row => Entry(0, checked((byte)row)))];
        List<IndexEntry> adds = [.. Enumerable.Range(existingCount, addedCount).Select(row => Entry(0, checked((byte)row)))];

        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, []);

        Assert.NotNull(result);
        Assert.NotSame(existing, result);
        Assert.Equal(Enumerable.Range(0, existingCount + addedCount).Select(row => checked((byte)row)), result.Select(entry => entry.DataRow));
    }

    [Fact]
    public void Splice_SortedRunsAllocateOnlyTheResultStorage()
    {
        const int entryCount = 4096;
        byte[] key = [1];
        var existing = new List<IndexEntry>(entryCount);
        for (int i = 0; i < entryCount; i++)
        {
            existing.Add(new IndexEntry(key, i + 100, 0));
        }

        List<IndexEntry> adds = [new(key, 5000, 0)];
        _ = IndexEntrySplicer.Splice(existing, adds, []);

        // The synchronous call stays on this thread. Allow object/header slack
        // while refusing a second entry buffer or a whole-input sorting buffer.
        long before = GC.GetAllocatedBytesForCurrentThread();
        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, []);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.NotNull(result);
        Assert.Equal(entryCount + 1, result.Count);
        Assert.Equal(5000, result[^1].DataPage);
        Assert.InRange(allocated, 0, ((entryCount + 1L) * 32) + 4096);
    }

    /// <summary>Unordered runs sort the private result buffer without a second entry or projected-key buffer.</summary>
    [Fact]
    public void Splice_UnorderedRunsAllocateOnlyTheResultStorage()
    {
        const int existingCount = 4096;
        const int addedCount = 128;
        byte[] key = new byte[128];
        Array.Fill(key, (byte)0x80);
        byte[] originalKey = [.. key];
        var existing = new List<IndexEntry>(existingCount);
        var adds = new List<IndexEntry>(addedCount);
        for (int index = existingCount - 1; index >= 0; index--)
        {
            existing.Add(new(key, 100 + index, 0));
        }

        for (int index = addedCount - 1; index >= 0; index--)
        {
            adds.Add(new(key, 5000 + index, 0));
        }

        IndexEntry[] original = [.. existing];
        IndexEntry[] additions = [.. adds];
        _ = IndexEntrySplicer.Splice(existing, adds, []);

        long before = GC.GetAllocatedBytesForCurrentThread();
        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, []);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.NotNull(result);
        Assert.Equal(existingCount + addedCount, result.Count);
        Assert.InRange(allocated, 0, ((existingCount + addedCount) * 32L) + 4096);
        for (int index = 0; index < result.Count; index++)
        {
            Assert.Same(key, result[index].Key);
            Assert.Equal(index < existingCount ? 100 + index : 5000 + index - existingCount, result[index].DataPage);
            Assert.Equal(0, result[index].DataRow);
        }

        Assert.Equal(original, existing);
        Assert.Equal(additions, adds);
        Assert.Equal(originalKey, key);
    }

    /// <summary>Distinct arrays with equal complete native entries retain their bytes and member identities.</summary>
    /// <param name="format">The leaf encoding format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Splice_CompleteEntryTiesPreserveNativeBytesAndInputKeys(DatabaseFormat format)
    {
        var entries = new List<IndexEntry>();
        for (int index = 0; index < 96; index++)
        {
            entries.Add(new([checked((byte)(index % 4)), 0x80], 100 + (index % 3), checked((byte)(index % 2))));
        }

        List<IndexEntry> existing = entries.GetRange(0, 64);
        List<IndexEntry> adds = entries.GetRange(64, 32);
        IndexEntry[] original = [.. existing];
        IndexEntry[] additions = [.. adds];
        byte[][] originalKeys = entries.Select(static entry => entry.Key.ToArray()).ToArray();
        var expected = new List<IndexEntry>();
        for (byte key = 0; key < 4; key++)
        {
            for (long page = 100; page < 103; page++)
            {
                for (byte row = 0; row < 2; row++)
                {
                    expected.AddRange(entries.Where(entry => entry.Key[0] == key && entry.DataPage == page && entry.DataRow == row));
                }
            }
        }

        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, []);

        Assert.NotNull(result);
        Assert.Equal(entries.Count, result.Count);
        Assert.True(entries.Select(static entry => entry.Key).ToHashSet().SetEquals(result.Select(static entry => entry.Key)));
        var profile = JetFormat.ForNewDatabase(format);
        byte[] expectedPage = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, expected, enablePrefixCompression: true);
        byte[] actualPage = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, result, enablePrefixCompression: true);
        Assert.Equal(expectedPage, actualPage);
        Assert.Equal(original, existing);
        Assert.Equal(additions, adds);
        for (int index = 0; index < entries.Count; index++)
        {
            Assert.Equal(originalKeys[index], entries[index].Key);
        }
    }

    [Fact]
    public void Splice_SortsUnorderedInputsByKeyAndBookmark()
    {
        List<IndexEntry> existing = [Entry(3, 0), Entry(1, 1), Entry(3, 2), Entry(2, 3)];
        List<IndexEntry> adds = [Entry(3, 4), Entry(0, 5), Entry(3, 6), Entry(2, 7)];

        List<IndexEntry>? result = IndexEntrySplicer.Splice(existing, adds, [(100, 3)]);

        Assert.NotNull(result);
        Assert.Equal([5, 1, 7, 0, 2, 4, 6], result.Select(entry => entry.DataRow));
        Assert.Equal([0, 1, 2, 3], existing.Select(entry => entry.DataRow));
        Assert.Equal([4, 5, 6, 7], adds.Select(entry => entry.DataRow));
    }

    [Fact]
    public void Splice_DuplicateRemovalTargetIsRefused()
    {
        List<IndexEntry> existing = [Entry(2, 0), Entry(1, 1)];

        Assert.Null(IndexEntrySplicer.Splice(existing, [], [(100, 0), (100, 0)]));
        Assert.Equal([0, 1], existing.Select(entry => entry.DataRow));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    public void Splice_MatchesIndependentNativeBucketsForEverySmallRunAndRemovalSubset(int removalMask)
    {
        // This alphabet orders empty, prefix, longer-prefix and high-byte keys
        // independently of the production comparator and sorting algorithm.
        byte[][] alphabet = [[], [0x7F], [0x7F, 0], [0xFF]];
        for (int sample = 0; sample < 1024; sample++)
        {
            int[] ranks = new int[5];
            var entries = new IndexEntry[5];
            byte[][] originalKeys = new byte[5][];
            for (int index = 0; index < entries.Length; index++)
            {
                ranks[index] = (sample >> (index * 2)) & 3;
                byte[] key = [.. alphabet[ranks[index]]];
                originalKeys[index] = [.. key];
                entries[index] = new(key, 100 + index, 0);
            }

            List<IndexEntry> existing = [entries[0], entries[1], entries[2]];
            List<IndexEntry> adds = [entries[3], entries[4]];
            var removes = new List<(long Page, byte Row)>();
            for (int index = 0; index < existing.Count; index++)
            {
                if ((removalMask & (1 << index)) != 0)
                {
                    removes.Add((entries[index].DataPage, entries[index].DataRow));
                }
            }

            var expected = new List<IndexEntry>();
            for (int rank = 0; rank < alphabet.Length; rank++)
            {
                for (int index = 0; index < entries.Length; index++)
                {
                    if (ranks[index] == rank && (index >= existing.Count || (removalMask & (1 << index)) == 0))
                    {
                        expected.Add(entries[index]);
                    }
                }
            }

            List<IndexEntry>? actual = IndexEntrySplicer.Splice(existing, adds, removes);

            Assert.NotNull(actual);
            Assert.Equal(expected, actual);
            Assert.Equal(entries.Take(3), existing);
            Assert.Equal(entries.Skip(3), adds);
            for (int index = 0; index < entries.Length; index++)
            {
                Assert.Equal(originalKeys[index], entries[index].Key);
            }
        }
    }

    [Fact]
    public void ApplyIntermediateOps_SortsOriginalIndicesAndPreservesDeclarationOrderWithinTies()
    {
        List<DecodedIntermediateEntry> original = [Child(1), Child(2), Child(3)];
        List<IntermediateOp> operations =
        [
            new(2, IntermediateOpType.Remove, default),
            new(0, IntermediateOpType.Replace, Child(4)),
            new(1, IntermediateOpType.InsertAfter, Child(7)),
            new(0, IntermediateOpType.InsertAfter, Child(5)),
            new(0, IntermediateOpType.InsertAfter, Child(6)),
        ];

        List<DecodedIntermediateEntry> result = IndexHelpers.ApplyIntermediateOps(original, operations);

        Assert.Equal([4, 5, 6, 2, 7], result.Select(entry => entry.ChildPage));
        Assert.Equal([1, 2, 3], original.Select(entry => entry.ChildPage));
        Assert.Equal([2, 0, 1, 0, 0], operations.Select(operation => operation.OriginalIndex));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ApplyIntermediateOps_ConflictingOperationsAreRefused(bool replaceFirst)
    {
        IntermediateOpType first = replaceFirst ? IntermediateOpType.Replace : IntermediateOpType.Remove;
        IntermediateOpType second = replaceFirst ? IntermediateOpType.Remove : IntermediateOpType.Replace;
        List<IntermediateOp> operations = [new(0, first, Child(2)), new(0, second, Child(3))];

        Assert.Empty(IndexHelpers.ApplyIntermediateOps([Child(1)], operations));
    }

    [Fact]
    public void CompareEntries_OrdersUnsignedKeyBytesThenPageAndRow()
    {
        byte[][] keys = [[], [0], [0, 0], [0x7F], [0x80], [0xFF]];
        long[] pages = [1, 255, 256, 65536, 0xFFFFFF];
        byte[] rows = [0, 1, 255];
        var entries = new List<IndexEntry>();
        foreach (byte[] key in keys)
        {
            foreach (long page in pages)
            {
                foreach (byte row in rows)
                {
                    entries.Add(new(key, page, row));
                }
            }
        }

        for (int left = 0; left < entries.Count; left++)
        {
            for (int right = 0; right < entries.Count; right++)
            {
                Assert.Equal(Math.Sign(left - right), IndexPageCodec.CompareEntries(entries[left], entries[right]));
            }
        }
    }

    [Fact]
    public void MutationDescent_SeparatesDuplicateKeysByBookmark()
    {
        List<DecodedIntermediateEntry> summaries = [new(new([1], 100, 10), 2), new(new([1], 100, 20), 3), new(new([1], 101, 0), 4)];
        Assert.Equal(0, IndexHelpers.SelectChildIndexFromDecoded(summaries, new([1], 100, 10)));
        Assert.Equal(1, IndexHelpers.SelectChildIndexFromDecoded(summaries, new([1], 100, 11)));
        Assert.Equal(2, IndexHelpers.SelectChildIndexFromDecoded(summaries, new([1], 100, 21)));
        Assert.Equal(-1, IndexHelpers.SelectChildIndexFromDecoded(summaries, new([1], 101, 1)));
        List<DescentStep> path = [new(1, [], summaries, 1)];
        Assert.True(IndexHelpers.ConfirmEntryTargetsSamePath(path, new([1], 100, 20)));
        Assert.False(IndexHelpers.ConfirmEntryTargetsSamePath(path, new([1], 100, 10)));
        Assert.False(IndexHelpers.ConfirmEntryTargetsSamePath(path, new([1], 100, 21)));
    }

    private static IndexEntry Entry(byte key, byte row) => new([key], 100, row);

    private static DecodedIntermediateEntry Child(byte page) => new(Entry(page, page), page);
}
