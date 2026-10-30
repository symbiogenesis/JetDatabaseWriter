namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using Xunit;

/// <summary>Stable ordering during leaf splices and intermediate mutations.</summary>
public sealed class IndexEntryOrderingTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Splice_PreservesTieOrderAcrossSortedAndUnorderedRuns(bool reverseExisting, bool reverseAdds)
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
            [0, 4, reverseExisting ? 2 : 1, reverseExisting ? 1 : 2, reverseAdds ? 6 : 5, reverseAdds ? 5 : 6, 3, 7],
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

    [Fact]
    public void Splice_SortsUnorderedInputsAndPreservesEqualKeyDeclarationOrder()
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

    private static IndexEntry Entry(byte key, byte row) => new([key], 100, row);

    private static DecodedIntermediateEntry Child(byte page) => new(Entry(page, page), page);
}
