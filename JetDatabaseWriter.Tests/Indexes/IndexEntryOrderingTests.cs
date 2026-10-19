namespace JetDatabaseWriter.Tests.Indexes;

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
