namespace JetDatabaseWriter.Tests.Indexes;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Unit tests for <see cref="IndexPlanner"/>: confirms the leading-key-column match
/// rule, the exact / key-prefix / range criteria it builds, descending-range
/// exclusion, candidate skipping, and the deterministic tie-breaks used to pick one
/// index over another.
/// </summary>
public sealed class IndexPlannerTests
{
    [Fact]
    public void SingleColumnEquality_ProducesExactSeek()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_Id", firstDp: 10, indexNumber: 0, unique: false, ("Id", true)) };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("Id", 5) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal("IX_Id", plan.Index.Name);
        Assert.Equal(IndexQueryKind.Exact, plan.Criteria.Kind);
        Assert.Equal(new object?[] { 5 }, plan.Criteria.Values);
        Assert.Equal(1, plan.MatchedKeyColumns);
    }

    [Fact]
    public void CompositeLeadingEquality_ProducesKeyPrefix()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_AB", firstDp: 11, indexNumber: 0, unique: false, ("A", true), ("B", true)) };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("A", 1) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal(IndexQueryKind.KeyPrefix, plan.Criteria.Kind);
        Assert.Equal(new object?[] { 1 }, plan.Criteria.Values);
        Assert.Equal(1, plan.MatchedKeyColumns);
    }

    [Fact]
    public void CompositeFullEquality_ProducesExact()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_AB", firstDp: 11, indexNumber: 0, unique: false, ("A", true), ("B", true)) };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("A", 1), ColumnPredicate.EqualTo("B", 2) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal(IndexQueryKind.Exact, plan.Criteria.Kind);
        Assert.Equal(new object?[] { 1, 2 }, plan.Criteria.Values);
        Assert.Equal(2, plan.MatchedKeyColumns);
    }

    [Fact]
    public void EqualityPrefixPlusRange_ProducesRange()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_AB", firstDp: 11, indexNumber: 0, unique: false, ("A", true), ("B", true)) };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("A", 1), ColumnPredicate.GreaterThan("B", 2) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal(IndexQueryKind.Range, plan.Criteria.Kind);
        Assert.Equal(2, plan.MatchedKeyColumns);

        Assert.NotNull(plan.Criteria.Lower);
        Assert.Equal(new object?[] { 1, 2 }, plan.Criteria.Lower.Values);
        Assert.False(plan.Criteria.Lower.IsInclusive);

        Assert.NotNull(plan.Criteria.Upper);
        Assert.Equal(new object?[] { 1 }, plan.Criteria.Upper.Values);
        Assert.True(plan.Criteria.Upper.IsInclusive);
    }

    [Fact]
    public void SingleColumnBetween_ProducesInclusiveRange()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_A", firstDp: 12, indexNumber: 0, unique: false, ("A", true)) };
        var criteria = new RowCriteria { ColumnPredicate.Between("A", 5, 9) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal(IndexQueryKind.Range, plan.Criteria.Kind);
        Assert.NotNull(plan.Criteria.Lower);
        Assert.Equal(new object?[] { 5 }, plan.Criteria.Lower.Values);
        Assert.True(plan.Criteria.Lower.IsInclusive);
        Assert.NotNull(plan.Criteria.Upper);
        Assert.Equal(new object?[] { 9 }, plan.Criteria.Upper.Values);
        Assert.True(plan.Criteria.Upper.IsInclusive);
    }

    [Fact]
    public void UpperBoundOnly_ProducesRangeWithNoLower()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_A", firstDp: 12, indexNumber: 0, unique: false, ("A", true)) };
        var criteria = new RowCriteria { ColumnPredicate.LessThanOrEqual("A", 9) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal(IndexQueryKind.Range, plan.Criteria.Kind);
        Assert.Null(plan.Criteria.Lower);
        Assert.NotNull(plan.Criteria.Upper);
        Assert.Equal(new object?[] { 9 }, plan.Criteria.Upper.Values);
        Assert.True(plan.Criteria.Upper.IsInclusive);
    }

    [Fact]
    public void DescendingRangeColumn_IsNotPushed()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_A", firstDp: 12, indexNumber: 0, unique: false, ("A", false)) };
        var criteria = new RowCriteria { ColumnPredicate.GreaterThan("A", 2) };

        Assert.Null(IndexPlanner.TryPlan(indexes, criteria));
    }

    [Fact]
    public void EqualityOnDescendingColumn_IsAllowed()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_A", firstDp: 12, indexNumber: 0, unique: false, ("A", false)) };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("A", 2) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal(IndexQueryKind.Exact, plan.Criteria.Kind);
    }

    [Fact]
    public void NoMatchingColumn_ReturnsNull()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_Other", firstDp: 12, indexNumber: 0, unique: false, ("Other", true)) };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("Id", 5) };

        Assert.Null(IndexPlanner.TryPlan(indexes, criteria));
    }

    [Fact]
    public void UnbuiltIndex_IsSkipped()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_Id", firstDp: 0, indexNumber: 0, unique: false, ("Id", true)) };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("Id", 5) };

        Assert.Null(IndexPlanner.TryPlan(indexes, criteria));
    }

    [Fact]
    public void EmptyCriteria_ReturnsNull()
    {
        var indexes = new List<IndexMetadata> { MakeIndex("IX_Id", firstDp: 10, indexNumber: 0, unique: false, ("Id", true)) };

        Assert.Null(IndexPlanner.TryPlan(indexes, new RowCriteria()));
    }

    [Fact]
    public void PrefersIndexThatMatchesMoreKeyColumns()
    {
        var indexes = new List<IndexMetadata>
        {
            MakeIndex("IX_A", firstDp: 10, indexNumber: 0, unique: false, ("A", true)),
            MakeIndex("IX_AB", firstDp: 11, indexNumber: 1, unique: false, ("A", true), ("B", true)),
        };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("A", 1), ColumnPredicate.EqualTo("B", 2) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal("IX_AB", plan.Index.Name);
        Assert.Equal(2, plan.MatchedKeyColumns);
    }

    [Fact]
    public void TieBreak_PrefersUniqueIndex()
    {
        var indexes = new List<IndexMetadata>
        {
            MakeIndex("IX_Id_NonUnique", firstDp: 10, indexNumber: 1, unique: false, ("Id", true)),
            MakeIndex("IX_Id_Unique", firstDp: 11, indexNumber: 0, unique: true, ("Id", true)),
        };
        var criteria = new RowCriteria { ColumnPredicate.EqualTo("Id", 5) };

        IndexPlan? plan = IndexPlanner.TryPlan(indexes, criteria);

        Assert.NotNull(plan);
        Assert.Equal("IX_Id_Unique", plan.Index.Name);
    }

    private static IndexMetadata MakeIndex(string name, int firstDp, int indexNumber, bool unique, params (string Col, bool Asc)[] cols)
    {
        var columns = new List<IndexColumnReference>();
        foreach ((string col, bool asc) in cols)
        {
            columns.Add(new IndexColumnReference { Name = col, ColumnNumber = 0, IsAscending = asc });
        }

        return new IndexMetadata
        {
            Name = name,
            IndexNumber = indexNumber,
            HasUniqueFlag = unique,
            Kind = IndexKind.Normal,
            Columns = columns,
            FirstDp = firstDp,
        };
    }
}
