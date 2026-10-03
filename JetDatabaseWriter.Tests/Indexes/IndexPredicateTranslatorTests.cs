namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Linq.Expressions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Unit tests for <see cref="IndexPredicateTranslator"/>: confirms which AND-combined
/// comparisons over direct column members are extracted as pushable
/// <see cref="ColumnPredicate"/> values, and that non-seekable shapes (OR branches,
/// computed members, column-to-column comparisons, method calls) are omitted so the
/// residual client-side filter handles them.
/// </summary>
public sealed class IndexPredicateTranslatorTests
{
    private static RowCriteria Extract(Expression<Func<Row, bool>> predicate) =>
        IndexPredicateTranslator.ExtractPushableCriteria(predicate);

    [Fact]
    public void Equality_OnColumn_IsPushed()
    {
        RowCriteria criteria = Extract(r => r.Id == 5);

        ColumnPredicate predicate = Assert.Single(criteria.Predicates);
        Assert.Equal("Id", predicate.ColumnName);
        Assert.Equal(ColumnPredicateOperator.Equal, predicate.Operator);
        Assert.Equal(5, predicate.Operand);
    }

    [Fact]
    public void TwoSidedRange_OnColumn_IsPushedAsTwoPredicates()
    {
        RowCriteria criteria = Extract(r => r.Score >= 10 && r.Score < 20);

        Assert.Equal(2, criteria.Count);
        Assert.Equal(ColumnPredicateOperator.GreaterThanOrEqual, criteria.Predicates[0].Operator);
        Assert.Equal(10, criteria.Predicates[0].Operand);
        Assert.Equal(ColumnPredicateOperator.LessThan, criteria.Predicates[1].Operator);
        Assert.Equal(20, criteria.Predicates[1].Operand);
    }

    [Fact]
    public void MultipleEqualities_AreAllPushed()
    {
        RowCriteria criteria = Extract(r => r.Id == 1 && r.Name == "x");

        Assert.Equal(2, criteria.Count);
        Assert.Equal("Id", criteria.Predicates[0].ColumnName);
        Assert.Equal(1, criteria.Predicates[0].Operand);
        Assert.Equal("Name", criteria.Predicates[1].ColumnName);
        Assert.Equal("x", criteria.Predicates[1].Operand);
    }

    [Fact]
    public void ConstantOnLeft_FlipsOperator()
    {
#pragma warning disable SA1131 // The constant-on-the-left shape is what this test feeds the translator.
        RowCriteria criteria = Extract(r => 10 < r.Score);
#pragma warning restore SA1131

        ColumnPredicate predicate = Assert.Single(criteria.Predicates);
        Assert.Equal("Score", predicate.ColumnName);
        Assert.Equal(ColumnPredicateOperator.GreaterThan, predicate.Operator);
        Assert.Equal(10, predicate.Operand);
    }

    [Fact]
    public void CapturedVariable_IsEvaluated()
    {
#pragma warning disable RCS1118 // A local, not a const: the lambda must capture a closure field.
        int threshold = 7;
#pragma warning restore RCS1118
        RowCriteria criteria = Extract(r => r.Score > threshold);

        ColumnPredicate predicate = Assert.Single(criteria.Predicates);
        Assert.Equal(ColumnPredicateOperator.GreaterThan, predicate.Operator);
        Assert.Equal(7, predicate.Operand);
    }

    [Fact]
    public void CapturedNullableValue_ProducesTheSameCriteriaAsALiteral()
    {
#pragma warning disable RCS1118 // Locals, not consts: the lambdas must capture closure fields.
        int? threshold = 7;
        int optional = 3;
#pragma warning restore RCS1118

        // int > int? lifts the column side; int? == int lifts the captured side.
        AssertSameCriteria(r => r.Score > 7, r => r.Score > threshold);
        AssertSameCriteria(r => r.Optional == 3, r => r.Optional == optional);
    }

    [Fact]
    public void CapturedPropertyChain_ProducesTheSameCriteriaAsALiteral()
    {
        var filter = new Filter { Range = new ScoreRange { Low = 10, High = 20 }, Name = "x", Day = DayOfWeek.Friday };

        AssertSameCriteria(r => r.Score >= 10 && r.Score < 20, r => r.Score >= filter.Range.Low && r.Score < filter.Range.High);
        AssertSameCriteria(r => r.Name == "x", r => r.Name == filter.Name);
        AssertSameCriteria(r => r.Day == DayOfWeek.Friday, r => r.Day == filter.Day);
    }

    [Fact]
    public void OrBranch_IsNotPushed()
    {
        RowCriteria criteria = Extract(r => r.Id == 1 || r.Name == "x");

        Assert.Empty(criteria.Predicates);
    }

    [Fact]
    public void ColumnToColumnComparison_IsNotPushed()
    {
        RowCriteria criteria = Extract(r => r.Id == r.Score);

        Assert.Empty(criteria.Predicates);
    }

    [Fact]
    public void NestedMember_IsNotPushed()
    {
        RowCriteria criteria = Extract(r => r.When.Year == 2024);

        Assert.Empty(criteria.Predicates);
    }

    [Fact]
    public void MethodCall_IsNotPushed()
    {
        RowCriteria criteria = Extract(r => r.Name.StartsWith('A'));

        Assert.Empty(criteria.Predicates);
    }

    [Fact]
    public void AndWithOrSubtree_PushesOnlyTheUnderstoodConjunct()
    {
        RowCriteria criteria = Extract(r => r.Id == 1 && (r.Score == 2 || r.Score == 3));

        ColumnPredicate predicate = Assert.Single(criteria.Predicates);
        Assert.Equal("Id", predicate.ColumnName);
        Assert.Equal(1, predicate.Operand);
    }

    [Fact]
    public void Between_ViaTwoComparisons_PushesBothBounds()
    {
        DateTime start = new(2024, 1, 1);
        DateTime end = new(2024, 12, 31);
        RowCriteria criteria = Extract(r => r.When >= start && r.When <= end);

        Assert.Equal(2, criteria.Count);
        Assert.Equal("When", criteria.Predicates[0].ColumnName);
        Assert.Equal(ColumnPredicateOperator.GreaterThanOrEqual, criteria.Predicates[0].Operator);
        Assert.Equal(start, criteria.Predicates[0].Operand);
        Assert.Equal(ColumnPredicateOperator.LessThanOrEqual, criteria.Predicates[1].Operator);
        Assert.Equal(end, criteria.Predicates[1].Operand);
    }

    private static void AssertSameCriteria(Expression<Func<Row, bool>> literal, Expression<Func<Row, bool>> captured)
    {
        RowCriteria expected = Extract(literal);
        RowCriteria actual = Extract(captured);

        Assert.NotEmpty(expected.Predicates);
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected.Predicates[i].ColumnName, actual.Predicates[i].ColumnName);
            Assert.Equal(expected.Predicates[i].Operator, actual.Predicates[i].Operator);
            Assert.Equal(expected.Predicates[i].Operand, actual.Predicates[i].Operand);
            Assert.Equal(expected.Predicates[i].Operand?.GetType(), actual.Predicates[i].Operand?.GetType());
        }
    }

#pragma warning disable CA1812 // Only appears in expression trees, never instantiated.
    private sealed class Row
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public int Score { get; set; }

        public int? Optional { get; set; }

        public DayOfWeek Day { get; set; }

        public DateTime When { get; set; }
    }

    /// <summary>A captured object whose properties the predicates read through a chain.</summary>
    private sealed class Filter
    {
        public ScoreRange Range { get; set; } = new();

        public string Name { get; set; } = string.Empty;

        public DayOfWeek Day { get; set; }
    }

    /// <summary>The second link of the captured chain.</summary>
    private sealed class ScoreRange
    {
        public int Low { get; set; }

        public int High { get; set; }
    }
}
