namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Linq.Expressions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Unit tests for <see cref="IndexPredicateTranslator"/>: confirms which AND-combined
/// comparisons over direct column members are extracted as pushable
/// <see cref="ColumnPredicate"/> values until a non-seekable shape (OR branches,
/// computed members, column-to-column comparisons, method calls) stops extraction.
/// That shape and later conjuncts remain in the residual client-side filter.
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
    public void CapturedPropertyChain_RemainsResidual()
    {
        var filter = new Filter { Range = new ScoreRange { Low = 10, High = 20 }, Name = "x", Day = DayOfWeek.Friday };

        Assert.Empty(Extract(r => r.Score >= filter.Range.Low && r.Score < filter.Range.High).Predicates);
        Assert.Empty(Extract(r => r.Name == filter.Name).Predicates);
        Assert.Empty(Extract(r => r.Day == filter.Day).Predicates);
    }

    [Fact]
    public void ComputedOperand_KeepsOnlyLeadingSeekCriteria()
    {
        RowCriteria criteria = Extract(r => r.Id == 1 && r.Score == ThrowValue() && r.Name == "x");

        ColumnPredicate predicate = Assert.Single(criteria.Predicates);
        Assert.Equal("Id", predicate.ColumnName);
        Assert.Equal(1, predicate.Operand);
        Assert.Empty(Extract(r => ThrowValue() == r.Score && r.Id == 1).Predicates);
    }

    [Fact]
    public void CapturedPropertyBeforeComparison_BlocksLaterSeekCriteria()
    {
        var filter = new Filter();

        Assert.Empty(Extract(r => r.Name == filter.Name && r.Id == 1).Predicates);
        Assert.Empty(Extract(r => r.When >= DateTime.UtcNow && r.Id == 1).Predicates);
    }

    [Fact]
    public void CapturedVariableChanges_AreReadOnEveryExtraction()
    {
        int threshold = 7;
        Expression<Func<Row, bool>> predicate = r => r.Score > threshold;

        Assert.Equal(7, Assert.Single(Extract(predicate).Predicates).Operand);
        threshold = 9;
        Assert.Equal(9, Assert.Single(Extract(predicate).Predicates).Operand);
    }

    [Fact]
    public void LossyColumnConversions_AreNotPushed()
    {
        Assert.Empty(Extract(r => unchecked((byte)r.Id) == 1).Predicates);
        Assert.Empty(Extract(r => checked((byte)r.Id) == 1).Predicates);
        Assert.Empty(Extract(r => unchecked((byte)(long)r.Id) < 2).Predicates);
        Assert.Empty(Extract(r => (int)(float)r.Id == 16_777_216).Predicates);
        Assert.Empty(Extract(r => (double)(float)r.Id == 16_777_216.0).Predicates);
#pragma warning disable CS8629 // Nullable unwrapping must stay residual so its exception is preserved.
        Assert.Empty(Extract(r => (int)r.Optional == 1).Predicates);
#pragma warning restore CS8629
    }

    [Fact]
    public void LosslessColumnConversions_ArePushed()
    {
#pragma warning disable IDE0004 // Explicit casts exercise conversion nodes in the predicate tree.
        Assert.Single(Extract(r => (long)r.Id == 1L).Predicates);
        Assert.Single(Extract(r => (double)r.Id == 1.0).Predicates);
        Assert.Single(Extract(r => (decimal)r.Id == 1m).Predicates);
        Assert.Single(Extract(r => (long?)r.Id == 1L).Predicates);
        Assert.Single(Extract(r => (long?)r.Optional == 1L).Predicates);
        Assert.Single(Extract(r => (int)r.Day == 1).Predicates);
#pragma warning restore IDE0004
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
    public void ResidualBeforeComparison_BlocksLaterSeekCriteria()
    {
        Assert.Empty(Extract(r => checked((byte)r.Id) == 1 && r.Id == 1).Predicates);
        Assert.Empty(Extract(r => r.Name.StartsWith('A') && r.Id == 1).Predicates);
#pragma warning disable CS8629 // Nullable unwrapping must run before the later equality.
        Assert.Empty(Extract(r => (int)r.Optional == 1 && r.Id == 1).Predicates);
#pragma warning restore CS8629
    }

    [Fact]
    public void CustomComparisonBeforeEquality_BlocksLaterSeekCriteria()
    {
        Expression<Func<int, int, bool>> comparison = (left, right) => ThrowingComparison(left, right);
        ParameterExpression parameter = Expression.Parameter(typeof(Row), "r");
        MemberExpression id = Expression.Property(parameter, nameof(Row.Id));
        BinaryExpression first = Expression.Equal(id, Expression.Constant(1), liftToNull: false, ((MethodCallExpression)comparison.Body).Method);
        var predicate = Expression.Lambda<Func<Row, bool>>(
            Expression.AndAlso(first, Expression.Equal(id, Expression.Constant(1))), parameter);

        Assert.Empty(Extract(predicate).Predicates);
    }

    [Fact]
    public void ResidualBetweenComparisons_KeepsOnlyLeadingSeekCriteria()
    {
#pragma warning disable IDE0047 // Keep a right-nested AND to exercise evaluation-order flattening.
        RowCriteria criteria = Extract(r => r.Score >= 0 && (checked((byte)r.Id) == 1 && r.Id == 1));
#pragma warning restore IDE0047

        ColumnPredicate predicate = Assert.Single(criteria.Predicates);
        Assert.Equal("Score", predicate.ColumnName);
        Assert.Equal(ColumnPredicateOperator.GreaterThanOrEqual, predicate.Operator);
        Assert.Equal(0, predicate.Operand);
    }

    [Fact]
    public void ResidualAfterComparison_KeepsLeadingSeekCriteria()
    {
        RowCriteria criteria = Extract(r => r.Id == 1 && checked((byte)r.Id) == 1);

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

    private static bool ThrowingComparison(int left, int right) => throw new InvalidOperationException($"Comparison: {left}, {right}.");

    private static int ThrowValue() => throw new InvalidOperationException("Seek operand was evaluated.");

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
