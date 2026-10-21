namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Linq.Expressions;
using JetDatabaseWriter.Infrastructure;
using Xunit;

/// <summary>
/// Tests for <see cref="ClosureValueReader"/>: it reads the operand shapes the C# compiler
/// produces for captured values without compiling them, returns <see langword="false"/> for
/// everything else so the caller compiles it, and never reads a value the compiled
/// expression would not produce.
/// </summary>
#pragma warning disable RCS1118 // Locals, not consts: the lambdas must capture closure fields.
public sealed class ClosureValueReaderTests
{
    private const int StaticConstant = 3;

    private static readonly int[] Numbers = [10, 20, 30];

    private readonly int instanceField = 41;

    private static int StaticField { get; } = 17;

    private static string StaticProperty => "static";

    [Fact]
    public void TryRead_Constant_ReadsItsValue()
    {
        AssertReads(Expression.Constant(5), 5);
        AssertReads(Expression.Constant(null, typeof(string)), null);
    }

    [Fact]
    public void TryRead_CapturedLocal_ReadsTheClosureField()
    {
        int id = 5;
        Expression<Func<int>> lambda = () => id;

        Assert.IsType<MemberExpression>(lambda.Body, exactMatch: false);
        AssertReads(lambda.Body, 5);
    }

    [Fact]
    public void TryRead_InstanceField_ReadsThroughThis()
    {
        Expression<Func<int>> lambda = () => this.instanceField;

        AssertReads(lambda.Body, 41);
    }

    [Fact]
    public void TryRead_NestedClosure_ReadsThroughTheEnclosingClosure()
    {
        int outer = 7;
        Expression<Func<int, bool>>? lambda = null;
        for (int i = 0; i < 1; i++)
        {
            int inner = i;
            lambda = x => x == inner && x == outer;
        }

        // `outer` is a field of the enclosing closure, reached through a field of the inner one.
        var right = (BinaryExpression)((BinaryExpression)lambda!.Body).Right;
        MemberExpression outerAccess = Assert.IsType<MemberExpression>(right.Right, exactMatch: false);
        Assert.IsType<MemberExpression>(outerAccess.Expression, exactMatch: false);
        AssertReads(outerAccess, 7);
    }

    [Fact]
    public void TryRead_StaticFieldAndProperty_ReadTheirValues()
    {
        Expression<Func<int[]>> field = () => Numbers;
        Expression<Func<int>> property = () => StaticField;
        Expression<Func<string>> getterOnly = () => StaticProperty;

        AssertReads(field.Body, Numbers);
        AssertReads(property.Body, 17);
        AssertReads(getterOnly.Body, "static");
    }

    [Fact]
    public void TryRead_CapturedObjectPropertyChain_ReadsEachLink()
    {
        var holder = new Holder { Child = new Holder { Value = 12 } };
        Expression<Func<int>> lambda = () => holder.Child!.Value;

        AssertReads(lambda.Body, 12);
    }

    [Fact]
    public void TryRead_CapturedStructField_ReadsTheCopy()
    {
        var point = new Point(3, 4);
        Expression<Func<int>> lambda = () => point.Y;

        AssertReads(lambda.Body, 4);
    }

    [Fact]
    public void TryRead_NullableLift_ReadsTheValue()
    {
        int captured = 5;
        Expression<Func<int?, bool>> lambda = x => x == captured;

        // The compiler lifts the captured int to int? with a Convert.
        UnaryExpression lift = Assert.IsType<UnaryExpression>(((BinaryExpression)lambda.Body).Right, exactMatch: false);
        Assert.Equal(typeof(int?), lift.Type);
        AssertReads(lift, 5);
    }

    [Fact]
    public void TryRead_CapturedNullableValue_ReadsValueAndHasValue()
    {
        int? present = 8;
        int? absent = null;
        Expression<Func<int>> value = () => present.Value;
        Expression<Func<bool>> hasValue = () => present.HasValue;
        Expression<Func<bool>> absentHasValue = () => absent.HasValue;
        Expression<Func<int>> absentValue = () => absent!.Value;

        AssertReads(value.Body, 8);
        AssertReads(hasValue.Body, true);
        AssertReads(absentHasValue.Body, false);

        // Value of a null Nullable<T> throws; only the compiled expression reproduces that.
        Assert.False(ClosureValueReader.TryRead(absentValue.Body, out _));
    }

    [Fact]
    public void TryRead_EnumAndItsUnderlyingType_ConvertBothWays()
    {
        DayOfWeek day = DayOfWeek.Wednesday;
        int number = 5;
        Expression<Func<int>> toInt = () => (int)day;
        Expression<Func<DayOfWeek>> toEnum = () => (DayOfWeek)number;

        AssertReads(toInt.Body, 3);
        AssertReads(toEnum.Body, DayOfWeek.Friday);
    }

    [Fact]
    public void TryRead_LosslessWidening_ReadsTheWidenedValue()
    {
        int number = 5;
        short small = 6;
        float single = 1.5f;
        Expression<Func<long, bool>> toLong = x => x == number;
        Expression<Func<decimal>> toDecimal = () => small;
        Expression<Func<double>> toDouble = () => single;

        AssertReads(((BinaryExpression)toLong.Body).Right, 5L);
        AssertReads(toDecimal.Body, 6m);
        AssertReads(toDouble.Body, 1.5d);
    }

    [Fact]
    public void TryRead_NullCapturedValue_ReadsNull()
    {
        string? name = null;
        int? missing = null;
        Expression<Func<string?>> reference = () => name;
        Expression<Func<long?>> lifted = () => missing;

        AssertReads(reference.Body, null);
        AssertReads(lifted.Body, null);
    }

    [Fact]
    public void TryRead_ComputedOperands_ReturnFalse()
    {
        int id = 5;
        long big = 5;
        int number = 1 << 30;
        Expression<Func<int>> arithmetic = () => id + 1;
        Expression<Func<DateTime>> staticChain = () => DateTime.UtcNow.Date;
        Expression<Func<int>> arrayIndex = () => Numbers[0];
        Expression<Func<int>> narrowing = () => (int)big;
        Expression<Func<float>> rounding = () => number;
        Expression<Func<string>> call = () => id.ToString(System.Globalization.CultureInfo.InvariantCulture);

        Assert.False(ClosureValueReader.TryRead(arithmetic.Body, out _));
        Assert.False(ClosureValueReader.TryRead(staticChain.Body, out _));
        Assert.False(ClosureValueReader.TryRead(arrayIndex.Body, out _));
        Assert.False(ClosureValueReader.TryRead(narrowing.Body, out _));
        Assert.False(ClosureValueReader.TryRead(rounding.Body, out _));
        Assert.False(ClosureValueReader.TryRead(call.Body, out _));
        Assert.Equal(StaticConstant + 3, ClosureValueReader.Evaluate(arithmetic.Body));
    }

    [Fact]
    public void TryRead_NullObjectInTheChain_ReturnsFalseAndEvaluateThrowsLikeTheCompiledExpression()
    {
        Holder? holder = null;
        Expression<Func<int>> lambda = () => holder!.Value;

        Assert.False(ClosureValueReader.TryRead(lambda.Body, out _));
        Assert.Throws<NullReferenceException>(() => ClosureValueReader.Evaluate(lambda.Body));
        Assert.Throws<NullReferenceException>(() => lambda.Compile()());
    }

    [Fact]
    public void TryRead_PropertyGetterThatThrows_ThrowsTheSameExceptionAsTheCompiledExpression()
    {
        var holder = new Holder();
        Expression<Func<int>> lambda = () => holder.Throwing;

        Exception read = Assert.Throws<InvalidOperationException>(() => ClosureValueReader.TryRead(lambda.Body, out _));
        Exception compiled = Assert.Throws<InvalidOperationException>(() => lambda.Compile()());
        Assert.Equal(compiled.Message, read.Message);
    }

    [Fact]
    public void Evaluate_ReadableAndComputedOperands_MatchTheCompiledExpression()
    {
        int id = 9;
        Expression<Func<int>> captured = () => id;
        Expression<Func<long>> computed = () => (id * 2L) + 1;

        Assert.Equal(captured.Compile()(), ClosureValueReader.Evaluate(captured.Body));
        Assert.Equal(computed.Compile()(), ClosureValueReader.Evaluate(computed.Body));
    }

    private static void AssertReads(Expression expression, object? expected)
    {
        Assert.True(ClosureValueReader.TryRead(expression, out object? value));
        Assert.Equal(expected, value);
        if (expected is not null)
        {
            Assert.IsType(expected.GetType(), value);
        }
    }

    /// <summary>A captured object with a nested reference, a value and a throwing getter.</summary>
    private sealed class Holder
    {
        public Holder? Child { get; set; }

        public int Value { get; set; }

        public int Throwing => throw new InvalidOperationException($"The getter threw with Value {this.Value}.");
    }

    /// <summary>A captured struct.</summary>
    /// <param name="X">The first coordinate.</param>
    /// <param name="Y">The second coordinate.</param>
    private readonly record struct Point(int X, int Y);
}
#pragma warning restore RCS1118
