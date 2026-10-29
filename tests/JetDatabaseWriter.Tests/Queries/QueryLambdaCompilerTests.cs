namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using JetDatabaseWriter.Queries;
using Xunit;

/// <summary>Cached lambda templates bind live values without retaining another execution's captures.</summary>
public sealed class QueryLambdaCompilerTests
{
    [Fact]
    public void SameShape_UsesEachClosureAndReadsLaterChanges()
    {
        var cache = new QueryLambdaCompiler(4);
        var first = new Bound { Value = 3 };
        var second = new Bound { Value = 7 };
        Func<int, bool> a = cache.Compile(Predicate(first));
        Func<int, bool> b = cache.Compile(Predicate(second));

        Assert.Equal(1, cache.Count);
        Assert.Equal(a.Method, b.Method);
        Assert.True(a(3));
        Assert.False(a(7));
        Assert.True(b(7));
        first.Value = 9;
        Assert.True(a(9));
        Assert.False(a(3));
        Assert.True(b(7));
    }

    [Fact]
    public void CapturedGetter_RemainsDeferredAndShortCircuited()
    {
        var cache = new QueryLambdaCompiler(4);
        var bound = new Bound { Value = 7, ThrowOnRead = true };
        Expression<Func<int, bool>> expression = value => value > 0 && value == bound.Read();
        Func<int, bool> predicate = cache.Compile(expression);

        Assert.Equal(0, bound.Reads);
        Assert.False(predicate(0));
        Assert.Equal(0, bound.Reads);
        Assert.Throws<InvalidOperationException>(() => predicate(7));
        Assert.Equal(1, bound.Reads);
        bound.ThrowOnRead = false;
        Assert.True(predicate(7));
        Assert.Equal(2, bound.Reads);
    }

    [Fact]
    public void ParameterOrderAndMembers_DistinguishTemplates()
    {
        var cache = new QueryLambdaCompiler(4);
        Expression<Func<int, int, int>> subtract = (left, right) => left - right;
        Expression<Func<int, int, int>> reverse = (left, right) => right - left;
        Expression<Func<Bound, int>> value = bound => bound.Value;
        Expression<Func<Bound, int>> reads = bound => bound.Reads;

        Assert.Equal(3, cache.Compile(subtract)(7, 4));
        Assert.Equal(-3, cache.Compile(reverse)(7, 4));
        Assert.Equal(9, cache.Compile(value)(new Bound { Value = 9 }));
        Assert.Equal(0, cache.Compile(reads)(new Bound { Value = 9 }));
        Assert.Equal(4, cache.Count);
    }

    [Fact]
    public void Eviction_DoesNotInvalidateAlreadyBoundDelegates()
    {
        var cache = new QueryLambdaCompiler(2);
        Func<int, int> first = cache.Compile<Func<int, int>>(value => value + 1);
        Func<int, int> second = cache.Compile<Func<int, int>>(value => value - 1);
        _ = cache.Compile<Func<int, int>>(value => value * 2);
        Func<int, int> recompiled = cache.Compile<Func<int, int>>(value => value + 7);

        Assert.Equal(2, cache.Count);
        Assert.NotEqual(first.Method, recompiled.Method);
        Assert.Equal(5, first(4));
        Assert.Equal(3, second(4));
        Assert.Equal(11, recompiled(4));
    }

    [Fact]
    public async Task ConcurrentSameShape_BindsIndependentConstantsOnce()
    {
        var cache = new QueryLambdaCompiler(4);
        Func<int, bool>[] predicates = await Task.WhenAll(Enumerable.Range(1, 16).Select(value => Task.Run(
            () => cache.Compile(Predicate(new Bound { Value = value })), TestContext.Current.CancellationToken)));

        Assert.Equal(1, cache.Count);
        for (int i = 0; i < predicates.Length; i++)
        {
            Assert.Equal(predicates[0].Method, predicates[i].Method);
            Assert.True(predicates[i](i + 1));
            Assert.False(predicates[i](i + 2));
        }
    }

    [Fact]
    public void FailedCompilation_IsNotCachedAndDoesNotPoisonLaterTemplates()
    {
        var cache = new QueryLambdaCompiler(4);
        ParameterExpression missing = Expression.Parameter(typeof(int), "missing");
        var invalid = Expression.Lambda<Func<int>>(missing);

        Assert.Throws<InvalidOperationException>(() => cache.Compile(invalid));
        Assert.Throws<InvalidOperationException>(() => cache.Compile(invalid));
        Assert.Equal(0, cache.Count);
        Assert.Equal(9, cache.Compile<Func<int>>(() => 9)());
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void UnsupportedControlFlow_PreservesDirectCompilation()
    {
        var cache = new QueryLambdaCompiler(4);
        ParameterExpression local = Expression.Variable(typeof(int), "local");
        var block = Expression.Lambda<Func<int>>(Expression.Block(
            [local], Expression.Assign(local, Expression.Constant(7)), local));

        Assert.Equal(7, cache.Compile(block)());
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void QuotedExpression_PreservesItsObservableTree()
    {
        var cache = new QueryLambdaCompiler(4);
        var bound = new Bound { Value = 7 };
        Expression<Func<Expression<Func<int>>>> expression = () => () => bound.Value;

        Expression<Func<int>> result = cache.Compile(expression)();

        Assert.Equal(ExpressionType.MemberAccess, result.Body.NodeType);
        Assert.Equal(7, result.Compile()());
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void NestedLambdas_KeepTheirDistinctParameterScopes()
    {
        var cache = new QueryLambdaCompiler(4);
        Expression<Func<int, Func<int, int>>> first = outer => inner => outer - inner + 3;
        Expression<Func<int, Func<int, int>>> second = outer => inner => outer - inner + 7;

        Assert.Equal(9, cache.Compile(first)(10)(4));
        Assert.Equal(13, cache.Compile(second)(10)(4));
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void DeepShape_UsesUncachedCompilation()
    {
        var cache = new QueryLambdaCompiler(4);
        Expression body = Expression.Constant(1);
        for (int i = 0; i < 100; i++)
        {
            body = Expression.Add(body, Expression.Constant(1));
        }

        Assert.Equal(101, cache.Compile(Expression.Lambda<Func<int>>(body))());
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void CheckedArithmetic_DoesNotReuseUncheckedSemantics()
    {
        var cache = new QueryLambdaCompiler(4);
        Expression<Func<int, int>> checkedAdd = value => checked(value + 1);
        Expression<Func<int, int>> uncheckedAdd = value => unchecked(value + 1);

        Assert.Throws<OverflowException>(() => cache.Compile(checkedAdd)(int.MaxValue));
        Assert.Equal(int.MinValue, cache.Compile(uncheckedAdd)(int.MaxValue));
        Assert.Equal(2, cache.Count);
    }

    [Fact]
    public void ExplicitLambdaNames_KeepDistinctMethodMetadata()
    {
        var cache = new QueryLambdaCompiler(4);
        var first = Expression.Lambda<Func<int>>(Expression.Constant(1), "First", tailCall: false, []);
        var second = Expression.Lambda<Func<int>>(Expression.Constant(2), "Second", tailCall: false, []);

        Assert.Equal("First", cache.Compile(first).Method.Name);
        Assert.Equal("Second", cache.Compile(second).Method.Name);
        Assert.Equal(2, cache.Count);
    }

    private static Expression<Func<int, bool>> Predicate(Bound bound) => value => value == bound.Value;

    private sealed class Bound
    {
        public int Value { get; set; }

        public int Reads { get; private set; }

        public bool ThrowOnRead { get; set; }

        public int Read()
        {
            this.Reads++;
            return this.ThrowOnRead ? throw new InvalidOperationException("Deferred operand.") : this.Value;
        }
    }
}
