namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Linq;

/// <summary>
/// Runs a query's tail over the rows the engine streams. The tail is everything above the
/// boundary <see cref="AccessQueryTranslator"/> finds: a <c>Select</c> projection and the
/// operators after it, or an operator the engine does not translate, such as an ordering with
/// a comparer. Each operator becomes one step over a sequence of boxed elements, so no step is
/// built per element type: lambdas are compiled once per execution against an
/// <see cref="object"/> parameter, and ordering and equality go through the key or element
/// type's own comparer (the default one, or the one the query passes), so the results are the
/// ones LINQ to Objects gives. <c>Where</c>, <c>Select</c>, the paging operators,
/// <c>Concat</c>, <c>Append</c> and the like pass rows on as they arrive, so a <c>Take</c> or
/// <c>FirstAsync</c> stops the table read; ordering, <c>Reverse</c>, <c>TakeLast</c> and the
/// set operators buffer what they need. A second sequence that is itself a
/// <c>Query&lt;T&gt;</c> runs through its own provider, asynchronously. Every other operator
/// (<c>GroupBy</c>, <c>SelectMany</c>, the joins, <c>Zip</c>, <c>Chunk</c>, the <c>...By</c>
/// set operators, and the overloads whose lambdas take an element index) throws
/// <see cref="NotSupportedException"/> before anything is read, naming the operator and
/// <c>AsAsyncEnumerable()</c>, which hands the rows to the async LINQ operators instead.
/// </summary>
internal static class InMemoryTail
{
    private static readonly Func<object?, object?> Identity = static item => item;

    private static readonly Dictionary<string, StepFactory> Steps = new(StringComparer.Ordinal)
    {
        ["Where"] = static (call, source, cancellationToken) => WhereAsync(source, CompileElementLambda<bool>(call), cancellationToken),
        ["Select"] = static (call, source, cancellationToken) => SelectAsync(source, CompileElementLambda<object?>(call), cancellationToken),
        ["Cast"] = static (call, source, cancellationToken) => SelectAsync(source, CompileCast(call.Method.GetGenericArguments()[0]), cancellationToken),
        ["OfType"] = static (call, source, cancellationToken) => WhereAsync(source, call.Method.GetGenericArguments()[0].IsInstanceOfType, cancellationToken),
        ["Skip"] = static (call, source, cancellationToken) => new SkipStage(CountArgument(call)).Apply(source, cancellationToken),
        ["Take"] = static (call, source, cancellationToken) => new TakeStage(CountArgument(call)).Apply(source, cancellationToken),
        ["SkipWhile"] = static (call, source, cancellationToken) => SkipWhileAsync(source, CompileElementLambda<bool>(call), cancellationToken),
        ["TakeWhile"] = static (call, source, cancellationToken) => TakeWhileAsync(source, CompileElementLambda<bool>(call), cancellationToken),
        ["SkipLast"] = static (call, source, cancellationToken) => SkipLastAsync(source, CountArgument(call), cancellationToken),
        ["TakeLast"] = static (call, source, cancellationToken) => TakeLastAsync(source, CountArgument(call), cancellationToken),
        ["Distinct"] = static (call, source, cancellationToken) => DistinctAsync(source, ElementEquality(call, comparerIndex: 1), cancellationToken),
        ["Union"] = static (call, source, cancellationToken) => UnionAsync(source, SecondSource(call, cancellationToken), ElementEquality(call, comparerIndex: 2), cancellationToken),
        ["Intersect"] = static (call, source, cancellationToken) => IntersectAsync(source, SecondSource(call, cancellationToken), ElementEquality(call, comparerIndex: 2), cancellationToken),
        ["Except"] = static (call, source, cancellationToken) => ExceptAsync(source, SecondSource(call, cancellationToken), ElementEquality(call, comparerIndex: 2), cancellationToken),
        ["Concat"] = static (call, source, cancellationToken) => ConcatAsync(source, SecondSource(call, cancellationToken), cancellationToken),
        ["Reverse"] = static (_, source, cancellationToken) => ReverseAsync(source, cancellationToken),
        ["DefaultIfEmpty"] = static (call, source, cancellationToken) => DefaultIfEmptyAsync(source, DefaultElement(call), cancellationToken),
        ["Append"] = static (call, source, cancellationToken) => AppendAsync(source, ClosureValueReader.Evaluate(call.Arguments[1]), cancellationToken),
        ["Prepend"] = static (call, source, cancellationToken) => PrependAsync(source, ClosureValueReader.Evaluate(call.Arguments[1]), cancellationToken),
    };

    /// <summary>Builds the step one tail operator runs, over the sequence the operators before it produce.</summary>
    /// <param name="call">The operator call.</param>
    /// <param name="source">The sequence the operators before it produce.</param>
    /// <param name="cancellationToken">A token used to cancel the enumeration.</param>
    /// <returns>The operator's output sequence.</returns>
    private delegate IAsyncEnumerable<object?> StepFactory(MethodCallExpression call, IAsyncEnumerable<object?> source, CancellationToken cancellationToken);

    /// <summary>
    /// Composes the tail of <paramref name="expression"/>, the operators between
    /// <paramref name="boundary"/> and the root, over <paramref name="rows"/>. Every lambda is
    /// compiled and every operator checked here, before a row is read, so an unsupported
    /// operator throws before the table read starts.
    /// </summary>
    /// <param name="rows">The rows the engine streams for the boundary.</param>
    /// <param name="expression">The full query expression.</param>
    /// <param name="boundary">The engine boundary inside <paramref name="expression"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the enumeration.</param>
    /// <returns>The tail's output, boxed.</returns>
    /// <exception cref="NotSupportedException">Thrown when the tail holds an operator this class does not run.</exception>
    public static IAsyncEnumerable<object?> Apply(IAsyncEnumerable<object?> rows, Expression expression, Expression boundary, CancellationToken cancellationToken)
    {
        var calls = new List<MethodCallExpression>();
        for (Expression node = expression; !ReferenceEquals(node, boundary); node = calls[^1].Arguments[0])
        {
            calls.Add((MethodCallExpression)node);
        }

        calls.Reverse();
        IAsyncEnumerable<object?> sequence = rows;
        for (int i = 0; i < calls.Count; i++)
        {
            MethodCallExpression call = calls[i];
            if (!IsOrdering(call))
            {
                sequence = CreateStep(call, sequence, cancellationToken);
                continue;
            }

            // An ordering and the ThenBy calls that refine it sort together, in one buffer.
            List<SortKey> keys = [CreateSortKey(call)];
            while (i + 1 < calls.Count && IsThenBy(calls[i + 1]))
            {
                i++;
                keys.Add(CreateSortKey(calls[i]));
            }

            sequence = SortAsync(sequence, keys, cancellationToken);
        }

        return sequence;
    }

    /// <summary>Returns the exception a query operator that does not run on a <c>Query&lt;T&gt;</c> result throws.</summary>
    /// <param name="operatorName">The operator's name.</param>
    /// <param name="overload">Describes the unsupported overload, or <see langword="null"/> when the operator is not supported at all.</param>
    /// <returns>The exception to throw.</returns>
    internal static NotSupportedException UnsupportedOperator(string operatorName, string? overload = null)
    {
        string form = overload is null ? $"'{operatorName}'" : $"'{operatorName}' {overload}";
        return new NotSupportedException(
            $"The query operator {form} is not supported on Query<T> results. Use AsAsyncEnumerable() and apply '{operatorName}' "
            + "with the async LINQ operators, or materialize the rows with ToListAsync and apply it with LINQ to Objects.");
    }

    private static NotSupportedException IncludeAboveBoundary(string operatorName) =>
        new($"'{operatorName}' loads navigations onto the entities the table read returns, so it must come before Select "
            + $"and the other operators that run after the read. Move '{operatorName}' ahead of them.");

    private static IAsyncEnumerable<object?> CreateStep(MethodCallExpression call, IAsyncEnumerable<object?> source, CancellationToken cancellationToken)
    {
        if (AccessQueryExtensions.IsIncludeMethod(call.Method) || AccessQueryExtensions.IsThenIncludeMethod(call.Method))
        {
            throw IncludeAboveBoundary(call.Method.Name);
        }

        return call.Method.DeclaringType == typeof(Queryable) && Steps.TryGetValue(call.Method.Name, out StepFactory? create)
            ? create(call, source, cancellationToken)
            : throw UnsupportedOperator(call.Method.Name);
    }

    private static bool IsOrdering(MethodCallExpression call) =>
        call.Method.DeclaringType == typeof(Queryable)
        && call.Method.Name is "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" or "Order" or "OrderDescending";

    private static bool IsThenBy(MethodCallExpression call) =>
        call.Method.DeclaringType == typeof(Queryable) && call.Method.Name is "ThenBy" or "ThenByDescending";

    private static SortKey CreateSortKey(MethodCallExpression call)
    {
        Type[] typeArguments = call.Method.GetGenericArguments();
        bool descending = call.Method.Name.EndsWith("Descending", StringComparison.Ordinal);

        // Order and OrderDescending sort the elements themselves; the others take a key selector.
        return call.Method.Name is "Order" or "OrderDescending"
            ? new SortKey(Identity, KeyOrder(typeArguments[0], OptionalArgument(call, 1)), descending)
            : new SortKey(CompileElementLambda<object?>(call), KeyOrder(typeArguments[1], OptionalArgument(call, 2)), descending);
    }

    private static Expression? OptionalArgument(MethodCallExpression call, int index) =>
        call.Arguments.Count > index ? call.Arguments[index] : null;

    /// <summary>
    /// Compiles the single-parameter lambda in <paramref name="call"/>'s second argument into a
    /// delegate over the boxed element: the parameter is bound to the unboxed element and the
    /// result converted to <typeparamref name="TResult"/>, so one delegate shape serves every
    /// element type.
    /// </summary>
    /// <typeparam name="TResult">The delegate's result: <see cref="bool"/> for a predicate, <see cref="object"/> for a selector.</typeparam>
    /// <param name="call">The operator call whose second argument is the lambda.</param>
    /// <returns>The compiled delegate.</returns>
    /// <exception cref="NotSupportedException">Thrown for an overload whose lambda also takes the element's index.</exception>
    private static Func<object?, TResult> CompileElementLambda<TResult>(MethodCallExpression call)
    {
        var lambda = (LambdaExpression)StripQuote(call.Arguments[1]);
        if (lambda.Parameters.Count != 1)
        {
            throw UnsupportedOperator(call.Method.Name, "with an element index");
        }

        ParameterExpression item = Expression.Parameter(typeof(object), "item");
        Expression body = Expression.Invoke(lambda, Expression.Convert(item, lambda.Parameters[0].Type));
        if (body.Type != typeof(TResult))
        {
            body = Expression.Convert(body, typeof(TResult));
        }

        return Expression.Lambda<Func<object?, TResult>>(body, item).Compile();
    }

    private static Func<object?, object?> CompileCast(Type target)
    {
        // The unbox or reference cast to the target throws just as Enumerable.Cast's does.
        ParameterExpression item = Expression.Parameter(typeof(object), "item");
        Expression cast = Expression.Convert(Expression.Convert(item, target), typeof(object));
        return Expression.Lambda<Func<object?, object?>>(cast, item).Compile();
    }

    private static Expression StripQuote(Expression expression) =>
        expression is UnaryExpression { NodeType: ExpressionType.Quote } quote ? quote.Operand : expression;

    private static int CountArgument(MethodCallExpression call) =>
        call.Arguments[1].Type == typeof(int)
            ? Convert.ToInt32(ClosureValueReader.Evaluate(call.Arguments[1]), CultureInfo.InvariantCulture)
            : throw UnsupportedOperator(call.Method.Name, "with a range");

    private static object? DefaultElement(MethodCallExpression call) =>
        ClosureValueReader.Evaluate(OptionalArgument(call, 1) ?? Expression.Default(call.Method.GetGenericArguments()[0]));

    /// <summary>
    /// Returns the second sequence of <c>Concat</c>, <c>Union</c>, <c>Intersect</c> or
    /// <c>Except</c>. Another <c>Query&lt;T&gt;</c> passes its own expression tree, which runs
    /// through its own provider, asynchronously; anything else is an in-memory sequence.
    /// </summary>
    /// <param name="call">The operator call whose second argument is the sequence.</param>
    /// <param name="cancellationToken">A token used to cancel the enumeration.</param>
    /// <returns>The prepared sequence, validated before either source is read.</returns>
    private static IAsyncEnumerable<object?> SecondSource(MethodCallExpression call, CancellationToken cancellationToken)
    {
        Expression argument = call.Arguments[1];
        Expression root = argument;
        while (root is MethodCallExpression { Arguments.Count: > 0 } inner)
        {
            root = inner.Arguments[0];
        }

        if (root is ConstantExpression { Value: IQueryable { Provider: IAccessQueryEngine engine } })
        {
            return engine.ExecuteStreamAsync(argument, cancellationToken);
        }

        var values = (IEnumerable)ClosureValueReader.Evaluate(argument)!;
        return values.Cast<object?>().ToAsyncEnumerable();
    }

    /// <summary>
    /// Returns a comparer that orders boxed keys through <paramref name="keyType"/>'s comparer:
    /// the one the query passes, or <see cref="Comparer{T}.Default"/> when it passes none, as
    /// LINQ to Objects does.
    /// </summary>
    /// <param name="keyType">The key type.</param>
    /// <param name="comparerArgument">The query's <see cref="IComparer{T}"/> argument, or <see langword="null"/>.</param>
    /// <returns>The comparer over boxed keys.</returns>
    private static Comparer<object?> KeyOrder(Type keyType, Expression? comparerArgument)
    {
        Type comparerType = typeof(IComparer<>).MakeGenericType(keyType);
        object comparer = (comparerArgument is null ? null : ClosureValueReader.Evaluate(comparerArgument))
            ?? DefaultInstance(typeof(Comparer<>), keyType);
        ParameterExpression x = Expression.Parameter(typeof(object), "x");
        ParameterExpression y = Expression.Parameter(typeof(object), "y");
        MethodCallExpression compare = Expression.Call(
            Expression.Constant(comparer, comparerType),
            RequireMethod(comparerType, "Compare", keyType, keyType),
            Expression.Convert(x, keyType),
            Expression.Convert(y, keyType));
        return Comparer<object?>.Create(Expression.Lambda<Comparison<object?>>(compare, x, y).Compile());
    }

    /// <summary>
    /// Returns an equality comparer over boxed elements that goes through the element type's
    /// comparer: the one the query passes, or <see cref="EqualityComparer{T}.Default"/> when it
    /// passes none, as LINQ to Objects does.
    /// </summary>
    /// <param name="call">The set operator call; its first type argument is the element type.</param>
    /// <param name="comparerIndex">The position of the optional <see cref="IEqualityComparer{T}"/> argument.</param>
    /// <returns>The comparer over boxed elements.</returns>
    private static BoxedEqualityComparer ElementEquality(MethodCallExpression call, int comparerIndex)
    {
        Type element = call.Method.GetGenericArguments()[0];
        Type comparerType = typeof(IEqualityComparer<>).MakeGenericType(element);
        object comparer = (OptionalArgument(call, comparerIndex) is { } argument ? ClosureValueReader.Evaluate(argument) : null)
            ?? DefaultInstance(typeof(EqualityComparer<>), element);
        ConstantExpression instance = Expression.Constant(comparer, comparerType);
        ParameterExpression x = Expression.Parameter(typeof(object), "x");
        ParameterExpression y = Expression.Parameter(typeof(object), "y");
        MethodCallExpression equals = Expression.Call(
            instance,
            RequireMethod(comparerType, "Equals", element, element),
            Expression.Convert(x, element),
            Expression.Convert(y, element));
        MethodCallExpression hash = Expression.Call(instance, RequireMethod(comparerType, "GetHashCode", element), Expression.Convert(x, element));
        return new BoxedEqualityComparer(
            Expression.Lambda<Func<object?, object?, bool>>(equals, x, y).Compile(),
            Expression.Lambda<Func<object?, int>>(hash, x).Compile());
    }

    private static object DefaultInstance(Type comparerDefinition, Type type) =>
        comparerDefinition.MakeGenericType(type).GetProperty("Default")?.GetValue(obj: null)
            ?? throw new InvalidOperationException($"{comparerDefinition.Name} has no default instance for {type}.");

    private static MethodInfo RequireMethod(Type type, string name, params Type[] parameterTypes) =>
        type.GetMethod(name, parameterTypes)
            ?? throw new InvalidOperationException($"{type} has no method {name} with {parameterTypes.Length} parameters.");

    private static async IAsyncEnumerable<object?> WhereAsync(
        IAsyncEnumerable<object?> source,
        Func<object?, bool> predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (predicate(item))
            {
                yield return item;
            }
        }
    }

    private static async IAsyncEnumerable<object?> SelectAsync(
        IAsyncEnumerable<object?> source,
        Func<object?, object?> selector,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return selector(item);
        }
    }

    private static async IAsyncEnumerable<object?> SkipWhileAsync(
        IAsyncEnumerable<object?> source,
        Func<object?, bool> predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        bool yielding = false;
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (!yielding && predicate(item))
            {
                continue;
            }

            yielding = true;
            yield return item;
        }
    }

    private static async IAsyncEnumerable<object?> TakeWhileAsync(
        IAsyncEnumerable<object?> source,
        Func<object?, bool> predicate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (!predicate(item))
            {
                yield break;
            }

            yield return item;
        }
    }

    private static async IAsyncEnumerable<object?> SkipLastAsync(
        IAsyncEnumerable<object?> source,
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var held = new Queue<object?>();
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            held.Enqueue(item);
            if (held.Count > count)
            {
                yield return held.Dequeue();
            }
        }
    }

    private static async IAsyncEnumerable<object?> TakeLastAsync(
        IAsyncEnumerable<object?> source,
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (count <= 0)
        {
            yield break;
        }

        var last = new Queue<object?>();
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (last.Count == count)
            {
                _ = last.Dequeue();
            }

            last.Enqueue(item);
        }

        foreach (object? item in last)
        {
            yield return item;
        }
    }

    private static async IAsyncEnumerable<object?> SortAsync(
        IAsyncEnumerable<object?> source,
        List<SortKey> keys,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new List<object?>();
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(item);
        }

        // Enumerable's sort is stable, so ties keep their stream order, as LINQ to Objects keeps them.
        SortKey first = keys[0];
        IOrderedEnumerable<object?> ordered = first.Descending
            ? buffer.OrderByDescending(first.Selector, first.Comparer)
            : buffer.OrderBy(first.Selector, first.Comparer);
        for (int i = 1; i < keys.Count; i++)
        {
            SortKey key = keys[i];
            ordered = key.Descending
                ? ordered.ThenByDescending(key.Selector, key.Comparer)
                : ordered.ThenBy(key.Selector, key.Comparer);
        }

        foreach (object? item in ordered)
        {
            yield return item;
        }
    }

    private static async IAsyncEnumerable<object?> ReverseAsync(
        IAsyncEnumerable<object?> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new List<object?>();
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(item);
        }

        for (int i = buffer.Count - 1; i >= 0; i--)
        {
            yield return buffer[i];
        }
    }

    private static async IAsyncEnumerable<object?> DistinctAsync(
        IAsyncEnumerable<object?> source,
        BoxedEqualityComparer comparer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var seen = new HashSet<object?>(comparer);
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (seen.Add(item))
            {
                yield return item;
            }
        }
    }

    private static async IAsyncEnumerable<object?> UnionAsync(
        IAsyncEnumerable<object?> first,
        IAsyncEnumerable<object?> second,
        BoxedEqualityComparer comparer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var seen = new HashSet<object?>(comparer);
        await foreach (object? item in first.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (seen.Add(item))
            {
                yield return item;
            }
        }

        await foreach (object? item in second.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (seen.Add(item))
            {
                yield return item;
            }
        }
    }

    private static async IAsyncEnumerable<object?> IntersectAsync(
        IAsyncEnumerable<object?> first,
        IAsyncEnumerable<object?> second,
        BoxedEqualityComparer comparer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // As in Enumerable.Intersect, the second sequence is read first, and each match is yielded once.
        HashSet<object?> remaining = await ToSetAsync(second, comparer, cancellationToken).ConfigureAwait(false);
        await foreach (object? item in first.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (remaining.Remove(item))
            {
                yield return item;
            }
        }
    }

    private static async IAsyncEnumerable<object?> ExceptAsync(
        IAsyncEnumerable<object?> first,
        IAsyncEnumerable<object?> second,
        BoxedEqualityComparer comparer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // As in Enumerable.Except, the second sequence is read first, and each survivor is yielded once.
        HashSet<object?> excluded = await ToSetAsync(second, comparer, cancellationToken).ConfigureAwait(false);
        await foreach (object? item in first.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (excluded.Add(item))
            {
                yield return item;
            }
        }
    }

    private static async ValueTask<HashSet<object?>> ToSetAsync(
        IAsyncEnumerable<object?> source,
        BoxedEqualityComparer comparer,
        CancellationToken cancellationToken)
    {
        var set = new HashSet<object?>(comparer);
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            _ = set.Add(item);
        }

        return set;
    }

    private static async IAsyncEnumerable<object?> ConcatAsync(
        IAsyncEnumerable<object?> first,
        IAsyncEnumerable<object?> second,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (object? item in first.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }

        await foreach (object? item in second.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private static async IAsyncEnumerable<object?> DefaultIfEmptyAsync(
        IAsyncEnumerable<object?> source,
        object? defaultValue,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        bool any = false;
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            any = true;
            yield return item;
        }

        if (!any)
        {
            yield return defaultValue;
        }
    }

    private static async IAsyncEnumerable<object?> AppendAsync(
        IAsyncEnumerable<object?> source,
        object? element,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }

        yield return element;
    }

    private static async IAsyncEnumerable<object?> PrependAsync(
        IAsyncEnumerable<object?> source,
        object? element,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        yield return element;
        await foreach (object? item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>One ordering key: its selector over the boxed element, its comparer and its direction.</summary>
    /// <param name="Selector">Projects the boxed element to its boxed key.</param>
    /// <param name="Comparer">Orders the boxed keys through the key type's comparer.</param>
    /// <param name="Descending">Whether the key sorts in descending order.</param>
    private readonly record struct SortKey(Func<object?, object?> Selector, Comparer<object?> Comparer, bool Descending);

    /// <summary>Equates boxed elements through compiled calls to the element type's equality comparer.</summary>
    /// <param name="equals">Unboxes both elements and compares them.</param>
    /// <param name="hash">Unboxes an element that is not null and hashes it.</param>
    private sealed class BoxedEqualityComparer(Func<object?, object?, bool> equals, Func<object?, int> hash) : IEqualityComparer<object?>
    {
        bool IEqualityComparer<object?>.Equals(object? x, object? y) => equals(x, y);

        int IEqualityComparer<object?>.GetHashCode(object? obj) => obj is null ? 0 : hash(obj);
    }
}
