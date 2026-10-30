namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Linq;

/// <summary>
/// Runs a query's tail over the rows the engine streams. The tail is everything above the
/// boundary <see cref="AccessQueryTranslator"/> finds: a <c>Select</c> projection and the
/// operators after it, or an operator the engine does not translate, such as an ordering with
/// a comparer. Each operator becomes one step over a sequence of boxed elements, so no step is
/// built per element type: lambdas reuse bounded compiled templates against an
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
[RequiresUnreferencedCode("LINQ queries and Include discover entity types and members at runtime. Use typed row readers for trimmed applications.")]
[RequiresDynamicCode("LINQ queries and Include construct generic types at runtime. Use typed row readers for NativeAOT applications.")]
internal static class InMemoryTail
{
    private static readonly Func<object?, object?> Identity = static item => item;

    private static readonly Dictionary<string, StepFactory> Steps = new(StringComparer.Ordinal)
    {
        ["Where"] = static (call, source, _) => source.Where(CompileElementLambda<bool>(call)),
        ["Select"] = static (call, source, _) => source.Select(CompileElementLambda<object?>(call)),
        ["Cast"] = static (call, source, _) => source.Select(CompileCast(call.Method.GetGenericArguments()[0])),
        ["OfType"] = static (call, source, _) => source.Where(call.Method.GetGenericArguments()[0].IsInstanceOfType),
        ["Skip"] = static (call, source, _) => source.Skip(CountArgument(call)),
        ["Take"] = static (call, source, _) => source.Take(CountArgument(call)),
        ["SkipWhile"] = static (call, source, _) => source.SkipWhile(CompileElementLambda<bool>(call)),
        ["TakeWhile"] = static (call, source, _) => source.TakeWhile(CompileElementLambda<bool>(call)),
        ["SkipLast"] = static (call, source, _) => source.SkipLast(CountArgument(call)),
        ["TakeLast"] = static (call, source, _) => source.TakeLast(CountArgument(call)),
        ["Distinct"] = static (call, source, _) => source.Distinct(ElementEquality(call, comparerIndex: 1)),
        ["Union"] = static (call, source, cancellationToken) => source.Union(SecondSource(call, cancellationToken), ElementEquality(call, comparerIndex: 2)),
        ["Intersect"] = static (call, source, cancellationToken) => IntersectAsync(source, SecondSource(call, cancellationToken), ElementEquality(call, comparerIndex: 2), cancellationToken),
        ["Except"] = static (call, source, cancellationToken) => ExceptAsync(source, SecondSource(call, cancellationToken), ElementEquality(call, comparerIndex: 2), cancellationToken),
        ["Concat"] = static (call, source, cancellationToken) => source.Concat(SecondSource(call, cancellationToken)),
        ["Reverse"] = static (_, source, _) => source.Reverse(),
        ["DefaultIfEmpty"] = static (call, source, _) => source.DefaultIfEmpty(DefaultElement(call)),
        ["Append"] = static (call, source, _) => source.Append(ClosureValueReader.Evaluate(call.Arguments[1])),
        ["Prepend"] = static (call, source, _) => source.Prepend(ClosureValueReader.Evaluate(call.Arguments[1])),
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
    /// <exception cref="JetNotSupportedException">Thrown when the tail holds an operator this class does not run.</exception>
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
            List<QuerySortKey<object?>> keys = [CreateSortKey(call)];
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
    internal static JetNotSupportedException UnsupportedOperator(string operatorName, string? overload = null)
    {
        string form = overload is null ? $"'{operatorName}'" : $"'{operatorName}' {overload}";
        return new JetNotSupportedException(
            JetErrorCode.FeatureNotSupported,
            $"The query operator {form} is not supported on Query<T> results. Use AsAsyncEnumerable() and apply '{operatorName}' with the async LINQ operators, or materialize the rows with ToListAsync and apply it with LINQ to Objects.");
    }

    private static JetNotSupportedException IncludeAboveBoundary(string operatorName) =>
        new(JetErrorCode.FeatureNotSupported, $"'{operatorName}' loads navigations onto the entities the table read returns, so it must come before Select and the other operators that run after the read. Move '{operatorName}' ahead of them.");

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

    private static QuerySortKey<object?> CreateSortKey(MethodCallExpression call)
    {
        Type[] typeArguments = call.Method.GetGenericArguments();
        bool descending = call.Method.Name.EndsWith("Descending", StringComparison.Ordinal);

        // Order and OrderDescending sort the elements themselves; the others take a key selector.
        return call.Method.Name is "Order" or "OrderDescending"
            ? new QuerySortKey<object?>(Identity, KeyOrder(typeArguments[0], OptionalArgument(call, 1)), descending)
            : new QuerySortKey<object?>(CompileElementLambda<object?>(call), KeyOrder(typeArguments[1], OptionalArgument(call, 2)), descending);
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
    /// <exception cref="JetNotSupportedException">Thrown for an overload whose lambda also takes the element's index.</exception>
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

        return QueryLambdaCompiler.Shared.Compile(Expression.Lambda<Func<object?, TResult>>(body, item));
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

        object? value = ClosureValueReader.Evaluate(argument);
        if (value is IQueryable { Provider: IAccessQueryEngine capturedEngine } query)
        {
            return capturedEngine.ExecuteStreamAsync(query.Expression, cancellationToken);
        }

        var values = (IEnumerable)value!;
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
            ?? throw new JetOperationException(JetErrorCode.InvalidOperationState, $"{comparerDefinition.Name} has no default instance for {type}.");

    private static MethodInfo RequireMethod(Type type, string name, params Type[] parameterTypes) =>
        type.GetMethod(name, parameterTypes)
            ?? throw new JetOperationException(JetErrorCode.InvalidOperationState, $"{type} has no method {name} with {parameterTypes.Length} parameters.");

    private static async IAsyncEnumerable<object?> SortAsync(
        IAsyncEnumerable<object?> source,
        List<QuerySortKey<object?>> keys,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        List<object?> buffer = await source.ToListAsync(cancellationToken).ConfigureAwait(false);

        // Keep Enumerable's stable ordering: async ordering can lose a later key selector's
        // exception and instead throw from comparing its uninitialized key buffer.
        foreach (object? item in QueryOrdering.Apply(buffer, keys))
        {
            yield return item;
        }
    }

    private static async IAsyncEnumerable<object?> IntersectAsync(
        IAsyncEnumerable<object?> first,
        IAsyncEnumerable<object?> second,
        BoxedEqualityComparer comparer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Async LINQ's Intersect can skip either source when an operand is empty.
        // Prepare the second, then stream the first, preserving failures from both.
        HashSet<object?> remaining = await second.ToHashSetAsync(comparer, cancellationToken).ConfigureAwait(false);
        await foreach (object? item in first.Where(remaining.Remove).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    private static async IAsyncEnumerable<object?> ExceptAsync(
        IAsyncEnumerable<object?> first,
        IAsyncEnumerable<object?> second,
        BoxedEqualityComparer comparer,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Async LINQ's Except advances the first source before reading the second.
        // Preserve second-source failures first, including when the first source is empty.
        HashSet<object?> excluded = await second.ToHashSetAsync(comparer, cancellationToken).ConfigureAwait(false);
        await foreach (object? item in first.Where(excluded.Add).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>Equates boxed elements through compiled calls to the element type's equality comparer.</summary>
    /// <param name="equals">Unboxes both elements and compares them.</param>
    /// <param name="hash">Unboxes an element that is not null and hashes it.</param>
    private sealed class BoxedEqualityComparer(Func<object?, object?, bool> equals, Func<object?, int> hash) : IEqualityComparer<object?>
    {
        bool IEqualityComparer<object?>.Equals(object? x, object? y) => equals(x, y);

        int IEqualityComparer<object?>.GetHashCode(object? obj) => obj is null ? 0 : hash(obj);
    }
}
