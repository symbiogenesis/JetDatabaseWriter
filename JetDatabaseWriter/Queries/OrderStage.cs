namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;

/// <summary>
/// A contiguous run of <c>OrderBy</c>/<c>ThenBy</c> keys. Ordering buffers the source
/// and sorts it; later stages observe the sorted sequence.
/// </summary>
internal sealed class OrderStage : QueryStage
{
    public List<OrderingKey> Keys { get; } = [];

    public override async IAsyncEnumerable<T> Apply<T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var buffer = new List<T>();
        await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(item);
        }

        foreach (T item in Sort(buffer, this.Keys))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Finds an index whose key order is provably identical to this stage's stable sort, so
    /// the provider can read the source in key order and skip the in-memory sort. Returns
    /// <see langword="null"/> when any key is not an order-safe integer column access or no
    /// index covers the ordering.
    /// </summary>
    /// <param name="indexes">The candidate indexes on the queried table.</param>
    /// <returns>The covering index, or <see langword="null"/> when none qualifies.</returns>
    public IndexMetadata? FindCoveringIndex(IReadOnlyList<IndexMetadata> indexes)
    {
        var columns = new List<(string Column, bool Descending)>(this.Keys.Count);
        foreach (OrderingKey key in this.Keys)
        {
            if (!TryDescribeIntegerKey(key, out string? column))
            {
                return null;
            }

            columns.Add((column!, key.Descending));
        }

        foreach (IndexMetadata index in indexes)
        {
            if (Covers(index, columns))
            {
                return index;
            }
        }

        return null;
    }

    private static List<T> Sort<T>(List<T> buffer, List<OrderingKey> keys)
    {
        Func<T, object?> first = CompileKey<T>(keys[0].KeySelector);
        IOrderedEnumerable<T> ordered = keys[0].Descending
            ? buffer.OrderByDescending(first, QueryKeyComparer.Instance)
            : buffer.OrderBy(first, QueryKeyComparer.Instance);
        for (int i = 1; i < keys.Count; i++)
        {
            Func<T, object?> key = CompileKey<T>(keys[i].KeySelector);
            ordered = keys[i].Descending
                ? ordered.ThenByDescending(key, QueryKeyComparer.Instance)
                : ordered.ThenBy(key, QueryKeyComparer.Instance);
        }

        return ordered.ToList();
    }

    private static Func<T, object?> CompileKey<T>(LambdaExpression selector)
    {
        ParameterExpression parameter = selector.Parameters[0];
        Expression body = Expression.Convert(selector.Body, typeof(object));
        return Expression.Lambda<Func<T, object?>>(body, parameter).Compile();
    }

    private static bool TryDescribeIntegerKey(OrderingKey key, out string? column)
    {
        column = null;

        Expression body = key.KeySelector.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert)
        {
            body = convert.Operand;
        }

        // Only a direct property access on the lambda parameter (e.g. i => i.Id) maps to a
        // column; nested paths and computed keys are left to the in-memory sort. The CLR
        // type is restricted to signed integers/byte, whose JET index key bytes compare in
        // the same order as the values — unlike float/double (NaN) or date (pre-1899 OADate).
        if (body is MemberExpression { Member: PropertyInfo property, Expression: ParameterExpression parameter }
            && parameter == key.KeySelector.Parameters[0]
            && IsOrderSafeIntegerType(property.PropertyType))
        {
            column = property.Name;
            return true;
        }

        return false;
    }

    private static bool IsOrderSafeIntegerType(Type type)
    {
        Type underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying == typeof(int)
            || underlying == typeof(long)
            || underlying == typeof(short)
            || underlying == typeof(byte);
    }

    private static bool Covers(IndexMetadata index, List<(string Column, bool Descending)> keys)
    {
        // The index serves the ordering only when it has a usable B-tree root, never omits
        // rows (no ignore-nulls), and is unique — uniqueness rules out key ties, so the
        // index order equals the stable LINQ sort. Its key columns must match the ordering
        // columns one-for-one, in order, including sort direction.
        if (index.FirstDp <= 0 || !index.EnforcesUniqueness || index.IgnoreNulls || index.Columns.Count != keys.Count)
        {
            return false;
        }

        for (int i = 0; i < keys.Count; i++)
        {
            IndexColumnReference column = index.Columns[i];
            bool ascendingRequested = !keys[i].Descending;
            if (column.IsAscending != ascendingRequested
                || !string.Equals(column.Name, keys[i].Column, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }
}
