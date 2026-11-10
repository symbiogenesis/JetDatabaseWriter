namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

internal static class JetBufferOrdering
{
    private static readonly ConditionalWeakTable<IProperty, Comparison<ValueBuffer>> Comparisons = [];

    public static IOrderedEnumerable<ValueBuffer> Apply(IEnumerable<ValueBuffer> rows, IReadOnlyList<JetOrdering> ordering)
    {
        IOrderedEnumerable<ValueBuffer>? result = null;
        foreach (JetOrdering item in ordering)
        {
            Comparison<ValueBuffer> compare = Comparisons.GetValue(item.Property, CreateComparison);
            IComparer<ValueBuffer> comparer = Comparer<ValueBuffer>.Create(item.Descending ? (left, right) => compare(right, left) : compare);
            result = result is null ? rows.OrderBy(static row => row, comparer) : result.ThenBy(static row => row, comparer);
        }

        return result!;
    }

    private static Comparison<ValueBuffer> CreateComparison(IProperty property)
    {
        ParameterExpression left = Expression.Parameter(typeof(ValueBuffer), "left");
        ParameterExpression right = Expression.Parameter(typeof(ValueBuffer), "right");
        Type comparerType = typeof(Comparer<>).MakeGenericType(property.ClrType);
        MemberExpression comparer = Expression.Property(null, comparerType, nameof(Comparer<>.Default));
        MethodCallExpression body = Expression.Call(
            comparer,
            comparerType.GetMethod(nameof(Comparer<>.Compare))!,
            Expression.Convert(Expression.Property(left, "Item", Expression.Constant(property.GetIndex())), property.ClrType),
            Expression.Convert(Expression.Property(right, "Item", Expression.Constant(property.GetIndex())), property.ClrType));
        return Expression.Lambda<Comparison<ValueBuffer>>(body, left, right).Compile();
    }
}
