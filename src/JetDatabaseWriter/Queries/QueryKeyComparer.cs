namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Orders query keys produced by compiled key selectors. Database nulls sort first;
/// nonnull keys must have the same runtime type and use the selector type's default
/// comparer. Mixed types and noncomparable keys refuse instead of losing distinctions
/// through coercion or string formatting.
/// </summary>
internal sealed class QueryKeyComparer : Comparer<object?>
{
    private const int CacheCapacity = 64;
    private static readonly BoundedCache<Type, QueryKeyComparer> DefaultComparers = new(CacheCapacity);

    private readonly Comparison<object?>? compare;

    private QueryKeyComparer(Comparison<object?>? compare = null) => this.compare = compare;

    public static readonly QueryKeyComparer Instance = new();

    /// <summary>Returns the shared default ordering for a selector's declared key type.</summary>
    /// <param name="keyType">The selector's return type, before boxing.</param>
    /// <returns>A bounded cached adapter, or an unretained adapter for collectible types.</returns>
    internal static QueryKeyComparer For(Type keyType)
    {
        if (keyType == typeof(object))
        {
            return Instance;
        }

        return QueryPlanTemplateCache.HasCollectibleType(keyType)
            ? Create(keyType)
            : DefaultComparers.GetOrCreate(keyType, Create);
    }

    public override int Compare(object? x, object? y)
    {
        if (x is null or DBNull)
        {
            return y is null or DBNull ? 0 : -1;
        }

        if (y is null or DBNull)
        {
            return 1;
        }

        if (x.GetType() != y.GetType())
        {
            throw new ArgumentException("Default query ordering requires nonnull keys of the same runtime type. Convert keys to one type or supply an explicit comparer.");
        }

        if (this.compare is not null)
        {
            return this.compare(x, y);
        }

        return x is IComparable comparable
            ? comparable.CompareTo(y)
            : throw new ArgumentException("Default query ordering requires comparable keys. Supply an explicit comparer for this key type.");
    }

    private static QueryKeyComparer Create(Type keyType)
    {
        ParameterExpression x = Expression.Parameter(typeof(object), "x");
        ParameterExpression y = Expression.Parameter(typeof(object), "y");
        MemberExpression comparer = Expression.Property(null, typeof(Comparer<>).MakeGenericType(keyType), nameof(Default));
        MethodCallExpression compare = Expression.Call(
            comparer,
            nameof(Compare),
            Type.EmptyTypes,
            Expression.Convert(x, keyType),
            Expression.Convert(y, keyType));
        var expression = Expression.Lambda<Comparison<object?>>(compare, x, y);
        return new QueryKeyComparer(QueryPlanTemplateCache.HasCollectibleType(keyType)
            ? expression.Compile()
            : QueryLambdaCompiler.Shared.Compile(expression));
    }
}
