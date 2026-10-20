namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Orders query keys produced by compiled key selectors. Database nulls sort first;
/// same-typed <see cref="IComparable"/> keys compare directly; mismatched types are
/// coerced where possible, falling back to an ordinal string comparison.
/// </summary>
internal sealed class QueryKeyComparer : IComparer<object?>
{
    public static readonly QueryKeyComparer Instance = new();

    public int Compare(object? x, object? y)
    {
        if (x is null or DBNull)
        {
            return y is null or DBNull ? 0 : -1;
        }

        if (y is null or DBNull)
        {
            return 1;
        }

        if (x.GetType() == y.GetType() && x is IComparable sameTyped)
        {
            return sameTyped.CompareTo(y);
        }

        if (x is IComparable comparable && ScalarConversion.TryConvert(y, x.GetType(), out object? converted))
        {
            return comparable.CompareTo(converted);
        }

        return string.CompareOrdinal(x.ToString(), y.ToString());
    }
}
