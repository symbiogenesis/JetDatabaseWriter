namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;
using System.Globalization;

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

        if (x is IComparable comparable)
        {
            try
            {
                return comparable.CompareTo(Convert.ChangeType(y, x.GetType(), CultureInfo.InvariantCulture));
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
            {
                // Fall through to the string comparison.
            }
        }

        return string.CompareOrdinal(x.ToString(), y.ToString());
    }
}
